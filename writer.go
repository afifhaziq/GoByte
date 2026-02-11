package main

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"

	"github.com/parquet-go/parquet-go"
)

type StreamWriter interface {
	WritePacket(p PacketResult) error
	Close() error
}

// CSVStreamWriter writes packets to CSV incrementally
type CSVStreamWriter struct {
	file          *os.File
	bufWriter     *bufio.Writer
	csvWriter     *csv.Writer
	maxPacketSize int
	hasClass      bool
	headerWritten bool
	flushCounter  int      // Track writes for periodic flushing
	rowBuffer     []string // Reusable row buffer to reduce allocations
	mutex         sync.Mutex
}

// NewCSVStreamWriter creates a new streaming CSV writer
func NewCSVStreamWriter(filename string, maxPacketSize int, hasClass bool) (*CSVStreamWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	// Reduced buffer size for WSL2 stability (128KB instead of 4MB)
	bufWriter := bufio.NewWriterSize(file, 128*1024)
	csvWriter := csv.NewWriter(bufWriter)

	// Pre-allocate reusable row buffer
	rowSize := maxPacketSize
	if hasClass {
		rowSize++
	}

	w := &CSVStreamWriter{
		file:          file,
		bufWriter:     bufWriter,
		csvWriter:     csvWriter,
		maxPacketSize: maxPacketSize,
		hasClass:      hasClass,
		headerWritten: false,
		flushCounter:  0,
		rowBuffer:     make([]string, rowSize),
	}

	// Write header
	if err := w.writeHeader(); err != nil {
		file.Close()
		return nil, err
	}

	return w, nil
}

func (w *CSVStreamWriter) writeHeader() error {
	headerSize := w.maxPacketSize
	if w.hasClass {
		headerSize += 1
	}

	header := make([]string, headerSize)
	for i := 0; i < w.maxPacketSize; i++ {
		header[i] = fmt.Sprintf("Byte_%d", i)
	}
	if w.hasClass {
		header[w.maxPacketSize] = "Class"
	}

	w.headerWritten = true
	return w.csvWriter.Write(header)
}

func (w *CSVStreamWriter) WritePacket(p PacketResult) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	
	data := p.Data

	
	rowSize := len(data)
	if w.hasClass {
		rowSize++
	}

	// Use pre-allocated buffer if size matches, otherwise create new one
	var row []string
	if rowSize == len(w.rowBuffer) {
		row = w.rowBuffer
	} else {
		row = make([]string, rowSize)
	}

	// Convert bytes to strings
	for i, b := range data {
		row[i] = strconv.Itoa(int(b))
	}

	// Add class label if present
	if w.hasClass {
		row[len(data)] = p.Class
	}

	if err := w.csvWriter.Write(row); err != nil {
		return err
	}

	w.flushCounter++

	
	if w.flushCounter >= 10000 {
		w.csvWriter.Flush()
		if err := w.csvWriter.Error(); err != nil {
			return fmt.Errorf("csv flush error: %w", err)
		}
		w.bufWriter.Flush()
		w.flushCounter = 0

		
		runtime.GC()
		debug.FreeOSMemory()
	}

	return nil
}

func (w *CSVStreamWriter) Close() error {
	// Final flush before closing
	w.csvWriter.Flush()
	if err := w.csvWriter.Error(); err != nil {
		w.file.Close()
		return fmt.Errorf("csv final flush error: %w", err)
	}
	if err := w.bufWriter.Flush(); err != nil {
		w.file.Close()
		return fmt.Errorf("buffer final flush error: %w", err)
	}
	return w.file.Close()
}

// NumpyStreamWriter writes packets to NumPy .npy format incrementally
// Outputs uint8 array matching CSV schema with optional class labels
type NumpyStreamWriter struct {
	file           *os.File
	bufWriter      *bufio.Writer
	dataFile       *os.File      // Main data file
	dataBufWriter  *bufio.Writer // Buffer for data
	labelsFile     *os.File      // Separate file for labels (if hasClass)
	labelsBufWriter *bufio.Writer // Buffer for labels
	maxPacketSize  int
	hasClass       bool
	packetCount    int64
	flushCounter   int
	headerSize     int64 // Size of header to seek back to
	mutex          sync.Mutex
	classToInt     map[string]byte // Map class names to integers
	nextClassID    byte            // Next available class ID
	baseFilename   string          // Base filename without extension
}

// NewNumpyStreamWriter creates a new streaming NumPy writer
// If hasClass is true, creates two files: <basename>_data.npy and <basename>_labels.npy
func NewNumpyStreamWriter(filename string, maxPacketSize int, hasClass bool) (*NumpyStreamWriter, error) {
	// Remove extension if present and store base filename
	baseFilename := strings.TrimSuffix(filename, ".npy")
	baseFilename = strings.TrimSuffix(baseFilename, ".npz")
	
	// Create main data file
	dataFilename := baseFilename + "_data.npy"
	dataFile, err := os.Create(dataFilename)
	if err != nil {
		return nil, fmt.Errorf("failed to create data file: %w", err)
	}

	dataBufWriter := bufio.NewWriterSize(dataFile, 4*1024*1024) // 4MB buffer

	w := &NumpyStreamWriter{
		file:           dataFile, // Keep for backward compatibility
		bufWriter:      dataBufWriter,
		dataFile:       dataFile,
		dataBufWriter:  dataBufWriter,
		maxPacketSize:  maxPacketSize,
		hasClass:       hasClass,
		packetCount:    0,
		flushCounter:   0,
		classToInt:     make(map[string]byte),
		nextClassID:    0,
		baseFilename:   baseFilename,
	}

	// Write placeholder header for data file
	headerSize, err := w.writePlaceholderHeader(dataBufWriter, maxPacketSize)
	if err != nil {
		dataFile.Close()
		return nil, err
	}
	w.headerSize = headerSize

	// Create labels file if needed
	if hasClass {
		labelsFilename := baseFilename + "_labels.npy"
		labelsFile, err := os.Create(labelsFilename)
		if err != nil {
			dataFile.Close()
			return nil, fmt.Errorf("failed to create labels file: %w", err)
		}
		labelsBufWriter := bufio.NewWriterSize(labelsFile, 1*1024*1024) // 1MB buffer
		
		w.labelsFile = labelsFile
		w.labelsBufWriter = labelsBufWriter

		// Write placeholder header for labels file (1D array of uint8)
		_, err = w.writePlaceholderHeader(labelsBufWriter, 0) // 0 = 1D array
		if err != nil {
			dataFile.Close()
			labelsFile.Close()
			return nil, err
		}
	}

	return w, nil
}

// writePlaceholderHeader writes a NumPy header with shape (0, cols) that will be updated later
// If cols is 0, writes a 1D array header for labels
func (w *NumpyStreamWriter) writePlaceholderHeader(writer *bufio.Writer, cols int) (int64, error) {
	// Magic number: \x93NUMPY (6 bytes) - Using version 1.0 for compatibility
	magic := []byte{0x93, 'N', 'P', 'Y', 0x01, 0x00} // Version 1.0
	if _, err := writer.Write(magic); err != nil {
		return 0, err
	}

	// Create header string - reserve space for large row numbers (up to 10 digits)
	var headerStr string
	if cols > 0 {
		// 2D array for data: (rows, cols)
		headerStr = fmt.Sprintf("{'descr': '|u1', 'fortran_order': False, 'shape': (%10d, %d)}", 0, cols)
	} else {
		// 1D array for labels: (rows,)
		headerStr = fmt.Sprintf("{'descr': '|u1', 'fortran_order': False, 'shape': (%10d,)}", 0)
	}

	// Pad header to 64-byte alignment for better performance
	headerStr = w.padHeaderTo64(headerStr)

	// Write header length as uint16 little-endian (2 bytes for version 1.0)
	headerLen := uint16(len(headerStr))
	if err := binary.Write(writer, binary.LittleEndian, headerLen); err != nil {
		return 0, err
	}

	// Write header string (N bytes)
	if _, err := writer.Write([]byte(headerStr)); err != nil {
		return 0, err
	}

	totalHeaderSize := int64(len(magic) + 2 + len(headerStr))
	return totalHeaderSize, nil
}

// padHeaderTo64 pads the header string to make total header size a multiple of 64 bytes
func (w *NumpyStreamWriter) padHeaderTo64(header string) string {
	// Total header block = 6 (magic+version) + 2 (header_len for v1.0) + len(header)
	// Magic is: \x93NPY (4 bytes) + version (2 bytes) = 6 bytes total
	totalSize := 6 + 2 + len(header)

	// NumPy requires header to end with newline and be padded with spaces
	// Add newline first
	header += "\n"
	totalSize += 1

	// Calculate padding needed to reach next 64-byte boundary (optional but recommended)
	remainder := totalSize % 64
	if remainder != 0 {
		paddingNeeded := 64 - remainder
		header += strings.Repeat(" ", paddingNeeded)
	}

	return header
}

// WritePacket writes a packet to NumPy format (raw binary for data, integer for class)
func (w *NumpyStreamWriter) WritePacket(p PacketResult) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Write packet data as raw uint8 bytes (NO string conversion!)
	if _, err := w.dataBufWriter.Write(p.Data); err != nil {
		return fmt.Errorf("error writing data: %w", err)
	}

	// Write class label if present
	if w.hasClass && p.Class != "" {
		// Map class name to integer
		classID, exists := w.classToInt[p.Class]
		if !exists {
			classID = w.nextClassID
			w.classToInt[p.Class] = classID
			w.nextClassID++
		}

		// Write class ID as single uint8 byte
		if err := w.labelsBufWriter.WriteByte(classID); err != nil {
			return fmt.Errorf("error writing label: %w", err)
		}
	}

	w.packetCount++
	w.flushCounter++

	
	if w.flushCounter >= 50000 {
		w.dataBufWriter.Flush()
		if w.hasClass {
			w.labelsBufWriter.Flush()
		}
		w.flushCounter = 0

		// Force garbage collection to free memory
		runtime.GC()
		debug.FreeOSMemory()
	}

	return nil
}

// Close finalizes the NumPy file by updating the header with actual packet count
func (w *NumpyStreamWriter) Close() error {
	// Final flush of all buffers
	if err := w.dataBufWriter.Flush(); err != nil {
		return fmt.Errorf("error flushing data buffer: %w", err)
	}
	if w.hasClass {
		if err := w.labelsBufWriter.Flush(); err != nil {
			return fmt.Errorf("error flushing labels buffer: %w", err)
		}
	}

	// Update data file header with actual packet count
	if err := w.updateHeader(w.dataFile, w.maxPacketSize, w.packetCount); err != nil {
		w.dataFile.Close()
		if w.hasClass {
			w.labelsFile.Close()
		}
		return fmt.Errorf("error updating data header: %w", err)
	}

	// Update labels file header if present
	if w.hasClass {
		if err := w.updateHeader(w.labelsFile, 0, w.packetCount); err != nil {
			w.dataFile.Close()
			w.labelsFile.Close()
			return fmt.Errorf("error updating labels header: %w", err)
		}
	}

	// Close files
	if err := w.dataFile.Close(); err != nil {
		return err
	}
	if w.hasClass {
		if err := w.labelsFile.Close(); err != nil {
			return err
		}

		// Write class mapping to a JSON file for reference
		if err := w.writeClassMapping(); err != nil {
			// Non-fatal error, just log it
			fmt.Printf("Warning: failed to write class mapping: %v\n", err)
		}
	}

	return nil
}

// updateHeader seeks back to the file header and updates it with the actual row count
func (w *NumpyStreamWriter) updateHeader(file *os.File, cols int, rows int64) error {
	// Seek to position after magic (6 bytes) and before header_len (2 bytes for v1.0)
	if _, err := file.Seek(6, 0); err != nil {
		return err
	}

	// Create updated header string
	var headerStr string
	if cols > 0 {
		// 2D array for data
		headerStr = fmt.Sprintf("{'descr': '|u1', 'fortran_order': False, 'shape': (%10d, %d)}", rows, cols)
	} else {
		// 1D array for labels
		headerStr = fmt.Sprintf("{'descr': '|u1', 'fortran_order': False, 'shape': (%10d,)}", rows)
	}

	// Pad header to same size as placeholder
	headerStr = w.padHeaderTo64(headerStr)

	// Write updated header length (uint16 for v1.0)
	headerLen := uint16(len(headerStr))
	if err := binary.Write(file, binary.LittleEndian, headerLen); err != nil {
		return err
	}

	// Write updated header string
	if _, err := file.Write([]byte(headerStr)); err != nil {
		return err
	}

	return nil
}

// writeClassMapping writes the class name to integer mapping as a JSON file
func (w *NumpyStreamWriter) writeClassMapping() error {
	mappingFile := w.baseFilename + "_classes.json"
	file, err := os.Create(mappingFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create reverse mapping (int -> class name)
	reverseMap := make(map[byte]string)
	for className, classID := range w.classToInt {
		reverseMap[classID] = className
	}

	// Write as simple JSON
	file.WriteString("{\n")
	for i := byte(0); i < w.nextClassID; i++ {
		className := reverseMap[i]
		if i > 0 {
			file.WriteString(",\n")
		}
		file.WriteString(fmt.Sprintf("  \"%d\": \"%s\"", i, className))
	}
	file.WriteString("\n}\n")

	return nil
}

// ParquetPacket is a simple struct for Parquet without reflection overhead
type ParquetPacket struct {
	Data  []byte `parquet:"data"`
	Class string `parquet:"class,optional"`
}

// ParquetStreamWriter writes packets to Parquet incrementally
type ParquetStreamWriter struct {
	file          *os.File
	writer        *parquet.Writer
	maxPacketSize int
	hasClass      bool
	flushCounter  int // Track writes for periodic flushing
	mutex         sync.Mutex
}

// NewParquetStreamWriter creates a new streaming Parquet writer
func NewParquetStreamWriter(filename string, maxPacketSize int, hasClass bool) (*ParquetStreamWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	// Create simple schema-based writer (no reflection per packet!)
	schema := parquet.SchemaOf(ParquetPacket{})
	writer := parquet.NewWriter(file, schema,
		parquet.Compression(&parquet.Zstd),
		parquet.PageBufferSize(256*1024),
	)

	return &ParquetStreamWriter{
		file:          file,
		writer:        writer,
		maxPacketSize: maxPacketSize,
		hasClass:      hasClass,
		flushCounter:  0,
	}, nil
}

func (w *ParquetStreamWriter) WritePacket(p PacketResult) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Packets are already standardized by parser - write as-is
	// No length modification needed here
	packet := ParquetPacket{
		Data:  p.Data,
		Class: p.Class,
	}

	if err := w.writer.Write(packet); err != nil {
		return err
	}

	w.flushCounter++

	
	if w.flushCounter >= 50000 {
		// Flush parquet buffer to disk
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("flush error: %w", err)
		}
		w.flushCounter = 0

		// Force garbage collection to free memory
		runtime.GC()
		debug.FreeOSMemory()
	}

	return nil
}

func (w *ParquetStreamWriter) Close() error {
	// Final flush before closing
	if err := w.writer.Flush(); err != nil {
		w.file.Close()
		return err
	}
	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return err
	}
	return w.file.Close()
}

// writeCSVOptimized writes packets to CSV with optimizations.
// Packets are expected to be already standardized by the parser.
// For variable-length packets (outputLength==0), all packets are padded to max size for consistent columns.
func writeCSVOptimized(filename string, packets []PacketResult, outputLength int) error {
	if len(packets) == 0 {
		return fmt.Errorf("no packets to write")
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Use buffered writer for better I/O performance
	bufWriter := bufio.NewWriterSize(file, 1024*1024) // 1MB buffer
	defer bufWriter.Flush()

	writer := csv.NewWriter(bufWriter)
	defer writer.Flush()

	// Determine if we have class labels
	hasClassLabels := packets[0].Class != ""

	// For variable-length packets (outputLength==0), pad all to max size for consistent CSV columns
	if outputLength == 0 {
		packets = padToMaxSize(packets)
	}

	// Determine packet size (all packets should now be same size)
	packetSize := len(packets[0].Data)

	// Write header - Format: Byte_0, Byte_1, ..., Byte_N, Class (if present)
	headerSize := packetSize
	if hasClassLabels {
		headerSize++
	}

	header := make([]string, headerSize)
	for i := 0; i < packetSize; i++ {
		header[i] = fmt.Sprintf("Byte_%d", i)
	}
	if hasClassLabels {
		header[packetSize] = "Class"
	}

	if err := writer.Write(header); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}

	// Write data rows
	for _, p := range packets {
		rowSize := len(p.Data)
		if hasClassLabels {
			rowSize++
		}
		row := make([]string, rowSize)

		// Convert bytes to strings
		for i, b := range p.Data {
			row[i] = strconv.Itoa(int(b))
		}

		// Add class label if present
		if hasClassLabels {
			row[len(p.Data)] = p.Class
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("error writing record: %w", err)
		}
	}

	return nil
}

// writeNumpy writes packets to NumPy format (batch mode, in-memory)
// Creates separate files for data and labels (if hasClass)
// Packets are expected to be already standardized by the parser.
func writeNumpy(filename string, packets []PacketResult, outputLength int) error {
	if len(packets) == 0 {
		return fmt.Errorf("no packets to write")
	}

	// Remove extension and get base filename
	baseFilename := strings.TrimSuffix(filename, ".npy")
	baseFilename = strings.TrimSuffix(baseFilename, ".npz")

	// Determine if we have class labels
	hasClassLabels := packets[0].Class != ""

	// For variable-length packets (outputLength==0), pad all to max size for consistent array shape
	if outputLength == 0 {
		packets = padToMaxSize(packets)
	}

	// Determine packet size (all packets should now be same size)
	packetSize := len(packets[0].Data)
	numPackets := len(packets)

	// Write data array
	dataFilename := baseFilename + "_data.npy"
	if err := writeNumpyArray2D(dataFilename, packets, packetSize, numPackets); err != nil {
		return fmt.Errorf("error writing data array: %w", err)
	}

	// Write labels array if present
	if hasClassLabels {
		labelsFilename := baseFilename + "_labels.npy"
		classesFilename := baseFilename + "_classes.json"
		if err := writeNumpyLabels(labelsFilename, classesFilename, packets); err != nil {
			return fmt.Errorf("error writing labels array: %w", err)
		}
	}

	return nil
}

// writeNumpyArray2D writes a 2D uint8 array in NumPy .npy format
func writeNumpyArray2D(filename string, packets []PacketResult, cols, rows int) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	bufWriter := bufio.NewWriterSize(file, 4*1024*1024)
	defer bufWriter.Flush()

	// Write magic number (version 1.0 for maximum compatibility)
	magic := []byte{0x93, 'N', 'P', 'Y', 0x01, 0x00}
	bufWriter.Write(magic)

	// Create header
	headerStr := fmt.Sprintf("{'descr': '|u1', 'fortran_order': False, 'shape': (%d, %d)}", rows, cols)
	
	// Pad header to 64-byte boundary
	headerStr = padNumpyHeader(headerStr)

	// Write header length (uint16 for v1.0)
	headerLen := uint16(len(headerStr))
	binary.Write(bufWriter, binary.LittleEndian, headerLen)

	// Write header
	bufWriter.Write([]byte(headerStr))

	// Write all packet data as raw bytes
	for _, p := range packets {
		bufWriter.Write(p.Data)
	}

	return nil
}

// writeNumpyLabels writes a 1D uint8 array for class labels
func writeNumpyLabels(labelsFilename, classesFilename string, packets []PacketResult) error {
	// Build class name to ID mapping
	classToInt := make(map[string]byte)
	nextClassID := byte(0)

	// First pass: collect unique classes
	for _, p := range packets {
		if p.Class != "" {
			if _, exists := classToInt[p.Class]; !exists {
				classToInt[p.Class] = nextClassID
				nextClassID++
			}
		}
	}

	// Create labels file
	file, err := os.Create(labelsFilename)
	if err != nil {
		return err
	}
	defer file.Close()

	bufWriter := bufio.NewWriterSize(file, 1*1024*1024)
	defer bufWriter.Flush()

	// Write magic number (version 1.0 for maximum compatibility)
	magic := []byte{0x93, 'N', 'P', 'Y', 0x01, 0x00}
	bufWriter.Write(magic)

	// Create header for 1D array
	headerStr := fmt.Sprintf("{'descr': '|u1', 'fortran_order': False, 'shape': (%d,)}", len(packets))
	
	// Pad header to 64-byte boundary
	headerStr = padNumpyHeader(headerStr)

	// Write header length (uint16 for v1.0)
	headerLen := uint16(len(headerStr))
	binary.Write(bufWriter, binary.LittleEndian, headerLen)

	// Write header
	bufWriter.Write([]byte(headerStr))

	// Write labels as uint8
	for _, p := range packets {
		classID := classToInt[p.Class]
		bufWriter.WriteByte(classID)
	}

	// Write class mapping file
	if err := writeClassMappingFile(classesFilename, classToInt); err != nil {
		// Non-fatal, just warn
		fmt.Printf("Warning: failed to write class mapping: %v\n", err)
	}

	return nil
}

// padNumpyHeader pads header to 64-byte alignment
func padNumpyHeader(header string) string {
	// Total header block = 6 (magic+version) + 2 (header_len for v1.0) + len(header)
	totalSize := 6 + 2 + len(header)
	
	// NumPy requires header to end with newline
	header += "\n"
	totalSize += 1
	
	// Pad with spaces to 64-byte boundary (optional but recommended)
	remainder := totalSize % 64
	if remainder != 0 {
		paddingNeeded := 64 - remainder
		header += strings.Repeat(" ", paddingNeeded)
	}
	return header
}

// writeClassMappingFile writes the class ID to name mapping as JSON
func writeClassMappingFile(filename string, classToInt map[string]byte) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create reverse mapping
	reverseMap := make(map[byte]string)
	maxID := byte(0)
	for className, classID := range classToInt {
		reverseMap[classID] = className
		if classID > maxID {
			maxID = classID
		}
	}

	// Write JSON
	file.WriteString("{\n")
	for i := byte(0); i <= maxID; i++ {
		if className, exists := reverseMap[i]; exists {
			if i > 0 {
				file.WriteString(",\n")
			}
			file.WriteString(fmt.Sprintf("  \"%d\": \"%s\"", i, className))
		}
	}
	file.WriteString("\n}\n")

	return nil
}

// writeParquet writes packets to Parquet format with the same schema as CSV.
// Packets are expected to be already standardized by the parser.
// For variable-length packets (outputLength==0), all packets are padded to max size for consistent schema.
func writeParquet(filename string, packets []PacketResult, outputLength int) error {
	if len(packets) == 0 {
		return fmt.Errorf("no packets to write")
	}

	// Determine if we have class labels
	hasClassLabels := packets[0].Class != ""

	// For variable-length packets (outputLength==0), pad all to max size for consistent schema
	if outputLength == 0 {
		packets = padToMaxSize(packets)
	}

	// Determine packet size (all packets should now be same size)
	packetSize := len(packets[0].Data)

	// Build dynamic struct type with byte columns and optional class column
	fields := make([]reflect.StructField, 0, packetSize+1)

	// Add byte columns
	for i := 0; i < packetSize; i++ {
		fields = append(fields, reflect.StructField{
			Name: fmt.Sprintf("Byte_%d", i),
			Type: reflect.TypeOf(int32(0)),
			Tag:  reflect.StructTag(fmt.Sprintf(`parquet:"Byte_%d"`, i)),
		})
	}

	// Add class column if present
	if hasClassLabels {
		fields = append(fields, reflect.StructField{
			Name: "Class",
			Type: reflect.TypeOf(""),
			Tag:  `parquet:"Class"`,
		})
	}

	// Create the dynamic struct type
	structType := reflect.StructOf(fields)

	// Convert packets to dynamic structs
	rowValues := make([]interface{}, len(packets))
	for idx, p := range packets {
		rowPtr := reflect.New(structType)
		row := rowPtr.Elem()

		// Set byte values (packets are already padded to consistent size)
		for i := 0; i < packetSize; i++ {
			if i < len(p.Data) {
				row.Field(i).SetInt(int64(p.Data[i]))
			} else {
				row.Field(i).SetInt(0) // Safety padding
			}
		}

		// Set class value if present
		if hasClassLabels {
			row.Field(packetSize).SetString(p.Class)
		}

		rowValues[idx] = rowPtr.Interface()
	}

	// Create output file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Get schema from dynamic struct
	schema := parquet.SchemaOf(rowValues[0])

	// Create writer using reflection to handle dynamic type
	writer := parquet.NewWriter(file, schema, parquet.Compression(&parquet.Zstd))
	defer writer.Close()

	// Write rows using reflection
	for _, rowPtr := range rowValues {
		if err := writer.Write(rowPtr); err != nil {
			return fmt.Errorf("error writing row: %w", err)
		}
	}

	return nil
}
