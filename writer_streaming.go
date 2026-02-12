package main

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"os"
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

// CSVStreamWriter writes packets to CSV incrementally.
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

// NewCSVStreamWriter creates a new streaming CSV writer.
func NewCSVStreamWriter(filename string, maxPacketSize int, hasClass bool) (*CSVStreamWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	// Reduced buffer size for WSL2 stability (128KB instead of 4MB).
	bufWriter := bufio.NewWriterSize(file, 128*1024)
	csvWriter := csv.NewWriter(bufWriter)

	// Pre-allocate reusable row buffer.
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

	// Write header.
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

	// Use pre-allocated buffer if size matches, otherwise create new one.
	var row []string
	if rowSize == len(w.rowBuffer) {
		row = w.rowBuffer
	} else {
		row = make([]string, rowSize)
	}

	// Convert bytes to strings.
	for i, b := range data {
		row[i] = strconv.Itoa(int(b))
	}

	// Add class label if present.
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
	// Final flush before closing.
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

// NumpyStreamWriter writes packets to NumPy .npy format incrementally.
// Outputs uint8 array matching CSV schema with optional class labels.
type NumpyStreamWriter struct {
	dataFile        *os.File      // Main data file
	dataBufWriter   *bufio.Writer // Buffer for data
	labelsFile      *os.File      // Separate file for labels (if hasClass)
	labelsBufWriter *bufio.Writer // Buffer for labels
	maxPacketSize   int
	hasClass        bool
	packetCount     int64
	flushCounter    int
	mutex           sync.Mutex
	classToInt      map[string]byte // Map class names to integers
	nextClassID     byte            // Next available class ID
	baseFilename    string          // Base filename without extension
}

// NewNumpyStreamWriter creates a new streaming NumPy writer.
// If hasClass is true, creates two files: <basename>_data.npy and <basename>_labels.npy.
func NewNumpyStreamWriter(filename string, maxPacketSize int, hasClass bool) (*NumpyStreamWriter, error) {
	// Remove extension if present and store base filename.
	baseFilename := strings.TrimSuffix(filename, ".npy")
	baseFilename = strings.TrimSuffix(baseFilename, ".npz")

	// Create main data file.
	dataFilename := baseFilename + "_data.npy"
	dataFile, err := os.Create(dataFilename)
	if err != nil {
		return nil, fmt.Errorf("failed to create data file: %w", err)
	}

	dataBufWriter := bufio.NewWriterSize(dataFile, 4*1024*1024) // 4MB buffer

	w := &NumpyStreamWriter{
		dataFile:      dataFile,
		dataBufWriter: dataBufWriter,
		maxPacketSize: maxPacketSize,
		hasClass:      hasClass,
		packetCount:   0,
		flushCounter:  0,
		classToInt:    make(map[string]byte),
		nextClassID:   0,
		baseFilename:  baseFilename,
	}

	// Write placeholder header for data file.
	if err := w.writePlaceholderHeader(dataBufWriter, maxPacketSize); err != nil {
		dataFile.Close()
		return nil, err
	}

	// Create labels file if needed.
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

		// Write placeholder header for labels file (1D array of uint8).
		err = w.writePlaceholderHeader(labelsBufWriter, 0) // 0 = 1D array
		if err != nil {
			dataFile.Close()
			labelsFile.Close()
			return nil, err
		}
	}

	return w, nil
}

// writePlaceholderHeader writes a NumPy header with shape (0, cols) that will be updated later.
// If cols is 0, writes a 1D array header for labels.
func (w *NumpyStreamWriter) writePlaceholderHeader(writer *bufio.Writer, cols int) error {
	if err := writeNumpyMagic(writer); err != nil {
		return err
	}

	// Create header with rows=0 as placeholder.
	headerStr := createNumpyHeader(0, cols)

	// Write header length as uint16 little-endian (2 bytes for version 1.0).
	headerLen := uint16(len(headerStr))
	if err := binary.Write(writer, binary.LittleEndian, headerLen); err != nil {
		return err
	}

	// Write header string.
	if _, err := writer.Write([]byte(headerStr)); err != nil {
		return err
	}
	return nil
}

// WritePacket writes a packet to NumPy format (raw binary for data, integer for class).
func (w *NumpyStreamWriter) WritePacket(p PacketResult) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Write packet data as raw uint8 bytes (NO string conversion!).
	if _, err := w.dataBufWriter.Write(p.Data); err != nil {
		return fmt.Errorf("error writing data: %w", err)
	}

	// Write class label if present.
	if w.hasClass && p.Class != "" {
		// Map class name to integer.
		classID, exists := w.classToInt[p.Class]
		if !exists {
			classID = w.nextClassID
			w.classToInt[p.Class] = classID
			w.nextClassID++
		}

		// Write class ID as single uint8 byte.
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

		// Force garbage collection to free memory.
		runtime.GC()
		debug.FreeOSMemory()
	}

	return nil
}

// Close finalizes the NumPy file by updating the header with actual packet count.
func (w *NumpyStreamWriter) Close() error {
	// Final flush of all buffers.
	if err := w.dataBufWriter.Flush(); err != nil {
		return fmt.Errorf("error flushing data buffer: %w", err)
	}
	if w.hasClass {
		if err := w.labelsBufWriter.Flush(); err != nil {
			return fmt.Errorf("error flushing labels buffer: %w", err)
		}
	}

	// Update data file header with actual packet count.
	if err := w.updateHeader(w.dataFile, w.maxPacketSize, w.packetCount); err != nil {
		w.dataFile.Close()
		if w.hasClass {
			w.labelsFile.Close()
		}
		return fmt.Errorf("error updating data header: %w", err)
	}

	// Update labels file header if present.
	if w.hasClass {
		if err := w.updateHeader(w.labelsFile, 0, w.packetCount); err != nil {
			w.dataFile.Close()
			w.labelsFile.Close()
			return fmt.Errorf("error updating labels header: %w", err)
		}
	}

	// Close files.
	if err := w.dataFile.Close(); err != nil {
		return err
	}
	if w.hasClass {
		if err := w.labelsFile.Close(); err != nil {
			return err
		}

		// Write class mapping to a JSON file for reference.
		if err := w.writeClassMapping(); err != nil {
			// Non-fatal error, just log it.
			fmt.Printf("Warning: failed to write class mapping: %v\n", err)
		}
	}

	return nil
}

// updateHeader seeks back to the file header and updates it with the actual row count.
func (w *NumpyStreamWriter) updateHeader(file *os.File, cols int, rows int64) error {
	// Seek to position after magic+version (8 bytes) and before header_len (2 bytes for v1.0).
	// Format: \x93NUMPY (6) + \x01\x00 (2) = 8 bytes.
	if _, err := file.Seek(8, 0); err != nil {
		return err
	}

	// Create header with actual row count.
	headerStr := createNumpyHeader(rows, cols)

	// Write updated header length (uint16 for v1.0).
	headerLen := uint16(len(headerStr))
	if err := binary.Write(file, binary.LittleEndian, headerLen); err != nil {
		return err
	}

	// Write updated header string.
	if _, err := file.Write([]byte(headerStr)); err != nil {
		return err
	}

	return nil
}

// writeClassMapping writes the class name to integer mapping as a JSON file.
func (w *NumpyStreamWriter) writeClassMapping() error {
	mappingFile := w.baseFilename + "_classes.json"
	return writeClassMappingFile(mappingFile, w.classToInt)
}

// ParquetPacket is a simple struct for Parquet without reflection overhead.
type ParquetPacket struct {
	Data  []byte `parquet:"data"`
	Class string `parquet:"class,optional"`
}

// ParquetStreamWriter writes packets to Parquet incrementally.
type ParquetStreamWriter struct {
	file         *os.File
	writer       *parquet.Writer
	flushCounter int // Track writes for periodic flushing
	mutex        sync.Mutex
}

// NewParquetStreamWriter creates a new streaming Parquet writer.
func NewParquetStreamWriter(filename string, maxPacketSize int, hasClass bool) (*ParquetStreamWriter, error) {
	_ = maxPacketSize
	_ = hasClass

	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	// Create simple schema-based writer (no reflection per packet!).
	schema := parquet.SchemaOf(ParquetPacket{})
	writer := parquet.NewWriter(file, schema,
		parquet.Compression(&parquet.Zstd),
		parquet.PageBufferSize(256*1024),
	)

	return &ParquetStreamWriter{
		file:         file,
		writer:       writer,
		flushCounter: 0,
	}, nil
}

func (w *ParquetStreamWriter) WritePacket(p PacketResult) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Packets are already standardized by parser - write as-is.
	// No length modification needed here.
	packet := ParquetPacket{
		Data:  p.Data,
		Class: p.Class,
	}

	if err := w.writer.Write(packet); err != nil {
		return err
	}

	w.flushCounter++

	if w.flushCounter >= 50000 {
		// Flush parquet buffer to disk.
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("flush error: %w", err)
		}
		w.flushCounter = 0

		// Force garbage collection to free memory.
		runtime.GC()
		debug.FreeOSMemory()
	}

	return nil
}

func (w *ParquetStreamWriter) Close() error {
	// Final flush before closing.
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
