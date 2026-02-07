package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"

	"github.com/parquet-go/parquet-go"
)

// StreamWriter interface for incremental packet writing (memory-efficient)
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

	// Pad packet if needed
	data := p.Data
	if len(data) < w.maxPacketSize {
		padded := make([]byte, w.maxPacketSize)
		copy(padded, data)
		data = padded
	} else if len(data) > w.maxPacketSize {
		data = data[:w.maxPacketSize]
	}

	// Reuse row buffer instead of allocating new one
	for i, b := range data {
		w.rowBuffer[i] = strconv.Itoa(int(b))
	}

	if w.hasClass {
		w.rowBuffer[w.maxPacketSize] = p.Class
	}

	if err := w.csvWriter.Write(w.rowBuffer); err != nil {
		return err
	}

	w.flushCounter++

	// CRITICAL: Flush every 10K packets to prevent memory buildup
	if w.flushCounter >= 10000 {
		w.csvWriter.Flush()
		if err := w.csvWriter.Error(); err != nil {
			return fmt.Errorf("csv flush error: %w", err)
		}
		w.bufWriter.Flush()
		w.flushCounter = 0

		// Force garbage collection to free memory
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

// ParquetPacket is a simple struct for Parquet without reflection overhead
type ParquetPacket struct {
	Data  []byte `parquet:"data"`
	Class string `parquet:"class,optional"`
}

// ParquetStreamWriter writes packets to Parquet incrementally (memory-efficient)
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
		parquet.PageBufferSize(256*1024), // Smaller page buffer for memory efficiency
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

	// Simple struct write (no reflection!)
	packet := ParquetPacket{
		Data:  p.Data,
		Class: p.Class,
	}

	if err := w.writer.Write(packet); err != nil {
		return err
	}

	w.flushCounter++

	// CRITICAL: Flush every 50K packets to prevent memory buildup
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

// writeCSVOptimized writes packets to CSV with optimizations:

func writeCSVOptimized(filename string, packets []PacketResult, outputLength int) error {
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

	// Determine if we have class labels (check first packet)
	hasClassLabels := len(packets) > 0 && packets[0].Class != ""

	// Determine max packet size for variable-length packets (outputLength == 0)
	maxPacketSize := outputLength
	if outputLength == 0 && len(packets) > 0 {
		// Find the maximum packet size
		for _, p := range packets {
			if len(p.Data) > maxPacketSize {
				maxPacketSize = len(p.Data)
			}
		}
	}

	// Write header - Format: Byte_0, Byte_1, ..., Byte_N, Class (if present)
	headerSize := maxPacketSize
	if hasClassLabels {
		headerSize += 1 // Add Class column at the end
	}

	header := make([]string, headerSize)

	// Byte columns first
	for i := 0; i < maxPacketSize; i++ {
		header[i] = fmt.Sprintf("Byte_%d", i)
	}

	// Class column last (if present)
	if hasClassLabels {
		header[maxPacketSize] = "Class"
	}

	if err := writer.Write(header); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}

	// Write data rows
	for _, p := range packets {
		// Prepare row based on actual packet size
		currentRowSize := len(p.Data)
		if hasClassLabels {
			currentRowSize += 1
		}
		row := make([]string, currentRowSize)

		// Byte columns first
		for i, b := range p.Data {
			row[i] = strconv.Itoa(int(b))
		}

		// Class column last (if present)
		if hasClassLabels {
			row[len(p.Data)] = p.Class
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("error writing record: %w", err)
		}
	}

	return nil
}

// writeParquet writes packets to Parquet format with the same schema as CSV.
func writeParquet(filename string, packets []PacketResult, outputLength int) error {
	if len(packets) == 0 {
		return fmt.Errorf("no packets to write")
	}

	// Determine if we have class labels (check first packet)
	hasClassLabels := len(packets) > 0 && packets[0].Class != ""

	// Determine max packet size for variable-length packets
	maxPacketSize := outputLength
	if outputLength == 0 {
		for _, p := range packets {
			if len(p.Data) > maxPacketSize {
				maxPacketSize = len(p.Data)
			}
		}
	}

	// Build dynamic struct type with byte columns and optional class column
	fields := make([]reflect.StructField, 0, maxPacketSize+1)

	// Add byte columns
	for i := 0; i < maxPacketSize; i++ {
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

		// Set byte values
		for i := 0; i < maxPacketSize; i++ {
			if i < len(p.Data) {
				row.Field(i).SetInt(int64(p.Data[i]))
			} else {
				row.Field(i).SetInt(0) // Pad with zeros
			}
		}

		// Set class value if present
		if hasClassLabels {
			row.Field(maxPacketSize).SetString(p.Class)
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
