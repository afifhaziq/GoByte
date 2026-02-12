package main

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
)

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

	// Use buffered writer for better I/O performance.
	bufWriter := bufio.NewWriterSize(file, 1024*1024) // 1MB buffer
	defer bufWriter.Flush()

	writer := csv.NewWriter(bufWriter)
	defer writer.Flush()

	// Determine if we have class labels.
	hasClassLabels := packets[0].Class != ""

	// For variable-length packets (outputLength==0), pad all to max size for consistent CSV columns.
	if outputLength == 0 {
		packets = padToMaxSize(packets)
	}

	// Determine packet size (all packets should now be same size).
	packetSize := len(packets[0].Data)

	// Write header - Format: Byte_0, Byte_1, ..., Byte_N, Class (if present).
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

	// Write data rows.
	for _, p := range packets {
		rowSize := len(p.Data)
		if hasClassLabels {
			rowSize++
		}
		row := make([]string, rowSize)

		// Convert bytes to strings.
		for i, b := range p.Data {
			row[i] = strconv.Itoa(int(b))
		}

		// Add class label if present.
		if hasClassLabels {
			row[len(p.Data)] = p.Class
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("error writing record: %w", err)
		}
	}

	return nil
}

// writeNumpy writes packets to NumPy format (batch mode, in-memory).
// Creates separate files for data and labels (if hasClass).
// Packets are expected to be already standardized by the parser.
func writeNumpy(filename string, packets []PacketResult, outputLength int) error {
	if len(packets) == 0 {
		return fmt.Errorf("no packets to write")
	}

	// Remove extension and get base filename.
	baseFilename := strings.TrimSuffix(filename, ".npy")
	baseFilename = strings.TrimSuffix(baseFilename, ".npz")

	// Determine if we have class labels.
	hasClassLabels := packets[0].Class != ""

	// For variable-length packets (outputLength==0), pad all to max size for consistent array shape.
	if outputLength == 0 {
		packets = padToMaxSize(packets)
	}

	// Determine packet size (all packets should now be same size).
	packetSize := len(packets[0].Data)
	numPackets := len(packets)

	// Write data array.
	dataFilename := baseFilename + "_data.npy"
	if err := writeNumpyArray2D(dataFilename, packets, packetSize, numPackets); err != nil {
		return fmt.Errorf("error writing data array: %w", err)
	}

	// Write labels array if present.
	if hasClassLabels {
		labelsFilename := baseFilename + "_labels.npy"
		classesFilename := baseFilename + "_classes.json"
		if err := writeNumpyLabels(labelsFilename, classesFilename, packets); err != nil {
			return fmt.Errorf("error writing labels array: %w", err)
		}
	}

	return nil
}

// writeNumpyArray2D writes a 2D uint8 array in NumPy .npy format.
func writeNumpyArray2D(filename string, packets []PacketResult, cols, rows int) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	bufWriter := bufio.NewWriterSize(file, 4*1024*1024)
	defer bufWriter.Flush()

	if err := writeNumpyMagic(bufWriter); err != nil {
		return err
	}

	// Create header.
	headerStr := createNumpyHeader(int64(rows), cols)

	// Write header length (uint16 for v1.0).
	headerLen := uint16(len(headerStr))
	if err := binary.Write(bufWriter, binary.LittleEndian, headerLen); err != nil {
		return err
	}

	// Write header.
	if _, err := bufWriter.Write([]byte(headerStr)); err != nil {
		return err
	}

	// Write all packet data as raw bytes.
	for _, p := range packets {
		if _, err := bufWriter.Write(p.Data); err != nil {
			return err
		}
	}

	return nil
}

// writeNumpyLabels writes a 1D uint8 array for class labels.
func writeNumpyLabels(labelsFilename, classesFilename string, packets []PacketResult) error {
	// Build class name to ID mapping.
	classToInt := make(map[string]byte)
	nextClassID := byte(0)

	// First pass: collect unique classes.
	for _, p := range packets {
		if p.Class != "" {
			if _, exists := classToInt[p.Class]; !exists {
				classToInt[p.Class] = nextClassID
				nextClassID++
			}
		}
	}

	// Create labels file.
	file, err := os.Create(labelsFilename)
	if err != nil {
		return err
	}
	defer file.Close()

	bufWriter := bufio.NewWriterSize(file, 1*1024*1024)
	defer bufWriter.Flush()

	if err := writeNumpyMagic(bufWriter); err != nil {
		return err
	}

	// Create header for 1D array.
	headerStr := createNumpyHeader(int64(len(packets)), 0)

	// Write header length (uint16 for v1.0).
	headerLen := uint16(len(headerStr))
	if err := binary.Write(bufWriter, binary.LittleEndian, headerLen); err != nil {
		return err
	}

	// Write header.
	if _, err := bufWriter.Write([]byte(headerStr)); err != nil {
		return err
	}

	// Write labels as uint8.
	for _, p := range packets {
		classID := classToInt[p.Class]
		if err := bufWriter.WriteByte(classID); err != nil {
			return err
		}
	}

	// Write class mapping file.
	if err := writeClassMappingFile(classesFilename, classToInt); err != nil {
		// Non-fatal, just warn.
		fmt.Printf("Warning: failed to write class mapping: %v\n", err)
	}

	return nil
}

// writeParquet writes packets to Parquet format with the same schema as CSV.
// Packets are expected to be already standardized by the parser.
// For variable-length packets (outputLength==0), all packets are padded to max size for consistent schema.
func writeParquet(filename string, packets []PacketResult, outputLength int) error {
	if len(packets) == 0 {
		return fmt.Errorf("no packets to write")
	}

	// Determine if we have class labels.
	hasClassLabels := packets[0].Class != ""

	// For variable-length packets (outputLength==0), pad all to max size for consistent schema.
	if outputLength == 0 {
		packets = padToMaxSize(packets)
	}

	// Determine packet size (all packets should now be same size).
	packetSize := len(packets[0].Data)

	// Build dynamic struct type with byte columns and optional class column.
	fields := make([]reflect.StructField, 0, packetSize+1)

	// Add byte columns.
	for i := 0; i < packetSize; i++ {
		fields = append(fields, reflect.StructField{
			Name: fmt.Sprintf("Byte_%d", i),
			Type: reflect.TypeOf(int32(0)),
			Tag:  reflect.StructTag(fmt.Sprintf(`parquet:"Byte_%d"`, i)),
		})
	}

	// Add class column if present.
	if hasClassLabels {
		fields = append(fields, reflect.StructField{
			Name: "Class",
			Type: reflect.TypeOf(""),
			Tag:  `parquet:"Class"`,
		})
	}

	// Create the dynamic struct type.
	structType := reflect.StructOf(fields)

	// Convert packets to dynamic structs.
	rowValues := make([]interface{}, len(packets))
	for idx, p := range packets {
		rowPtr := reflect.New(structType)
		row := rowPtr.Elem()

		// Set byte values (packets are already padded to consistent size).
		for i := 0; i < packetSize; i++ {
			if i < len(p.Data) {
				row.Field(i).SetInt(int64(p.Data[i]))
			} else {
				row.Field(i).SetInt(0) // Safety padding
			}
		}

		// Set class value if present.
		if hasClassLabels {
			row.Field(packetSize).SetString(p.Class)
		}

		rowValues[idx] = rowPtr.Interface()
	}

	// Create output file.
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Get schema from dynamic struct.
	schema := parquet.SchemaOf(rowValues[0])

	// Create writer using reflection to handle dynamic type.
	writer := parquet.NewWriter(file, schema, parquet.Compression(&parquet.Zstd))
	defer writer.Close()

	// Write rows using reflection.
	for _, rowPtr := range rowValues {
		if err := writer.Write(rowPtr); err != nil {
			return fmt.Errorf("error writing row: %w", err)
		}
	}

	return nil
}
