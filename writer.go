package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"strconv"

	"github.com/parquet-go/parquet-go"
)

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
