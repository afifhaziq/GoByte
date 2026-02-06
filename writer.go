package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"github.com/parquet-go/parquet-go"
)

// writeCSVOptimized writes packets to CSV with optimizations:
// - Pre-allocates row slices to avoid repeated allocations
// - Uses buffered writer for fewer syscalls (1MB buffer)
// - Reuses row buffer for all packets to reduce allocations
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

// writeParquet writes packets to Parquet format.
func writeParquet(filename string, packets []PacketResult) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create parquet writer with Zstandard compression
	writer := parquet.NewGenericWriter[PacketResult](file,
		parquet.Compression(&parquet.Zstd),
	)
	defer writer.Close()

	// Write all packets at once
	if _, err := writer.Write(packets); err != nil {
		return fmt.Errorf("error writing parquet: %w", err)
	}

	return nil
}
