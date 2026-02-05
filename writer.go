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

	// Write header - pre-allocate full size
	header := make([]string, 2+outputLength)
	header[0] = "Index"
	header[1] = "OriginalSize"
	for i := 0; i < outputLength; i++ {
		header[2+i] = fmt.Sprintf("Byte_%d", i)
	}

	if err := writer.Write(header); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}

	// Pre-allocate row buffer to reuse for all packets
	row := make([]string, 2+outputLength)

	// Write data rows
	for _, p := range packets {
		row[0] = strconv.Itoa(p.Index)
		row[1] = strconv.Itoa(p.OriginalSize)

		// Convert bytes to strings efficiently
		for i, b := range p.Data {
			row[2+i] = strconv.Itoa(int(b))
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("error writing record: %w", err)
		}
	}

	return nil
}

// writeParquet writes packets to Parquet format.
// Parquet is a columnar storage format that is:
// - 10-100x smaller than CSV (compressed)
// - Much faster to read for ML training (columnar layout)
// - Native support in pandas, PyTorch, TensorFlow
// - Better for large datasets
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
