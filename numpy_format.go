package main

import (
	"fmt"
	"os"
	"strings"
)

var numpyMagicV10 = []byte{0x93, 'N', 'U', 'M', 'P', 'Y', 0x01, 0x00}

// writeNumpyMagic writes the NumPy v1.0 magic string + version bytes.
func writeNumpyMagic(writer interface{ Write([]byte) (int, error) }) error {
	_, err := writer.Write(numpyMagicV10)
	return err
}

// createNumpyHeader creates a NumPy header dictionary string with proper padding.
func createNumpyHeader(rows int64, cols int) string {
	var headerStr string
	if cols > 0 {
		headerStr = fmt.Sprintf("{'descr': '|u1', 'fortran_order': False, 'shape': (%d, %d)}", rows, cols)
	} else {
		headerStr = fmt.Sprintf("{'descr': '|u1', 'fortran_order': False, 'shape': (%d,)}", rows)
	}

	return padNumpyHeader(headerStr)
}

// padNumpyHeader pads header to 64-byte alignment.
func padNumpyHeader(header string) string {
	// Total header block = 8 (magic+version) + 2 (header_len for v1.0) + len(header)
	// Breakdown: \x93NUMPY (6) + version bytes (2) + header_len uint16 (2) + header
	totalSize := 10 + len(header)

	// Pad to at least 128 bytes total for consistency.
	minSize := 128
	if totalSize < minSize {
		paddingNeeded := minSize - totalSize - 1 // -1 for the newline
		header += strings.Repeat(" ", paddingNeeded)
		totalSize = minSize - 1
	}

	// Then align to 64-byte boundary for performance.
	remainder := (totalSize + 1) % 64 // +1 for the newline
	if remainder != 0 {
		paddingNeeded := 64 - remainder
		header += strings.Repeat(" ", paddingNeeded)
	}

	// Add newline at the very end (required by NumPy format).
	header += "\n"

	return header
}

// writeClassMappingFile writes the class ID to name mapping as JSON.
func writeClassMappingFile(filename string, classToInt map[string]byte) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create reverse mapping.
	reverseMap := make(map[byte]string)
	maxID := byte(0)
	for className, classID := range classToInt {
		reverseMap[classID] = className
		if classID > maxID {
			maxID = classID
		}
	}

	// Write JSON.
	if _, err := file.WriteString("{\n"); err != nil {
		return err
	}
	for i := byte(0); i <= maxID; i++ {
		if className, exists := reverseMap[i]; exists {
			if i > 0 {
				if _, err := file.WriteString(",\n"); err != nil {
					return err
				}
			}
			if _, err := file.WriteString(fmt.Sprintf("  \"%d\": \"%s\"", i, className)); err != nil {
				return err
			}
		}
	}
	if _, err := file.WriteString("\n}\n"); err != nil {
		return err
	}

	return nil
}
