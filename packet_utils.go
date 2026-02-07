// The functions are separated to support both streaming (low memory) and
// batch (high memory) processing modes efficiently.

// standardizePacketLength is the primary API for per-packet length normalization.
// Used by all processing modes (streaming, batch, per-file).

package main

// If outputLength > 0: truncate or pad to exactly outputLength bytes
// If outputLength == 0: keep original size (no modification)
func standardizePacketLength(data []byte, outputLength int) []byte {
	if outputLength == 0 {
		// Keep original size - no truncation or padding
		return data
	}

	// Apply truncation/padding to specified length
	return truncatePad(data, outputLength)
}

// truncatePad returns a slice of exactly 'length' bytes.
// If data is longer, it's truncated. If shorter, it's padded with zeros.
func truncatePad(data []byte, length int) []byte {
	res := make([]byte, length)
	copy(res, data)
	return res
}

// determineMaxPacketSize calculates the maximum packet size from a slice of packets.
// Used for variable-length packet scenarios (when outputLength == 0).
func determineMaxPacketSize(packets []PacketResult) int {
	maxSize := 0
	for _, p := range packets {
		if len(p.Data) > maxSize {
			maxSize = len(p.Data)
		}
	}
	return maxSize
}

// padToMaxSize pads all packets in a slice to the maximum packet size.
// This is used for non-streaming modes with variable-length packets
// to ensure consistent column count in output files.
func padToMaxSize(packets []PacketResult) []PacketResult {
	if len(packets) == 0 {
		return packets
	}

	maxSize := determineMaxPacketSize(packets)
	if maxSize == 0 {
		return packets
	}

	// Pad all packets to max size
	for i := range packets {
		if len(packets[i].Data) < maxSize {
			padded := make([]byte, maxSize)
			copy(padded, packets[i].Data)
			packets[i].Data = padded
		}
	}

	return packets
}
