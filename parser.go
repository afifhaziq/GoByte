package main

import (
	"sync"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
)

// PacketResult struct to keep track of order and packet data
type PacketResult struct {
	Index        int     `parquet:"index"`
	OriginalSize int     `parquet:"original_size"`
	Data         []uint8 `parquet:"data"`
}

// PacketJob struct to pass to workers
type PacketJob struct {
	Index  int
	Packet gopacket.Packet
}

// truncatePad returns a slice of exactly 'length' bytes.
// If data is longer, it's truncated. If shorter, it's padded with zeros.
func truncatePad(data []byte, length int) []byte {
	res := make([]byte, length)
	copy(res, data)
	return res
}

// worker processes packets from the jobs channel and sends results to the results channel.
// This is the core packet processing logic that runs in parallel.
func worker(jobs <-chan PacketJob, results chan<- PacketResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		// HEAVY LIFTING: Decoding happens here when we access layers
		ethLayer := job.Packet.Layer(layers.LayerTypeEthernet)

		if ethLayer != nil {
			eth, _ := ethLayer.(*layers.Ethernet)

			// Extract payload (strips Ethernet header)
			payload := eth.LayerPayload()

			// Important: Depending on how gopacket is configured,
			// 'payload' might point to a memory buffer that gets reused.
			// It is safer to make a copy for the final list.
			dataCopy := make([]uint8, len(payload))
			copy(dataCopy, payload)

			results <- PacketResult{
				Index: job.Index,
				Data:  dataCopy,
			}
		}
	}
}
