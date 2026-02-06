package main

import (
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"sort"
	"sync"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
)

// PacketResult struct to keep track of order and packet data
type PacketResult struct {
	Index        int     `parquet:"index" csv:"index"`
	OriginalSize int     `parquet:"original_size" csv:"original_size"`
	Data         []uint8 `parquet:"data" csv:"-"`
	Class        string  `parquet:"class" csv:"class"`
	FileName     string  `parquet:"filename" csv:"filename"`
}

// PacketJob struct to pass to workers
type PacketJob struct {
	Index    int
	Packet   gopacket.Packet
	Class    string
	FileName string
}

// FileJob struct for file-level parallelism
type FileJob struct {
	FilePath string
	Class    string
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
				Index:    job.Index,
				Data:     dataCopy,
				Class:    job.Class,
				FileName: job.FileName,
			}
		}
	}
}

// processFile processes a single PCAP/PCAPNG file and returns all packets with metadata.
// This function uses packet-level parallelism with worker goroutines.
func processFile(fileJob FileJob, outputLength int, sortPackets bool, workersPerFile int) ([]PacketResult, error) {
	// Open PCAP file
	handle, err := pcap.OpenOffline(fileJob.FilePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open file %s: %w", fileJob.FilePath, err)
	}
	defer handle.Close()

	fileName := filepath.Base(fileJob.FilePath)

	// Setup channels for packet processing
	jobs := make(chan PacketJob, 256)
	results := make(chan PacketResult, 256)

	// Start workers for this file
	var wg sync.WaitGroup
	for w := 0; w < workersPerFile; w++ {
		wg.Add(1)
		go worker(jobs, results, &wg)
	}

	// Start collector goroutine
	finalPackets := make([]PacketResult, 0, 10000)
	done := make(chan bool)
	go func() {
		for res := range results {
			finalPackets = append(finalPackets, res)
		}
		done <- true
	}()

	// Read and distribute packets to workers
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	packetSource.DecodeOptions = gopacket.DecodeOptions{Lazy: true, NoCopy: true}

	counter := 0
	for packet := range packetSource.Packets() {
		jobs <- PacketJob{
			Index:    counter,
			Packet:   packet,
			Class:    fileJob.Class,
			FileName: fileName,
		}
		counter++
	}

	// Shutdown
	close(jobs)
	wg.Wait()
	close(results)
	<-done

	// Sort if requested
	if sortPackets {
		sort.Slice(finalPackets, func(i, j int) bool {
			return finalPackets[i].Index < finalPackets[j].Index
		})
	}

	// Truncate/pad all packets (only if outputLength > 0)
	for i := range finalPackets {
		finalPackets[i].OriginalSize = len(finalPackets[i].Data)
		if outputLength > 0 {
			finalPackets[i].Data = truncatePad(finalPackets[i].Data, outputLength)
		}
		// If outputLength == 0, keep original size (no padding/truncating)
	}

	return finalPackets, nil
}

// processFilesParallel processes multiple files with limited parallelism.
// Each file is processed with its own set of packet workers.
func processFilesParallel(fileJobs []FileJob, outputLength int, sortPackets bool, maxConcurrentFiles int) []PacketResult {
	// Calculate workers per file
	totalCores := runtime.NumCPU()
	workersPerFile := totalCores / maxConcurrentFiles
	if workersPerFile < 1 {
		workersPerFile = 1
	}

	fmt.Printf("Processing %d files with %d concurrent files, %d workers per file\n\n",
		len(fileJobs), maxConcurrentFiles, workersPerFile)

	// Create channel for file jobs
	fileChannel := make(chan FileJob, len(fileJobs))
	for _, job := range fileJobs {
		fileChannel <- job
	}
	close(fileChannel)

	// Collect results from all files
	var resultsMutex sync.Mutex
	allResults := make([]PacketResult, 0, 100000)

	// Start file processors
	var wg sync.WaitGroup
	for i := 0; i < maxConcurrentFiles; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for fileJob := range fileChannel {
				fmt.Printf("[Worker %d] Processing %s (class: %s)\n", workerID, filepath.Base(fileJob.FilePath), fileJob.Class)

				packets, err := processFile(fileJob, outputLength, sortPackets, workersPerFile)
				if err != nil {
					log.Printf("[Worker %d] Error processing %s: %v\n", workerID, fileJob.FilePath, err)
					continue
				}

				fmt.Printf("[Worker %d] Processed %s: %d packets\n", workerID, filepath.Base(fileJob.FilePath), len(packets))

				// Add results to global list (thread-safe)
				resultsMutex.Lock()
				allResults = append(allResults, packets...)
				resultsMutex.Unlock()
			}
		}(i)
	}

	wg.Wait()
	return allResults
}
