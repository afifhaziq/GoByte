package main

import (
	"fmt"
	"log"
	"os"
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

// Note: truncatePad has been moved to packet_utils.go for better modularity

// maskIPAddresses masks source and destination IP addresses in the packet.
// It handles both IPv4 and IPv6 packets.
func maskIPAddresses(data []byte) []byte {
	if len(data) < 20 {
		// Too short to be a valid IP packet
		return data
	}

	// Check IP version (upper 4 bits of first byte)
	version := data[0] >> 4

	switch version {
	case 4: // IPv4
		return maskIPv4(data)
	case 6: // IPv6
		return maskIPv6(data)
	default:
		// Not an IP packet, return as-is
		return data
	}
}

// maskIPv4 masks IPv4 source and destination addresses
func maskIPv4(data []byte) []byte {
	if len(data) < 20 {
		return data
	}

	// IPv4 header structure:
	// Byte 0: Version (4 bits) + IHL (4 bits)
	// Bytes 12-15: Source IP
	// Bytes 16-19: Destination IP

	// Extract IHL (Internet Header Length) from lower 4 bits of first byte
	// IHL specifies header length in 32-bit words (minimum 5, maximum 15)
	ihl := int(data[0]&0x0F) * 4 // Convert to bytes

	// Validate header length
	if len(data) < ihl || ihl < 20 {
		return data // Invalid header
	}

	// Zero out source IP (bytes 12-15)
	for i := 12; i < 16; i++ {
		data[i] = 0
	}

	// Zero out destination IP (bytes 16-19)
	for i := 16; i < 20; i++ {
		data[i] = 0
	}

	return data
}

// maskIPv6 masks IPv6 source and destination addresses
func maskIPv6(data []byte) []byte {
	if len(data) < 40 {
		return data
	}

	// IPv6 header structure (fixed 40 bytes):
	// Bytes 0-3:   Version (4 bits) + Traffic Class (8 bits) + Flow Label (20 bits)
	// Bytes 8-23:  Source IP (128 bits = 16 bytes)
	// Bytes 24-39: Destination IP (128 bits = 16 bytes)

	// Zero out source IP (bytes 8-23)
	for i := 8; i < 24; i++ {
		data[i] = 0
	}

	// Zero out destination IP (bytes 24-39)
	for i := 24; i < 40; i++ {
		data[i] = 0
	}

	return data
}

// worker processes packets from the jobs channel and sends results to the results channel.
// This is the core packet processing logic that runs in parallel.
func worker(jobs <-chan PacketJob, results chan<- PacketResult, wg *sync.WaitGroup, maskIP bool) {
	defer wg.Done()
	for job := range jobs {

		ethLayer := job.Packet.Layer(layers.LayerTypeEthernet)

		if ethLayer != nil {
			eth, _ := ethLayer.(*layers.Ethernet)

			// Extract payload (strips Ethernet header)
			payload := eth.LayerPayload()

			// 'payload' might point to a memory buffer that gets reused.
			// It is safer to make a copy for the final list.
			dataCopy := make([]uint8, len(payload))
			copy(dataCopy, payload)

			// Apply IP masking if requested
			if maskIP && len(dataCopy) > 0 {
				dataCopy = maskIPAddresses(dataCopy)
			}

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
func processFile(fileJob FileJob, outputLength int, sortPackets bool, workersPerFile int, maskIP bool) ([]PacketResult, error) {
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
		go worker(jobs, results, &wg, maskIP)
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

	// Standardize packet lengths consistently
	// If outputLength > 0: truncate/pad to that length
	// If outputLength == 0: keep original size
	for i := range finalPackets {
		finalPackets[i].OriginalSize = len(finalPackets[i].Data)
		finalPackets[i].Data = standardizePacketLength(finalPackets[i].Data, outputLength)
	}

	return finalPackets, nil
}

// processFileStreaming processes a single PCAP/PCAPNG file and streams packets directly to a writer.
// This is the memory-efficient version that doesn't accumulate packets in memory.
func processFileStreaming(fileJob FileJob, writer StreamWriter, outputLength int, workersPerFile int, maskIP bool) (int, error) {
	// Open PCAP file
	handle, err := pcap.OpenOffline(fileJob.FilePath)
	if err != nil {
		return 0, fmt.Errorf("cannot open file %s: %w", fileJob.FilePath, err)
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
		go worker(jobs, results, &wg, maskIP)
	}

	// Start writer goroutine that streams packets directly to disk
	packetCount := 0
	var writeErr error
	done := make(chan bool)
	go func() {
		for res := range results {
			res.OriginalSize = len(res.Data)
			// Standardize packet length consistently
			res.Data = standardizePacketLength(res.Data, outputLength)
			if err := writer.WritePacket(res); err != nil {
				writeErr = err
				break
			}
			packetCount++
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

	if writeErr != nil {
		return packetCount, fmt.Errorf("error writing packets: %w", writeErr)
	}

	return packetCount, nil
}

// processFilesParallel processes multiple files with limited parallelism.
// Each file is processed with its own set of packet workers.
func processFilesParallel(fileJobs []FileJob, outputLength int, sortPackets bool, maxConcurrentFiles int, maskIP bool) []PacketResult {
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

				packets, err := processFile(fileJob, outputLength, sortPackets, workersPerFile, maskIP)
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

// processFilesStreamingSingleOutput processes multiple files and streams all packets to a single output file.
// This is memory-efficient as packets are written immediately without accumulation.
func processFilesStreamingSingleOutput(fileJobs []FileJob, writer StreamWriter, outputLength int, maxConcurrentFiles int, maskIP bool) (int, error) {
	// Calculate workers per file
	totalCores := runtime.NumCPU()
	workersPerFile := totalCores / maxConcurrentFiles
	if workersPerFile < 1 {
		workersPerFile = 1
	}

	// Create channel for file jobs
	fileChannel := make(chan FileJob, len(fileJobs))
	for _, job := range fileJobs {
		fileChannel <- job
	}
	close(fileChannel)

	totalPackets := 0
	var packetMutex sync.Mutex
	var processErr error

	// Process files sequentially to maintain order and avoid writer contention
	fileNum := 0
	for fileJob := range fileChannel {
		fileNum++
		fmt.Printf("[%d/%d] Processing %s (class: %s)\n", fileNum, len(fileJobs), filepath.Base(fileJob.FilePath), fileJob.Class)

		count, err := processFileStreaming(fileJob, writer, outputLength, workersPerFile, maskIP)
		if err != nil {
			log.Printf("Error processing %s: %v\n", fileJob.FilePath, err)
			processErr = err
			break
		}

		packetMutex.Lock()
		totalPackets += count
		packetMutex.Unlock()

		// Print memory stats
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		fmt.Printf("[%d/%d] Processed %s: %d packets\n", fileNum, len(fileJobs), filepath.Base(fileJob.FilePath), count)
		fmt.Printf("        Memory: Alloc=%dMB, Sys=%dMB, TotalPackets=%d\n",
			m.Alloc/1024/1024, m.Sys/1024/1024, totalPackets)
	}

	if processErr != nil {
		return totalPackets, processErr
	}

	return totalPackets, nil
}

// processFilesStreamingPerFile processes multiple files and creates a separate output file for each input file.
// This is the most memory-efficient approach and allows parallel processing.
func processFilesStreamingPerFile(fileJobs []FileJob, outputDir string, outputFormat string, outputLength int, maxConcurrentFiles int, maskIP bool) error {
	// Calculate workers per file
	totalCores := runtime.NumCPU()
	workersPerFile := totalCores / maxConcurrentFiles
	if workersPerFile < 1 {
		workersPerFile = 1
	}

	fmt.Printf("Processing %d files with per-file output (maximum memory efficiency)\n", len(fileJobs))
	fmt.Printf("Output directory: %s\n", outputDir)
	fmt.Printf("Max concurrent files: %d, Workers per file: %d\n\n", maxConcurrentFiles, workersPerFile)

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// For streaming writers, we need to know the expected packet size for buffer allocation
	// If outputLength > 0: use that, otherwise use a reasonable default for buffer sizing
	bufferSize := outputLength
	if bufferSize == 0 {
		bufferSize = 1500 // Default for buffer allocation only
	}

	// Create channel for file jobs
	fileChannel := make(chan FileJob, len(fileJobs))
	for _, job := range fileJobs {
		fileChannel <- job
	}
	close(fileChannel)

	// Process files in parallel
	var wg sync.WaitGroup
	var errMutex sync.Mutex
	var firstError error

	for i := 0; i < maxConcurrentFiles; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			fileNum := 0
			for fileJob := range fileChannel {
				fileNum++

				// Generate output filename
				baseName := filepath.Base(fileJob.FilePath)
				ext := filepath.Ext(baseName)
				nameWithoutExt := baseName[:len(baseName)-len(ext)]

				var outputFile string
				if outputFormat == "parquet" {
					outputFile = filepath.Join(outputDir, nameWithoutExt+".parquet")
				} else {
					outputFile = filepath.Join(outputDir, nameWithoutExt+".csv")
				}

				fmt.Printf("[Worker %d] Processing %s -> %s\n", workerID, baseName, filepath.Base(outputFile))

				// Create writer for this file
				var writer StreamWriter
				var err error
				hasClass := fileJob.Class != ""

				if outputFormat == "parquet" {
					writer, err = NewParquetStreamWriter(outputFile, bufferSize, hasClass)
				} else {
					writer, err = NewCSVStreamWriter(outputFile, bufferSize, hasClass)
				}

				if err != nil {
					log.Printf("[Worker %d] Failed to create writer for %s: %v\n", workerID, outputFile, err)
					errMutex.Lock()
					if firstError == nil {
						firstError = err
					}
					errMutex.Unlock()
					continue
				}

				// Process file
				count, err := processFileStreaming(fileJob, writer, outputLength, workersPerFile, maskIP)
				writer.Close()

				if err != nil {
					log.Printf("[Worker %d] Error processing %s: %v\n", workerID, fileJob.FilePath, err)
					errMutex.Lock()
					if firstError == nil {
						firstError = err
					}
					errMutex.Unlock()
					continue
				}

				fmt.Printf("[Worker %d] Completed %s: %d packets -> %s\n", workerID, baseName, count, filepath.Base(outputFile))
			}
		}(i)
	}

	wg.Wait()
	return firstError
}
