// GoByte - PCAP parser for preprocessing network traffic data for deep learning models

// TODO: Iterate through list of files
// TODO: Insert class labels at the end (FB, Youtube) from config file
// TODO: Terminal support / progress indicator

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/pcap"
)

const banner = `
⡿⣡⣿⣿⡟⡼⡁⠁⣰⠂⡾⠉⢨⣿⠃⣿⡿⠍⣾⣟⢤⣿⢇⣿⢇⣿⣿⢿
⣱⣿⣿⡟⡐⣰⣧⡷⣿⣴⣧⣤⣼⣯⢸⡿⠁⣰⠟⢀⣼⠏⣲⠏⢸⣿⡟⣿
⣿⣿⡟⠁⠄⠟⣁⠄⢡⣿⣿⣿⣿⣿⣿⣦⣼⢟⢀⡼⠃⡹⠃⡀⢸⡿⢸⣿
⣿⣿⠃⠄⢀⣾⠋⠓⢰⣿⣿⣿⣿⣿⣿⠿⣿⣿⣾⣅⢔⣕⡇⡇⡼⢁⣿⣿
⣿⡟⠄⠄⣾⣇⠷⣢⣿⣿⣿⣿⣿⣿⣿⣭⣀⡈⠙⢿⣿⣿⡇⡧⢁⣾⣿⣿
⣿⡇⠄⣼⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠟⢻⠇⠄⠄⢿⣿⡇⢡⣾⣿⣿⣿
⣿⣷⢰⣿⣿⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⢰⣧⣀⡄⢀⠘⡿⣰⣿⣿⣿⣿⣿
⢹⣿⢸⣿⣿⠟⠻⢿⣿⣿⣿⣿⣿⣿⣿⣶⣭⣉⣤⣿⢈⣼⣿⣿⣿⣿⣿⣿
⢸⠇⡜⣿⡟⠄⠄⠄⠈⠙⣿⣿⣿⣿⣿⣿⣿⣿⠟⣱⣻⣿⣿⣿⣿⣿⠟⠁
⠄⣰⡗⠹⣿⣄⠄⠄⠄⢀⣿⣿⣿⣿⣿⣿⠟⣅⣥⣿⣿⣿⣿⠿⠋⠄⠄⣾
⠜⠋⢠⣷⢻⣿⣿⣶⣾⣿⣿⣿⣿⠿⣛⣥⣾⣿⠿⠟⠛⠉⠄⠄

Fast PCAP Parser for Deep Learning | Network Traffic Preprocessing
`

// const banner = `
//   ________      __________          __
//  /  _____/  ____\______   \___.__._/  |_  ____
// /   \  ___ /  _ \|    |  _<   |  |\   __\/ __ \
// \    \_\  (  <_> )    |   \\___  | |  | \  ___/
//  \______  /\____/|______  // ____| |__|  \___  >
//         \/              \/ \/                \/

// Fast PCAP Parser for Deep Learning | Network Traffic Preprocessing
// `

func main() {
	// --- CLI FLAGS ---
	inputFile := flag.String("input", "dataset/test.pcap", "Input PCAP file path")
	outputFormat := flag.String("format", "csv", "Output format: csv or parquet")
	outputFile := flag.String("output", "", "Output file path (default: output.csv or output.parquet)")
	outputLength := flag.Int("length", 1480, "Desired length of output bytes (pad/truncate)")
	numWorkers := flag.Int("numworkers", runtime.NumCPU(), "Numbers of CPU cores to use. (default: Use all cores available in the host machine)")
	sortPackets := flag.Bool("sort", true, "Retain packets order. set to false to shuffle (default: true)")

	// Custom usage function to show banner
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n", banner)
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s --input data.pcap --format parquet\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s --input data.pcap --output results.csv --length 64\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nFormats:\n")
		fmt.Fprintf(os.Stderr, "  csv     - Standard CSV format (large files)\n")
		fmt.Fprintf(os.Stderr, "  parquet - Compressed columnar format (recommended for ML/DL)\n")
	}

	flag.Parse()

	// Print banner on startup
	fmt.Print(banner)

	// Set default output file based on format
	if *outputFile == "" {
		if *outputFormat == "parquet" {
			*outputFile = "output.parquet"
		} else {
			*outputFile = "output.csv"
		}
	}

	// 1. Open File
	handle, err := pcap.OpenOffline(*inputFile)
	if err != nil {
		log.Fatalf("cannot open pcap file %s: %v", *inputFile, err)
	}
	defer handle.Close()

	fmt.Printf("Processing PCAP file: %s\n", *inputFile)
	fmt.Printf("Output format: %s\n", *outputFormat)
	fmt.Printf("Output file: %s\n", *outputFile)
	fmt.Printf("Packet length: %d bytes\n", *outputLength)
	fmt.Printf("Workers: %d\n\n", runtime.NumCPU())

	t0 := time.Now()

	var size uint16 = 2048
	// 2. Setup Channels
	// Jobs channel: Buffering helps keep the reader from blocking
	jobs := make(chan PacketJob, size)
	// Results channel
	results := make(chan PacketResult, size)

	//fmt.Printf("channel size: %d\n\n", size)

	// 3. Start Workers
	// Usually set to the number of CPU cores
	//numWorkers := runtime.NumCPU()
	var wg sync.WaitGroup

	for w := 0; w < *numWorkers; w++ {
		wg.Add(1)
		go worker(jobs, results, &wg)
	}

	// 4. Start a "Collector" in a separate goroutine
	// Pre-allocate slice with estimated capacity to reduce allocations
	// This ensures we don't block writing results while reading file
	finalPackets := make([]PacketResult, 0, 100000)
	done := make(chan bool)
	go func() {
		for res := range results {
			finalPackets = append(finalPackets, res)
		}
		done <- true
	}()

	// 5. Read File and Feed Workers
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())

	// We disable lazy decoding here so the main thread does the read,
	// but the heavy layer parsing happens in the worker.
	packetSource.DecodeOptions = gopacket.DecodeOptions{Lazy: true, NoCopy: true}

	counter := 0
	for packet := range packetSource.Packets() {
		jobs <- PacketJob{
			Index:  counter,
			Packet: packet,
		}
		counter++
	}

	// 6. Shutdown
	close(jobs)    // Signal workers no more jobs coming
	wg.Wait()      // Wait for workers to finish processing
	close(results) // Signal collector no more results coming
	<-done         // Wait for collector to finish

	// 7. (Optional) Restore Order
	// Parallel processing scrambles order. If you need
	// the list 1, 2, 3... sort it now.

	if *sortPackets {
		sort.Slice(finalPackets, func(i, j int) bool {
			return finalPackets[i].Index < finalPackets[j].Index
		})
	}

	tProcess := time.Since(t0)
	fmt.Printf("Processed %d packets in %v\n", len(finalPackets), tProcess)

	// 8. Truncate/pad all packets to desired length
	for i := range finalPackets {
		finalPackets[i].OriginalSize = len(finalPackets[i].Data)
		finalPackets[i].Data = truncatePad(finalPackets[i].Data, *outputLength)
	}

	// 9. Export to chosen format
	tWrite := time.Now()
	if *outputFormat == "parquet" {
		if err := writeParquet(*outputFile, finalPackets); err != nil {
			log.Fatalf("failed to write parquet: %v", err)
		}
	} else {
		if err := writeCSVOptimized(*outputFile, finalPackets, *outputLength); err != nil {
			log.Fatalf("failed to write csv: %v", err)
		}
	}
	tWriteDuration := time.Since(tWrite)

	// Print summary
	printSummary(len(finalPackets), *outputFile, *outputLength, tProcess, tWriteDuration, time.Since(t0))
}

// printSummary displays a formatted summary of the processing results
func printSummary(numPackets int, outputFile string, outputLength int, processTime, writeTime, totalTime time.Duration) {
	fmt.Printf("\nExported %d packets to %s (Length: %d bytes)\n", numPackets, outputFile, outputLength)
	fmt.Printf(" - Processing time: %v\n", processTime)
	fmt.Printf(" - Export time:     %v\n", writeTime)
	fmt.Printf(" - Total time:      %v\n", totalTime)

	// Show file size if available
	if info, err := os.Stat(outputFile); err == nil {
		sizeMB := float64(info.Size()) / (1024 * 1024)
		fmt.Printf(" - File size:       %.2f MB\n", sizeMB)
	}
}
