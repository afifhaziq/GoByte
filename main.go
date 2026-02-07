// GoByte - PCAP parser for preprocessing network traffic data for deep learning models

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

const banner = `
  ________      __________          __          
 /  _____/  ____\______   \___.__._/  |_  ____  
/   \  ___ /  _ \|    |  _<   |  |\   __\/ __ \ 
\    \_\  (  <_> )    |   \\___  | |  | \  ___/ 
 \______  /\____/|______  // ____| |__|  \___  >
        \/              \/ \/                \/ 

Fast PCAP Parser for Deep Learning | Network Traffic Preprocessing
`

func main() {
	// --- CLI FLAGS ---
	inputFile := flag.String("input", "", "Input PCAP file path (single file mode)")
	datasetDir := flag.String("dataset", "", "Dataset directory with class subdirectories (multi-file mode)")
	outputFormat := flag.String("format", "csv", "Output format: csv or parquet")
	outputFile := flag.String("output", "", "Output file path (default: output.csv or output.parquet)")
	outputLength := flag.Int("length", 0, "Desired length of output bytes (pad/truncate). 0 = keep original size (default: 0)")
	sortPackets := flag.Bool("sort", true, "Retain packets order. set to false to shuffle")
	maxConcurrentFiles := flag.Int("concurrent", 2, "Max concurrent files to process (multi-file mode)")
	streamingMode := flag.Bool("streaming", false, "Use streaming mode for memory efficiency (default: false)")
	perFileOutput := flag.Bool("per-file", false, "Create separate output file for each input file (dataset mode only, enables streaming)")
	ipMask := flag.Bool("ipmask", false, "Mask source and destination IP addresses")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n", banner)
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  Single file mode:\n")
		fmt.Fprintf(os.Stderr, "    %s --input data.pcap --format parquet\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "    %s --input data.pcap --output results.csv --length 512\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\n  Multi-file mode (with class labels):\n")
		fmt.Fprintf(os.Stderr, "    %s --dataset ./dataset --format parquet --concurrent 2\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "    %s --dataset ./dataset --per-file --streaming\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "    Dataset structure: dataset/class_a/*.pcap, dataset/class_b/*.pcap\n")
		fmt.Fprintf(os.Stderr, "\nFormats:\n")
		fmt.Fprintf(os.Stderr, "  csv     - Standard CSV format (large files)\n")
		fmt.Fprintf(os.Stderr, "  parquet - Compressed columnar format (recommended for ML/DL)\n")
		fmt.Fprintf(os.Stderr, "\nMemory Optimization (for large datasets):\n")
		fmt.Fprintf(os.Stderr, "  --streaming      - Stream packets to disk (low memory, ~200-300MB RAM)\n")
		fmt.Fprintf(os.Stderr, "  --per-file       - Create one output per input file (lowest memory, parallel)\n")
		fmt.Fprintf(os.Stderr, "\nNote: Default mode loads all packets in memory (fast, high memory usage)\n")
		fmt.Fprintf(os.Stderr, "      Streaming mode: less memory, processes files sequentially\n")
	}

	flag.Parse()

	fmt.Print(banner)

	// Create output directory if it doesn't exist
	outputDir := "output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Set default output file based on format
	if *outputFile == "" {
		if *outputFormat == "parquet" {
			*outputFile = filepath.Join(outputDir, "output.parquet")
		} else {
			*outputFile = filepath.Join(outputDir, "output.csv")
		}
	} else {
		// If user specified output file, place it in output directory
		*outputFile = filepath.Join(outputDir, filepath.Base(*outputFile))
	}

	// Validate input mode
	if *inputFile == "" && *datasetDir == "" {
		log.Fatal("Error: Must specify either --input (single file) or --dataset (multi-file)")
	}
	if *inputFile != "" && *datasetDir != "" {
		log.Fatal("Error: Cannot use both --input and --dataset. Choose one mode.")
	}

	t0 := time.Now()

	// Mode selection
	if *datasetDir != "" {
		// Multi-file mode with class labels
		if *perFileOutput {
			// Per-file output mode (most memory efficient, enables streaming automatically)
			processDatasetPerFile(*datasetDir, *outputFormat, *outputLength, *maxConcurrentFiles, *ipMask)
		} else if *streamingMode {
			// Streaming mode (memory efficient, single output)
			processDatasetStreaming(*datasetDir, *outputFile, *outputFormat, *outputLength, *maxConcurrentFiles, *ipMask)
		} else {
			// Default mode (loads all in memory - fast, high memory usage)
			finalPackets := processDataset(*datasetDir, *outputLength, *sortPackets, *maxConcurrentFiles, *ipMask)
			tProcess := time.Since(t0)
			fmt.Printf("\nProcessed %d packets in %v\n", len(finalPackets), tProcess)

			tWrite := time.Now()
			if *outputFormat == "parquet" {
				if err := writeParquet(*outputFile, finalPackets, *outputLength); err != nil {
					log.Fatalf("failed to write parquet: %v", err)
				}
			} else {
				if err := writeCSVOptimized(*outputFile, finalPackets, *outputLength); err != nil {
					log.Fatalf("failed to write csv: %v", err)
				}
			}
			tWriteDuration := time.Since(tWrite)
			printSummary(len(finalPackets), *outputFile, *outputLength, tProcess, tWriteDuration, time.Since(t0))
		}
	} else {
		// Single file mode
		if *streamingMode {
			processSingleFileStreaming(*inputFile, *outputFile, *outputFormat, *outputLength, *ipMask)
		} else {
			// Default mode (loads all in memory)
			finalPackets := processSingleFile(*inputFile, *outputLength, *sortPackets, *ipMask)
			tProcess := time.Since(t0)
			fmt.Printf("\nProcessed %d packets in %v\n", len(finalPackets), tProcess)

			tWrite := time.Now()
			if *outputFormat == "parquet" {
				if err := writeParquet(*outputFile, finalPackets, *outputLength); err != nil {
					log.Fatalf("failed to write parquet: %v", err)
				}
			} else {
				if err := writeCSVOptimized(*outputFile, finalPackets, *outputLength); err != nil {
					log.Fatalf("failed to write csv: %v", err)
				}
			}
			tWriteDuration := time.Since(tWrite)
			printSummary(len(finalPackets), *outputFile, *outputLength, tProcess, tWriteDuration, time.Since(t0))
		}
	}
}

// processSingleFile processes a single PCAP file (backward compatible mode)
func processSingleFile(filePath string, outputLength int, sortPackets bool, maskIP bool) []PacketResult {
	fmt.Printf("Mode: Single file\n")
	fmt.Printf("Processing: %s\n\n", filePath)

	fileJob := FileJob{
		FilePath: filePath,
		Class:    "",
	}

	packets, err := processFile(fileJob, outputLength, sortPackets, runtime.NumCPU(), maskIP)
	if err != nil {
		log.Fatalf("Failed to process file: %v", err)
	}

	return packets
}

// discoverDatasetFiles scans the dataset directory and returns all PCAP/PCAPNG files with their classes
func discoverDatasetFiles(datasetDir string) ([]FileJob, error) {
	entries, err := os.ReadDir(datasetDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read dataset directory: %w", err)
	}

	var fileJobs []FileJob

	// Scan each class directory
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		className := entry.Name()
		classPath := filepath.Join(datasetDir, className)

		// Find all PCAP/PCAPNG files in this class
		pcapFiles, err := filepath.Glob(filepath.Join(classPath, "*.pcap"))
		if err != nil {
			log.Printf("Warning: Error scanning %s: %v", classPath, err)
			continue
		}

		pcapngFiles, err := filepath.Glob(filepath.Join(classPath, "*.pcapng"))
		if err != nil {
			log.Printf("Warning: Error scanning %s: %v", classPath, err)
			continue
		}

		allFiles := append(pcapFiles, pcapngFiles...)
		fmt.Printf("Found class '%s': %d files\n", className, len(allFiles))

		for _, file := range allFiles {
			fileJobs = append(fileJobs, FileJob{
				FilePath: file,
				Class:    className,
			})
		}
	}

	if len(fileJobs) == 0 {
		return nil, fmt.Errorf("no PCAP/PCAPNG files found in dataset directory")
	}

	return fileJobs, nil
}

// processDataset processes multiple PCAP files organized by class directories (legacy mode)
func processDataset(datasetDir string, outputLength int, sortPackets bool, maxConcurrentFiles int, maskIP bool) []PacketResult {
	fmt.Printf("Mode: Multi-file dataset\n")
	fmt.Printf("Dataset directory: %s\n", datasetDir)
	fmt.Printf("Max concurrent files: %d\n\n", maxConcurrentFiles)

	fileJobs, err := discoverDatasetFiles(datasetDir)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\nTotal files to process: %d\n", len(fileJobs))

	// Process files with hybrid parallelism
	return processFilesParallel(fileJobs, outputLength, sortPackets, maxConcurrentFiles, maskIP)
}

// processDatasetStreaming processes dataset with streaming output (memory efficient, single file)
func processDatasetStreaming(datasetDir, outputFile, outputFormat string, outputLength, maxConcurrentFiles int, maskIP bool) {
	fmt.Printf("Mode: Multi-file dataset (streaming)\n")
	fmt.Printf("Dataset directory: %s\n", datasetDir)
	fmt.Printf("Output format: %s\n\n", outputFormat)

	t0 := time.Now()

	fileJobs, err := discoverDatasetFiles(datasetDir)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\nTotal files to process: %d\n\n", len(fileJobs))

	// Determine max packet size
	maxPacketSize := outputLength
	if maxPacketSize == 0 {
		maxPacketSize = 1500 // Default MTU
	}

	// Create streaming writer
	hasClass := len(fileJobs) > 0 && fileJobs[0].Class != ""
	var writer StreamWriter

	fmt.Printf("Processing %d files with streaming output (memory-efficient mode)\n", len(fileJobs))
	fmt.Printf("Output: %s\n", outputFile)
	fmt.Printf("Workers per file: %d\n\n", runtime.NumCPU())

	if outputFormat == "parquet" {
		writer, err = NewParquetStreamWriter(outputFile, maxPacketSize, hasClass)
	} else {
		writer, err = NewCSVStreamWriter(outputFile, maxPacketSize, hasClass)
	}

	if err != nil {
		log.Fatalf("Failed to create writer: %v", err)
	}

	// Process all files streaming to single output
	totalPackets, err := processFilesStreamingSingleOutput(fileJobs, writer, outputLength, maxConcurrentFiles, maskIP)
	writer.Close()

	if err != nil {
		log.Fatalf("Error during processing: %v", err)
	}

	tTotal := time.Since(t0)

	// Print summary
	fmt.Printf("\nStreaming mode completed:\n")
	fmt.Printf(" - Total packets: %d\n", totalPackets)
	fmt.Printf(" - Total time:    %v\n", tTotal)
	if info, err := os.Stat(outputFile); err == nil {
		sizeMB := float64(info.Size()) / (1024 * 1024)
		fmt.Printf(" - File size:     %.2f MB\n", sizeMB)
	}
	fmt.Printf(" - Output:        %s\n", outputFile)
}

// processDatasetPerFile processes dataset with per-file output (maximum memory efficiency)
func processDatasetPerFile(datasetDir, outputFormat string, outputLength, maxConcurrentFiles int, maskIP bool) {
	fmt.Printf("Mode: Multi-file dataset (per-file output)\n")
	fmt.Printf("Dataset directory: %s\n", datasetDir)
	fmt.Printf("Output format: %s\n\n", outputFormat)

	t0 := time.Now()

	fileJobs, err := discoverDatasetFiles(datasetDir)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\nTotal files to process: %d\n\n", len(fileJobs))

	// Create output directory
	outputDir := filepath.Join("output", "per_file_"+time.Now().Format("20060102_150405"))

	// Process files with per-file output
	err = processFilesStreamingPerFile(fileJobs, outputDir, outputFormat, outputLength, maxConcurrentFiles, maskIP)
	if err != nil {
		log.Fatalf("Error during processing: %v", err)
	}

	tTotal := time.Since(t0)

	// Print summary
	fmt.Printf("\nPer-file mode completed:\n")
	fmt.Printf(" - Total files:   %d\n", len(fileJobs))
	fmt.Printf(" - Total time:    %v\n", tTotal)
	fmt.Printf(" - Output dir:    %s\n", outputDir)
}

// processSingleFileStreaming processes a single file with streaming output
func processSingleFileStreaming(inputFile, outputFile, outputFormat string, outputLength int, maskIP bool) {
	fmt.Printf("Mode: Single file (streaming)\n")
	fmt.Printf("Processing: %s\n", inputFile)
	fmt.Printf("Output: %s\n\n", outputFile)

	t0 := time.Now()

	// Determine max packet size
	maxPacketSize := outputLength
	if maxPacketSize == 0 {
		maxPacketSize = 1500
	}

	// Create writer
	var writer StreamWriter
	var err error

	if outputFormat == "parquet" {
		writer, err = NewParquetStreamWriter(outputFile, maxPacketSize, false)
	} else {
		writer, err = NewCSVStreamWriter(outputFile, maxPacketSize, false)
	}

	if err != nil {
		log.Fatalf("Failed to create writer: %v", err)
	}

	// Process file
	fileJob := FileJob{
		FilePath: inputFile,
		Class:    "",
	}

	totalPackets, err := processFileStreaming(fileJob, writer, outputLength, runtime.NumCPU(), maskIP)
	writer.Close()

	if err != nil {
		log.Fatalf("Error processing file: %v", err)
	}

	tTotal := time.Since(t0)

	// Print summary
	fmt.Printf("\nStreaming mode completed:\n")
	fmt.Printf(" - Total packets: %d\n", totalPackets)
	fmt.Printf(" - Total time:    %v\n", tTotal)
	if info, err := os.Stat(outputFile); err == nil {
		sizeMB := float64(info.Size()) / (1024 * 1024)
		fmt.Printf(" - File size:     %.2f MB\n", sizeMB)
	}
	fmt.Printf(" - Output:        %s\n", outputFile)
}

// printSummary displays a formatted summary of the processing results
func printSummary(numPackets int, outputFile string, outputLength int, processTime, writeTime, totalTime time.Duration) {
	fmt.Println()

	if outputLength == 0 {
		fmt.Printf("Exported %d packets to %s (Variable length - original sizes kept)\n", numPackets, outputFile)
	} else {
		fmt.Printf("Exported %d packets to %s (Length: %d bytes)\n", numPackets, outputFile, outputLength)
	}

	fmt.Printf(" - Processing time: %v\n", processTime)
	fmt.Printf(" - Export time:     %v\n", writeTime)
	fmt.Printf(" - Total time:      %v\n", totalTime)

	// Show file size if available
	if info, err := os.Stat(outputFile); err == nil {
		sizeMB := float64(info.Size()) / (1024 * 1024)
		fmt.Printf(" - File size:       %.2f MB\n", sizeMB)
	}
}
