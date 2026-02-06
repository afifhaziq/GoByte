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
	sortPackets := flag.Bool("sort", true, "Retain packets order. set to false to shuffle (default: true)")
	maxConcurrentFiles := flag.Int("concurrent", 2, "Max concurrent files to process (multi-file mode)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n", banner)
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  Single file mode:\n")
		fmt.Fprintf(os.Stderr, "    %s --input data.pcap --format parquet\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "    %s --input data.pcap --output results.csv --length 2048\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\n  Multi-file mode (with class labels):\n")
		fmt.Fprintf(os.Stderr, "    %s --dataset ./dataset --format parquet --concurrent 2\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "    Dataset structure: dataset/class_a/*.pcap, dataset/class_b/*.pcap\n")
		fmt.Fprintf(os.Stderr, "\nFormats:\n")
		fmt.Fprintf(os.Stderr, "  csv     - Standard CSV format (large files)\n")
		fmt.Fprintf(os.Stderr, "  parquet - Compressed columnar format (recommended for ML)\n")
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
	var finalPackets []PacketResult

	// Mode selection
	if *datasetDir != "" {
		// Multi-file mode with class labels
		finalPackets = processDataset(*datasetDir, *outputLength, *sortPackets, *maxConcurrentFiles)
	} else {
		// Single file mode (backward compatible)
		finalPackets = processSingleFile(*inputFile, *outputLength, *sortPackets)
	}

	tProcess := time.Since(t0)
	fmt.Printf("\nProcessed %d packets in %v\n", len(finalPackets), tProcess)

	// Export to chosen format
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

	// Print summary
	printSummary(len(finalPackets), *outputFile, *outputLength, tProcess, tWriteDuration, time.Since(t0))
}

// processSingleFile processes a single PCAP file (backward compatible mode)
func processSingleFile(filePath string, outputLength int, sortPackets bool) []PacketResult {
	fmt.Printf("Mode: Single file\n")
	fmt.Printf("Processing: %s\n\n", filePath)

	fileJob := FileJob{
		FilePath: filePath,
		Class:    "",
	}

	packets, err := processFile(fileJob, outputLength, sortPackets, runtime.NumCPU())
	if err != nil {
		log.Fatalf("Failed to process file: %v", err)
	}

	return packets
}

// processDataset processes multiple PCAP files organized by class directories
func processDataset(datasetDir string, outputLength int, sortPackets bool, maxConcurrentFiles int) []PacketResult {
	fmt.Printf("Mode: Multi-file dataset\n")
	fmt.Printf("Dataset directory: %s\n", datasetDir)
	fmt.Printf("Max concurrent files: %d\n\n", maxConcurrentFiles)

	// Discover all class directories
	entries, err := os.ReadDir(datasetDir)
	if err != nil {
		log.Fatalf("Failed to read dataset directory: %v", err)
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
		log.Fatal("No PCAP/PCAPNG files found in dataset directory")
	}

	fmt.Printf("\nTotal files to process: %d\n", len(fileJobs))

	// Process files with hybrid parallelism
	return processFilesParallel(fileJobs, outputLength, sortPackets, maxConcurrentFiles)
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
