# GoByte

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Release](https://github.com/afifhaziq/GoByte/actions/workflows/release.yml/badge.svg)](https://github.com/afifhaziq/GoByte/actions/workflows/release.yml)
[![Security Scan](https://img.shields.io/github/actions/workflow/status/afifhaziq/GoByte/security.yml?branch=main&label=security)](https://github.com/afifhaziq/GoByte/actions/workflows/security.yml)
[![GitHub Release](https://img.shields.io/github/v/release/afifhaziq/GoByte?style=flat)](https://github.com/afifhaziq/GoByte/releases/latest)


**GoByte** is a fast PCAP/PCAPNG parser built in Go for preprocessing network traffic data for deep learning models. It extracts packet bytes and exports them in multiple formats (CSV, Parquet, or NumPy).

```
  ________      __________          __          
 /  _____/  ____\______   \___.__._/  |_  ____  
/   \  ___ /  _ \|    |  _<   |  |\   __\/ __ \ 
\    \_\  (  <_> )    |   \\___  | |  | \  ___/ 
 \______  /\____/|______  // ____| |__|  \___  >
        \/              \/ \/                \/ 

Fast PCAP Parser for Deep Learning | Network Traffic Preprocessing
```

## Features

- **High Performance**: Concurrent packet processing using Go's goroutines
- **Memory Efficient**: Streaming mode for processing large datasets with minimal RAM usage
- **Multiple Formats**: Export to CSV, Parquet, or NumPy (binary format optimized for ML/DL)
- **Class Labels**: Automatic labeling from directory structure (e.g., `dataset/malware/*.pcap`)
- **Batch Processing**: Process multiple PCAP files in parallel
- **Flexible Output**: Fixed-length padding/truncation or variable-length packets
- **Privacy Protection**: Optional IP address masking for anonymization
- **Deep Learning Ready**: Direct output for PyTorch, TensorFlow, scikit-learn
- **PCAP/PCAPNG Support**: Handles both formats automatically

## Use Cases

- **Network Traffic Classification**: Application identification, IoT device classification
- **Intrusion Detection Systems**: Anomaly detection, signature-based detection
- **Network Behavior Analysis**: Protocol analysis, traffic profiling
- **Deep Learning Preprocessing**: CNN, RNN, Transformer models for packet analysis

---

## Installation

### Option 1: Download Pre-built Binaries (Recommended)

Download the latest release for your platform:

- **Linux (AMD64)**: [gobyte-linux-amd64](https://github.com/afifhaziq/GoByte/releases/latest)
- **Linux (ARM64)**: [gobyte-linux-arm64](https://github.com/afifhaziq/GoByte/releases/latest)
- **macOS (Intel)**: [gobyte-darwin-amd64](https://github.com/afifhaziq/GoByte/releases/latest)
- **macOS (Apple Silicon)**: [gobyte-darwin-arm64](https://github.com/afifhaziq/GoByte/releases/latest)
- **Windows (AMD64)**: [gobyte-windows-amd64.exe](https://github.com/afifhaziq/GoByte/releases/latest)

#### Linux/macOS Setup:
```bash
# Download the binary (replace with your machine platform)
wget https://github.com/afifhaziq/GoByte/releases/latest/download/gobyte-linux-amd64

# Make it executable
chmod +x gobyte-linux-amd64

# Move to PATH (optional)
sudo mv gobyte-linux-amd64 /usr/local/bin/gobyte

# Verify installation
gobyte --help
```

#### Windows Setup:
1. Download `gobyte-windows-amd64.exe` from [Releases](https://github.com/afifhaziq/GoByte/releases)
2. Rename to `gobyte.exe`
3. Add to your PATH or run from the download directory

---

### Option 2: Build from Source

**Requirements:**
- Go 1.23 or higher
- libpcap development headers

#### Install Dependencies (Linux):
```bash
# Ubuntu/Debian
sudo apt-get install libpcap-dev

# Fedora/CentOS/RHEL
sudo yum install libpcap-devel

# Arch Linux
sudo pacman -S libpcap
```

#### Install Dependencies (macOS):
```bash
# libpcap is pre-installed on macOS
# If needed, install via Homebrew:
brew install libpcap
```

#### Build:
```bash
# Clone the repository
git clone https://github.com/afifhaziq/GoByte.git
cd GoByte

# Download Go dependencies
go mod download

# Build the binary
go build -o gobyte .

# Run
./gobyte --help
```

---

## Quick Start

### Don't Have PCAP Files? Download Sample Dataset

If you're new to network traffic analysis and don't have PCAP files to start with, download [MalayaNetwork_GT](https://huggingface.co/datasets/Afifhaziq/MalayaNetwork_GT), our ready-to-use dataset. 

The easiest way to install this dataset is to use the `uv` package manager (No virtual environment required). Click this [link](https://docs.astral.sh/uv/getting-started/installation/) to install it .

```bash
# Install uv (if not already installed)

# Download the MalayaNetwork_GT dataset (13.5 GB, 10 traffic classes)
uvx hf download Afifhaziq/MalayaNetwork_GT \
    --repo-type dataset \
    --local-dir ./dataset \
    --include "PCAP/*"

# Process the entire dataset with GoByte (streaming mode for large datasets)
gobyte --dataset dataset/PCAP --format numpy --length 1500 --streaming --output dataset.npy
# Or use parquet format:
# gobyte --dataset dataset/PCAP --format parquet --length 1500 --streaming
```

This will download network traffic from 10 application classes:
- **Bittorent**
- **ChromeRDP**
- **Discord**
- **EA Origin**
- **Microsoft Teams**
- **Slack**
- **Steam**
- **Teamviewer**
- **Webex**
- **Zoom**

[![Dataset on HF](https://huggingface.co/datasets/huggingface/badges/resolve/main/dataset-on-hf-lg-dark.svg)](https://huggingface.co/datasets/Afifhaziq/MalayaNetwork_GT)

---

## Usage

```
Usage: gobyte [options]

Options:
  --input string
        Input PCAP file path (single file mode)
  --dataset string
        Dataset directory with class subdirectories (multi-file mode)
  --format string
        Output format: csv, parquet, or numpy (default "csv")
  --output string
        Output file path (default: output.csv, output.parquet, or output.npy based on format)
  --length int
        Desired length of output bytes (pad/truncate). 0 = keep original size (default: 0)
  --sort
        Retain packets order. Set to false to shuffle (default: true)
  --concurrent int
        Max concurrent files to process (multi-file mode) (default: 2)
  --streaming
        Use streaming mode for memory efficiency (default: true)
  --per-file
        Create separate output file for each input file (dataset mode only)
  --ipmask
        Mask source and destination IP addresses

Memory Optimization:
  --streaming      Stream packets to disk (default: true, ~200-300MB RAM)
  --streaming=false Load all packets in memory (WARNING: can cause OOM for large datasets)
  --per-file       Create one output per input file (lowest memory, parallel)

Note: Streaming mode is enabled by default to prevent OOM errors.
      Use --streaming=false for in-memory processing (only recommended for small files).
```

### Examples

#### Single File Processing

Process a single PCAP file and export to CSV:

```bash
gobyte --input traffic.pcap --output results.csv
```

Process and pad/truncate packets to fixed length:

```bash
gobyte --input traffic.pcap --length 1480 --format numpy
```

Mask IP addresses for privacy:

```bash
gobyte --input traffic.pcap --ipmask --format numpy
```

#### Multi-File Processing with Class Labels

Organize your dataset like this:

```
dataset/
├── benign/
│   ├── file1.pcap
│   └── file2.pcap
├── malware/
│   ├── file1.pcap
│   └── file2.pcap
└── ddos/
    ├── file1.pcap
    └── file2.pcap
```

Process all files and automatically assign class labels:

```bash
gobyte --dataset dataset --format numpy --length 720
```

For large datasets, use streaming mode:

```bash
gobyte --dataset dataset --format numpy --length 720 --streaming
```

Note: Labels are automatically extracted from directory names. You may need to encode them numerically before training except for **numpy** format.

#### Detailed Examples

**Example 1: Basic CSV Export**
```bash
gobyte --input data.pcap
# Output: output/output.csv
```

**Example 2: Fixed-Length Packets (Recommended for Deep Learning)**
```bash
gobyte --input data.pcap --length 1500 --format parquet
# All packets padded/truncated to exactly 1500 bytes
# Parquet works best with fixed-length packets
```

**Example 3: Multi-File Dataset with Labels**
```bash
gobyte --dataset my_dataset --format parquet --concurrent 4
# Processes multiple files in parallel and assigns labels from directory names
```

**Example 4: Large Dataset with Streaming Mode**
```bash
gobyte --dataset my_dataset --format parquet --length 1500 --streaming
# Memory-efficient processing for large datasets (uses ~200-300MB RAM)
```

**Example 5: Per-File Output Mode**
```bash
gobyte --dataset my_dataset --format parquet --per-file
# Creates separate output file for each input file (maximum memory efficiency)
```

**Example 6: Variable-Length Packets**
```bash
gobyte --input data.pcap --length 0 --format csv
# Keeps original packet sizes (no padding/truncation)
# Note: Use CSV for variable-length packets (Parquet is slow for variable-length)
```

**Example 7: IP Address Masking for Privacy**
```bash
gobyte --input data.pcap --ipmask --format parquet
# Masks source and destination IP addresses (sets them to 0.0.0.0)
```

**Example 8: NumPy Format (Recommended for ML/DL)**
```bash
gobyte --dataset my_dataset --format numpy --length 1500 --streaming --output dataset.npy
# Exports to NumPy format: 3-4x smaller than CSV, 10-20x faster loading
# Output: dataset_data.npy, dataset_labels.npy, dataset_classes.json
```

See [example/README.md](example/README.md) for detailed NumPy usage examples and DL framework integration.

**Example 9: Combined Options**
```bash
gobyte --dataset my_dataset --length 720 --ipmask --streaming --format parquet
# Fixed-length packets with IP masking in memory-efficient streaming mode
```

---

## Output Formats

### CSV Format
- Human-readable text format
- Large file sizes
- Compatible with all data analysis tools
- **Recommended for variable-length packets** (`--length 0`)
- Fast and memory-efficient for all packet sizes

### Parquet Format (Recommended for Fixed-Length)
- Compressed columnar format
- 10-20x smaller than CSV
- Optimized for ML frameworks (PyTorch, TensorFlow)
- **Best with `--length` flag** (e.g., `--length 1500`)
- **Variable-length Parquet is slow and memory-intensive** - use CSV instead for variable-length data

### NumPy Format (Recommended for ML/DL)
- Binary format (`.npy` files)
- **3-4x smaller** than CSV (18 GB vs 50-70 GB for same dataset)
- **10-20x faster** loading (2-5 seconds vs 30-60 seconds)
- **Native ML/DL integration** - zero-copy with PyTorch, TensorFlow, JAX
- Memory-efficient streaming mode (~200-300 MB RAM)
- Outputs: `*_data.npy` (packet data), `*_labels.npy` (class labels), `*_classes.json` (mapping)

For detailed NumPy usage, examples, and ML framework integration, see [example/README.md](example/README.md).

---

## Performance & Benchmarks

### Benchmark Dataset: MalayaNetwork_GT

**Dataset Specifications:**
- **Total Files:** 31 PCAP files
- **Total Size:** 13 GB
- **Total Packets:** 12,584,314 packets
- **Classes:** 10 application types (Bittorent, ChromeRDP, Discord, EA Origin, Microsoft Teams, Slack, Steam, Teamviewer, Webex, Zoom)

### Benchmark Results

#### Full Dataset Processing (NumPy Format with Streaming)

Processing the entire MalayaNetwork_GT dataset (12.5M packets) with NumPy format in streaming mode:

Note: These results are based on the end to end time of preprocessing + exporting time of the dataset. If you prefer faster processing time, you can fork and remove the exporting process in `writer.go`.

| Metric | Result |
|--------|--------|
| **Processing Time** | 16.2 seconds |
| **Throughput** | ~776,000 packets/second |
| **Peak Memory Usage** | 55 MB |
| **Average Memory Usage** | 50 MB |
| **Output Size** | 18 GB (data.npy) + 13 MB (labels.npy) |
| **Size Reduction** | 3-4x smaller than CSV equivalent |

**Key Achievements:**
- Processed **12.5 million packets** in just **16 seconds**
- Used only **55 MB peak RAM** (lightweight and memory-efficient)
- Generated **18 GB NumPy output** (vs ~50-70 GB for CSV)
- Maintained **~776K packets/second** throughput

#### Single File Processing Performance

Processing a single 24 MB PCAP file (30,885 packets):

| Format | Output Size | Processing Time | Peak RAM |
|--------|-------------|------------------|----------|
| **CSV (variable-length)** | 80 MB | 675 ms | 39 MB |
| **Parquet (variable-length)** | 23 MB | 163 ms | 81 MB |
| **CSV (fixed 1500 bytes)** | 123 MB | 915 ms | 39 MB |

#### Memory Efficiency Comparison

| Mode | Peak RAM | Use Case |
|------|---------|----------|
| **Streaming Mode** | 50-55 MB | Large datasets (recommended) |
| **Per-File Mode** | 68-92 MB | Maximum parallelism |
| **Batch Mode** | Variable | Small datasets only |


View the full benchmark results and test suite: [benchmark_results.log](benchmark_results.log)

---

### Schema

**Fixed-Length** (`--length 1500`):
```
Byte_0, Byte_1, Byte_2, ..., Byte_1499, Class
69, 0, 0, ..., 0, benign
69, 0, 0, ..., 0, malware
```

**Variable-Length** (`--length 0`):
```
Byte_0, Byte_1, Byte_2, ..., Class
69, 0, 0, benign
69, 0, 0, 52, malware
```

### Project Structure
```
GoByte/
├── main.go              # CLI entry point and orchestration
├── parser.go            # PCAP parsing and concurrent processing
├── writer.go            # CSV/Parquet/NumPy export with streaming support
├── packet_utils.go      # Packet processing utilities
├── go.mod               # Go module definition
├── go.sum               # Dependency checksums
├── example/             # NumPy usage examples and utilities
├── dataset/             # Sample PCAP files (not committed)
├── output/              # Generated output files (not committed)
└── .github/
    └── workflows/
        └── release.yml  # Automated binary builds
```

## Common Issues

### Issue: "fatal error: pcap.h: No such file or directory"

```bash
# github.com/google/gopacket/pcap
../go/pkg/mod/github.com/google/gopacket@v1.1.19/pcap/pcap_unix.go:34:10: fatal error: pcap.h: No such file or directory
   34 | #include <pcap.h>
      |          ^~~~~~~~
compilation terminated.
```
Solution: Install the libpcap development headers for your operating system. Follow the instructions in the [Installation](#installation) section.

---

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/feature1`)
3. Run tests and formatting (`go test ./... && go fmt ./...`)
4. Commit your changes (`git commit -m 'Feature: Hello there'`)
5. Push to the branch (`git push origin feature/feature1`)
6. Open a Pull Request

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

[Report Bug](https://github.com/afifhaziq/GoByte/issues) · [Request Feature](https://github.com/afifhaziq/GoByte/issues)

</div>
