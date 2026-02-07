# GoByte

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/github/actions/workflow/status/afifhaziq/GoByte/release.yml?branch=main&label=build)](https://github.com/afifhaziq/GoByte/actions/workflows/release.yml)
[![Security Scan](https://img.shields.io/github/actions/workflow/status/afifhaziq/GoByte/security.yml?branch=main&label=security)](https://github.com/afifhaziq/GoByte/actions/workflows/security.yml)
[![GitHub Release](https://img.shields.io/github/v/release/afifhaziq/GoByte?style=flat)](https://github.com/afifhaziq/GoByte/releases/latest)


**GoByte** is a fast PCAP/PCAPNG parser built in Go for preprocessing network traffic data for deep learning models. It extracts packet bytes and exports them in tabular formats (CSV or Parquet).

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
- **Multiple Formats**: Export to CSV or Parquet (compressed, columnar format)
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

If you're new to network traffic analysis and don't have PCAP files to start with, download MalayaNetwork_GT, our ready-to-use dataset. 

The easiest way to install this dataset is to use the `uv` package manager (No virtual environment required). Click this link [uv](https://docs.astral.sh/uv/getting-started/installation/) to install it.

```bash
# Install uv (if not already installed)

# Download the MalayaNetwork_GT dataset (13.5 GB, 10 traffic classes)
uvx hf download Afifhaziq/MalayaNetwork_GT \
    --repo-type dataset \
    --local-dir ./dataset \
    --include "PCAP/*"

# Process the entire dataset with GoByte (streaming mode for large datasets)
gobyte --dataset dataset/PCAP --format parquet --length 1500 --streaming
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

View the dataset on Hugging Face:

[![Dataset on HF](https://huggingface.co/datasets/huggingface/badges/resolve/main/dataset-on-hf-lg-dark.svg)](https://huggingface.co/datasets/Afifhaziq/MalayaNetwork_GT)

<iframe
  src="https://huggingface.co/datasets/Afifhaziq/MalayaNetwork_GT/embed/viewer/by_application/bittorent"
  frameborder="0"
  width="100%"
  height="500px"
></iframe>

---

### Single File Processing

Process a single PCAP file and export to CSV:

```bash
gobyte --input traffic.pcap --output results.csv
```

Process and pad/truncate packets to 1480 bytes:

```bash
gobyte --input traffic.pcap --length 1480 --format parquet
```

Mask IP addresses for privacy:

```bash
gobyte --input traffic.pcap --ipmask --format parquet
```

### Multi-File Processing with Class Labels

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
gobyte --dataset dataset --format parquet --length 1500
```

For large datasets, use streaming mode:

```bash
gobyte --dataset dataset --format parquet --length 1500 --streaming
```

Note: Labels are automatically extracted from directory names. You may need to encode them numerically before training.

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
        Output format: csv or parquet (default "csv")
  --output string
        Output file path (default: output.csv or output.parquet)
  --length int
        Desired length of output bytes (pad/truncate). 0 = keep original size (default: 0)
  --sort
        Retain packets order. Set to false to shuffle (default: true)
  --concurrent int
        Max concurrent files to process (multi-file mode) (default: 2)
  --streaming
        Use streaming mode for memory efficiency (default: false)
  --per-file
        Create separate output file for each input file (dataset mode only)
  --ipmask
        Mask source and destination IP addresses

Memory Optimization:
  --streaming      Stream packets to disk (low memory, ~200-300MB RAM)
  --per-file       Create one output per input file (lowest memory, parallel)

Note: Default mode loads all packets in memory (fast, high memory usage)
      Streaming mode: less memory, processes files sequentially
```

### Examples

#### Example 1: Basic CSV Export
```bash
gobyte --input data.pcap
# Output: output/output.csv
```

#### Example 2: Fixed-Length Packets (Recommended for Deep Learning)
```bash
gobyte --input data.pcap --length 1500 --format parquet
# All packets padded/truncated to exactly 1500 bytes
```

#### Example 3: Multi-File Dataset with Labels
```bash
gobyte --dataset my_dataset --format parquet --concurrent 4
# Processes multiple files in parallel and assigns labels from directory names
```

#### Example 4: Large Dataset with Streaming Mode
```bash
gobyte --dataset my_dataset --format parquet --length 1500 --streaming
# Memory-efficient processing for large datasets (uses ~200-300MB RAM)
```

#### Example 5: Per-File Output Mode
```bash
gobyte --dataset my_dataset --format parquet --per-file
# Creates separate output file for each input file (maximum memory efficiency)
```

#### Example 6: Variable-Length Packets
```bash
gobyte --input data.pcap --length 0 --format csv
# Keeps original packet sizes (no padding/truncation)
```

#### Example 7: IP Address Masking for Privacy
```bash
gobyte --input data.pcap --ipmask --format parquet
# Masks source and destination IP addresses (sets them to 0.0.0.0)
```

#### Example 8: Combined Options
```bash
gobyte --dataset my_dataset --length 1500 --ipmask --streaming --format parquet
# Fixed-length packets with IP masking in memory-efficient streaming mode
```

---

## Output Formats

Both CSV and Parquet use the same schema for compatibility with ML frameworks.

### CSV Format
- Human-readable text format
- Large file sizes
- Compatible with all data analysis tools
- Use for small to medium datasets

### Parquet Format (Recommended)
- Compressed columnar format
- 10-20x smaller than CSV
- Optimized for ML frameworks (PyTorch, TensorFlow)
- Use for large datasets and production workflows

### Schema

**Fixed-Length** (`--length 1500`):
```
Byte_0, Byte_1, Byte_2, ..., Byte_1499, Class
8, 0, 69, 0, 0, ..., 0, benign
8, 0, 69, 0, 0, ..., 0, malware
```

**Variable-Length** (`--length 0`):
```
Byte_0, Byte_1, Byte_2, ..., Class
8, 0, 69, 0, 0, benign
8, 0, 69, 0, 0, 52, malware
```

### Project Structure
```
GoByte/
├── main.go              # CLI entry point and orchestration
├── parser.go            # PCAP parsing and concurrent processing
├── writer.go            # CSV/Parquet export with streaming support
├── go.mod               # Go module definition
├── go.sum               # Dependency checksums
├── dataset/             # Sample PCAP files (not committed)
├── output/              # Generated output files (not committed)
└── .github/
    └── workflows/
        └── release.yml  # Automated binary builds
```

---

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/feature1`)
3. Run tests and formatting (`go test ./... && go fmt ./...`)
4. Commit your changes (`git commit -m 'Hello there'`)
5. Push to the branch (`git push origin feature/feature1`)
6. Open a Pull Request

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

[Report Bug](https://github.com/afifhaziq/GoByte/issues) · [Request Feature](https://github.com/afifhaziq/GoByte/issues)

</div>
