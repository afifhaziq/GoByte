# GoByte NumPy Examples

This directory contains example scripts showing how to use GoByte's NumPy output files.

## Why NumPy Format?

GoByte's NumPy export format provides significant advantages over CSV:

- **3-4x smaller** file sizes (18 GB vs 50-70 GB for the same dataset)
- **10-20x faster** loading times (2-5 seconds vs 30-60 seconds)
- **Native ML/DL integration** with PyTorch, TensorFlow, and JAX (zero-copy)
- **Memory efficient** streaming mode (~200-300 MB RAM)
- **Binary format** - no string conversion overhead

**Recommendation:** Use NumPy format for all ML/DL workflows. Reserve CSV for small samples and human inspection only.

## Files

### `utils.py`
Shared utility functions for loading NumPy files and class mappings.

### `01_basic_loading.py`
Basic example: Load and inspect NumPy files.

```bash
python3 01_basic_loading.py
```

### `02_class_mapping.py`
Example: Use class mapping JSON to convert integer labels to names.

```bash
python3 02_class_mapping.py
```

## Prerequisites

We recommend using [uv](https://github.com/astral-sh/uv) for fast and reliable package management:

```bash
# Using uvx (recommended - no installation needed)
uvx --with numpy python3 01_basic_loading.py
uvx --with numpy python3 02_class_mapping.py

# Or install NumPy locally
uv add numpy
```

Alternatively, you can use `pip` if you prefer:
```bash
pip install numpy
```

## Usage

### Generating NumPy Files

1. **Streaming Mode** (Recommended for large datasets - memory efficient):
   ```bash
   ./gobyte --dataset PCAP --format numpy --length 1500 --streaming --output test_classes.npy
   ```

2. **Batch Mode** (For single files - in-memory). Caution: This mode will load all packets into memory, which may cause OOM errors for large datasets based on --length flag.
   ```bash
   ./gobyte --input file.pcap --format numpy --length 1500 --output single.npy
   ```

### Running Examples

Run examples using `uvx` (recommended):
```bash
cd example
uvx --with numpy python3 01_basic_loading.py
uvx --with numpy python3 02_class_mapping.py
```

Or with locally installed NumPy:
```bash
python3 01_basic_loading.py
python3 02_class_mapping.py
```

## Output Files Structure

After running GoByte, you'll have:
- `output/test_classes_data.npy` - Packet data (N × 1500 uint8)
- `output/test_classes_labels.npy` - Class labels (N × 1 uint8)
- `output/test_classes_classes.json` - Class ID to name mapping

### Format Details
- **Format:** NumPy v2.0 (binary format)
- **Data Type:** `uint8` (0-255)
- **Shape:** Data is (N, 1500), Labels is (N,)
- **Storage:** Raw binary (no string conversion)

## Quick Start

```python
from utils import load_npy, load_class_mapping

# Load data
data = load_npy('../output/test_classes_data.npy')
labels = load_npy('../output/test_classes_labels.npy')

# Load class mapping
id_to_name, name_to_id = load_class_mapping('../output/test_classes_classes.json')

# Use the data
print(f"Data shape: {data.shape}")
print(f"Labels shape: {labels.shape}")
```
