#!/usr/bin/env python3
"""
Example 1: Basic Loading of GoByte NumPy Files

This example shows how to load the data and labels files.
"""

import numpy as np

# Load data files
print("Loading NumPy files...")
data = np.load('../output/output_data.npy')
labels = np.load('../output/output_labels.npy')

# Show basic information
print(f"\nSuccessfully loaded!")
print(f"Data shape: {data.shape}")
print(f"Data dtype: {data.dtype}")
print(f"Labels shape: {labels.shape}")
print(f"Labels dtype: {labels.dtype}")
print(f"Unique class IDs: {np.unique(labels)}")

# Show value ranges
print(f"\nData value range: [{data.min()}, {data.max()}]")
print(f"Labels value range: [{labels.min()}, {labels.max()}]")

# Show sample data
print(f"\nSample (first packet, first 20 bytes):")
print(f"   {data[0, :20]}")

print(f"\nSample labels (first 10):")
print(f"   {labels[:10]}")
