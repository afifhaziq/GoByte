#!/usr/bin/env python3
"""
Example 2: Using Class Mappings

This example shows how to use the classes.json file to map
integer labels to human-readable class names.
"""

import numpy as np
from utils import load_npy, load_class_mapping

# Load data
data = load_npy('../output/test_classes_data.npy')
labels = load_npy('../output/test_classes_labels.npy')

# Load class mapping
id_to_name, name_to_id = load_class_mapping('../output/test_classes_classes.json')

if id_to_name is None:
    print("WARNING: No class mapping file found.")
    print("   (This is normal if processing without class labels)")
    exit(0)

print("Class Mapping:")
for class_id in sorted(id_to_name.keys()):
    print(f"  {class_id}: {id_to_name[class_id]}")

# Show class distribution
print("\nClass Distribution:")
unique_classes, counts = np.unique(labels, return_counts=True)
for class_id, count in zip(unique_classes, counts):
    class_name = id_to_name.get(class_id, f"Unknown({class_id})")
    percentage = (count / len(labels)) * 100
    print(f"  {class_id:2d}: {class_name:20s} - {count:10,} packets ({percentage:5.2f}%)")

# Example: Convert labels to class names
print("\nExample: Convert integer labels to class names")
sample_labels = labels[:10]
sample_names = [id_to_name[int(l)] for l in sample_labels]
print(f"First 10 labels as integers: {sample_labels}")
print(f"First 10 labels as names:    {sample_names}")

# Example: Get class ID from name
print("\nExample: Get class ID from name")
if 'Steam' in name_to_id:
    steam_id = name_to_id['Steam']
    print(f"'Steam' maps to class ID: {steam_id}")
