#!/usr/bin/env python3
"""
Example 2: Using Class Mappings

This example shows how to use the classes.json file to map
integer labels to human-readable class names.
"""

import numpy as np

def load_class_mapping(classes_file):
    """
    Load class ID to name mapping from JSON file.
    
    Args:
        classes_file: Path to classes.json file
        
    Returns:
        tuple: (id_to_name dict, name_to_id dict)
    """
    import json
    import os
    
    if not os.path.exists(classes_file):
        return None, None
    
    with open(classes_file, 'r') as f:
        class_map = json.load(f)
    
    id_to_name = {int(k): v for k, v in class_map.items()}
    name_to_id = {v: int(k) for k, v in class_map.items()}
    
    return id_to_name, name_to_id


# Load data
data = np.load('../output/output_data.npy')
labels = np.load('../output/output_labels.npy')

# Load class mapping
id_to_name, name_to_id = load_class_mapping('../output/output_classes.json')

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

# # Example: Get class ID from name
# print("\nExample: Get class ID from name")
# if 'Steam' in name_to_id:
#     steam_id = name_to_id['Steam']
#     print(f"'Steam' maps to class ID: {steam_id}")
