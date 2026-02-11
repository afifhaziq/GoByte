#!/usr/bin/env python3
"""
Utility functions for loading GoByte NumPy files
"""

import numpy as np
import ast


def load_npy(filename):
    """
    Load a NumPy .npy file with automatic fallback.
    
    Args:
        filename: Path to .npy file
        
    Returns:
        numpy.ndarray: Loaded array
    """
    try:
        # Try standard NumPy loading first
        return np.load(filename)
    except (ValueError, Exception):
        # Fallback: Manual parsing (always works)
        return load_npy_manual(filename)


def load_npy_manual(filename):
    """
    Manually parse a NumPy .npy file (bypasses NumPy's auto-detection).
    Supports both v1.0 and v2.0 formats.
    
    Args:
        filename: Path to .npy file
        
    Returns:
        numpy.ndarray: Loaded array
    """
    with open(filename, 'rb') as f:
        magic = f.read(6)  # \x93NPY + version
        version_major = magic[4]
        version_minor = magic[5]
        
        # Read header length based on version
        if version_major == 1:
            header_len = int.from_bytes(f.read(2), 'little')  # v1.0: 2 bytes
        else:
            header_len = int.from_bytes(f.read(4), 'little')  # v2.0+: 4 bytes
        
        # Read header
        header_bytes = f.read(header_len)
        # Decode and find the actual dict (before padding)
        header_str = header_bytes.decode('latin1', errors='ignore')
        # Find the dict part (between first { and last })
        dict_start = header_str.find('{')
        dict_end = header_str.rfind('}')
        if dict_start != -1 and dict_end != -1:
            header = header_str[dict_start:dict_end + 1]
        else:
            header = header_str.strip('\x00').strip()
        
        header_dict = ast.literal_eval(header)
        
        # Calculate where data actually starts (after 64-byte aligned header)
        header_start = 6 + (2 if version_major == 1 else 4)
        total_header_size = header_start + header_len
        # Round up to next 64-byte boundary
        data_start = ((total_header_size + 63) // 64) * 64
        # Seek to data start
        f.seek(data_start)
        
        return np.fromfile(f, dtype=np.dtype(header_dict['descr'])).reshape(header_dict['shape'])


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
