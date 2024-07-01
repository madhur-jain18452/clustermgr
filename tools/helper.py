"""
Defines helper functions and variables to work with memory calculation

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""
byte = 1

BINARY_CONVERSION_FACTOR = 1024
DECIMAL_CONVERSION_FACTOR = 1000

KiB = BINARY_CONVERSION_FACTOR * byte
MiB = BINARY_CONVERSION_FACTOR * KiB
GiB = BINARY_CONVERSION_FACTOR * MiB
TiB = BINARY_CONVERSION_FACTOR * GiB

KB = DECIMAL_CONVERSION_FACTOR * byte
MB = DECIMAL_CONVERSION_FACTOR * KB
GB = DECIMAL_CONVERSION_FACTOR * MB
TB = DECIMAL_CONVERSION_FACTOR * GB


def convert_mb_to_gb(memory_in_mb) -> float:
    return memory_in_mb / BINARY_CONVERSION_FACTOR


def convert_gb_to_mb(memory_in_gb) -> float:
    return memory_in_gb * BINARY_CONVERSION_FACTOR


def convert_bytes_to_gb(memory_in_bytes) -> float:
    return memory_in_bytes / (BINARY_CONVERSION_FACTOR**3)


def bytes_to_mb(memory_in_bytes) -> float:
    return memory_in_bytes / (BINARY_CONVERSION_FACTOR**2)
