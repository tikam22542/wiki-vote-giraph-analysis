#!/usr/bin/env python3
"""Convert wiki-Vote.txt to Giraph adjacency list format."""

from collections import defaultdict

def convert_edge_list_to_adjacency(input_file, output_file):
    adjacency = defaultdict(list)
    all_vertices = set()
    
    print(f"Reading {input_file}...")
    with open(input_file, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            parts = line.strip().split()
            if len(parts) == 2:
                src, dst = int(parts[0]), int(parts[1])
                adjacency[src].append(dst)
                all_vertices.add(src)
                all_vertices.add(dst)
    
    print(f"Found {len(all_vertices)} vertices, {sum(len(v) for v in adjacency.values())} edges")
    
    print(f"Writing to {output_file}...")
    with open(output_file, 'w') as f:
        for vertex in sorted(all_vertices):
            neighbors = adjacency.get(vertex, [])
            neighbor_str = ' '.join([f"{n}:0" for n in neighbors])
            if neighbor_str:
                f.write(f"{vertex} {neighbor_str}\n")
            else:
                f.write(f"{vertex}\n")
    
    print("✓ Conversion complete")

if __name__ == "__main__":
    convert_edge_list_to_adjacency("wiki-Vote.txt", "wiki-Vote-adjacency.txt")
