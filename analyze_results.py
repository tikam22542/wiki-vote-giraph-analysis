#!/usr/bin/env python3
"""
Analyze and compare Neo4j vs Giraph performance results.
"""

import pandas as pd
import sys

def create_comparison_table():
    """Create performance comparison table"""
    
    # Neo4j times from your execution
    neo4j_times = {
        'WCC': 2.5,
        'SCC': 2.8,
        'Triangle Count': 4.0,
        'Clustering Coefficient': 3.0,
        'Diameter': 6.0,
        'TOTAL': 15.41
    }
    
    # Read Giraph times
    try:
        giraph_df = pd.read_csv('giraph_times.csv', names=['Metric', 'Time'])
        giraph_times = dict(zip(giraph_df['Metric'], giraph_df['Time']))
    except:
        print("Error: giraph_times.csv not found. Run Giraph computations first.")
        sys.exit(1)
    
    # Create comparison
    metrics = ['WCC', 'SCC', 'Triangle Count', 'Clustering Coefficient', 'Diameter', 'TOTAL']
    
    comparison = []
    for metric in metrics:
        neo4j_time = neo4j_times.get(metric, 0)
        giraph_time = giraph_times.get(metric, 0)
        
        if giraph_time > 0:
            faster = 'Neo4j' if neo4j_time < giraph_time else 'Giraph'
            diff = abs(((giraph_time - neo4j_time) / neo4j_time) * 100)
        else:
            faster = 'N/A'
            diff = 0
        
        comparison.append({
            'Metric': metric,
            'Neo4j Time (s)': neo4j_time,
            'Giraph Time (s)': giraph_time,
            'Faster Tool': faster,
            '% Difference': f"{diff:.1f}%"
        })
    
    df = pd.DataFrame(comparison)
    
    print("\n" + "="*80)
    print("PERFORMANCE COMPARISON: Neo4j vs Apache Giraph")
    print("="*80)
    print(df.to_string(index=False))
    print("="*80)
    
    # Save to CSV
    df.to_csv('comparison_results.csv', index=False)
    print("\nComparison saved to: comparison_results.csv")
    
    # Analysis
    print("\n" + "="*80)
    print("ANALYSIS")
    print("="*80)
    
    neo4j_total = neo4j_times['TOTAL']
    giraph_total = giraph_times.get('TOTAL', 0)
    
    if giraph_total > 0:
        speedup = giraph_total / neo4j_total
        print(f"Neo4j Total Time: {neo4j_total:.2f}s")
        print(f"Giraph Total Time: {giraph_total:.2f}s")
        print(f"Neo4j Speedup: {speedup:.2f}x faster")
        
        if speedup > 1:
            print("\n✓ Neo4j outperformed Giraph for this dataset size (7K nodes, 103K edges)")
            print("  Reason: Neo4j's in-memory graph projections and optimized GDS algorithms")
            print("  are more efficient for medium-scale graphs.")
        else:
            print("\n✓ Giraph outperformed Neo4j")
            print("  Reason: Distributed processing advantage for this workload.")
    
    print("="*80)

if __name__ == "__main__":
    create_comparison_table()
