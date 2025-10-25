# wiki-vote-giraph-analysis
# Make scripts executable
chmod +x run_all_metrics.sh convert_to_adjacency.py analyze_results.py

# Run complete analysis
./run_all_metrics.sh

# Analyze and compare results
python3 analyze_results.py
