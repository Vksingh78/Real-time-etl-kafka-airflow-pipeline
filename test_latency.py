#!/usr/bin/env python3

import matplotlib.pyplot as plt
import numpy as np

# Latency data from our test
# P50 = 607ms, P95 = 622ms, P99 = 763ms

percentiles = [50, 95, 99]
latencies = [607, 622, 763]

# Create bar chart
plt.figure(figsize=(8, 5))
bars = plt.bar(percentiles, latencies, color=['blue', 'green', 'red'])
plt.xlabel('Percentile')
plt.ylabel('Latency (ms)')
plt.title('ETL Pipeline Latency Distribution')
plt.ylim(0, 800)

# Add values on top of bars
for bar, latency in zip(bars, latencies):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 10, 
             f'{latency} ms', ha='center', va='bottom')

plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.savefig('latency_graph.png', dpi=150)
plt.show()

print("Graph saved as latency_graph.png")