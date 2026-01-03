# -*- coding: utf-8 -*-
"""
This python script generate visualizations for Research Objective 2
"""

# QUERY RESPONSE TIME
import matplotlib.pyplot as plt
import numpy as np

# Updated Data
cache_status = ['Without Cache', 'Cache Miss', 'Cache Hit']
sql_response = [0.11, 10.84, 0.21]
nosql_response = [2.59, 612.39, 303.22]

# Set bar width and positions
x = np.arange(len(cache_status))  # label locations
width = 0.35  # width of bars

# Create figure and axes
fig, ax = plt.subplots(figsize=(10,6))

# Plot bars
bars_sql = ax.bar(x - width/2, sql_response, width, label='SQL', color='steelblue')
bars_nosql = ax.bar(x + width/2, nosql_response, width, label='NoSQL', color='darkorange')

# Set logarithmic y-axis
ax.set_yscale('log')

# Add labels, title, legend
ax.set_xlabel('Cache Status', fontsize=12, fontweight='bold')
ax.set_ylabel('Response Time (ms)', fontsize=12, fontweight='bold')
ax.set_title('Query Response Time under Read-Through Caching', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(cache_status)
ax.legend()

# Add data labels on top of bars (log scale aware)
def add_labels(bars):
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f'{height:.2f}',
                    xy=(bar.get_x() + bar.get_width()/2, height),
                    xytext=(0,3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=10, fontweight='bold')

add_labels(bars_sql)
add_labels(bars_nosql)

# Show plot
plt.tight_layout()
plt.savefig('caching_query_response_time.png', dpi=300, bbox_inches='tight')
plt.show()
