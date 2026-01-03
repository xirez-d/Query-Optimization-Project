# -*- coding: utf-8 -*-
"""
This python script generate visualizations for Research Objective 1
"""


# QUERY RESPONSE TIME VISUALIZATION

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Data preparation
sql_data = {
    'Test Case': [
        'B-Tree: Delivered', 'B-Tree: Shipped', 'B-Tree: Processing',
        'Composite: Low', 'Composite: Medium', 'Composite: High'
    ],
    'No Index': [0.005452, 0.001144, 0.000896, 0.034881, 0.002835, 0.011075],
    'With Index': [0.009226, 0.000478, 0.00042, 0.02759, 0.002464, 0.016979],
    'Improvement %': [-69.3, 58.2, 53.1, 20.9, 13.1, -53.3]
}

nosql_data = {
    'Test Case': [
        'B-Tree: Delivered', 'B-Tree: Shipped', 'B-Tree: Processing',
        'Composite: Low', 'Composite: Medium', 'Composite: High'
    ],
    'No Index': [0.0550, 0.0170, 0.0500, 0.3580, 8.0340, 76.2280],
    'With Index': [0.0190, 0.0040, 0.0050, 0.1880, 7.8140, 80.5690],
    'Improvement %': [65.5, 76.5, 90.0, 27.1, 2.7, -5.7]
}

sql_df = pd.DataFrame(sql_data)
nosql_df = pd.DataFrame(nosql_data)

x = np.arange(len(sql_df))
width = 0.35

# ============================================================================
# PLOT 1: SQL Response Time Comparison
# ============================================================================
fig, ax = plt.subplots(figsize=(10, 6))

bars1 = ax.bar(x - width/2, sql_df['No Index'], width, label='No Index', color='#e74c3c', alpha=0.8)
bars2 = ax.bar(x + width/2, sql_df['With Index'], width, label='With Index', color='#3498db', alpha=0.8)

ax.set_xlabel('Test Cases', fontsize=12, fontweight='bold')
ax.set_ylabel('Query Response Time (seconds) - Log Scale', fontsize=12, fontweight='bold')
ax.set_title('SQL Database Performance Impact', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(sql_df['Test Case'], rotation=45, ha='right')
ax.legend(fontsize=11)
ax.set_yscale('log')
ax.grid(axis='y', alpha=0.3, which='both')

# Add value labels on bars
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.4f}s', ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig('sql_response_time_comparison.png', dpi=300, bbox_inches='tight')
plt.show()

# ============================================================================
# PLOT 2: NoSQL Response Time Comparison
# ============================================================================
fig, ax = plt.subplots(figsize=(10, 6))

bars3 = ax.bar(x - width/2, nosql_df['No Index'], width, label='No Index', color='#e74c3c', alpha=0.8)
bars4 = ax.bar(x + width/2, nosql_df['With Index'], width, label='With Index', color='#3498db', alpha=0.8)

ax.set_xlabel('Test Cases', fontsize=12, fontweight='bold')
ax.set_ylabel('Query Response Time (seconds) - Log Scale', fontsize=12, fontweight='bold')
ax.set_title('NoSQL Database Performance Impact', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(nosql_df['Test Case'], rotation=45, ha='right')
ax.legend(fontsize=11)
ax.set_yscale('log')
ax.grid(axis='y', alpha=0.3, which='both')

# Add value labels on bars
for bars in [bars3, bars4]:
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}s', ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig('nosql_response_time_comparison.png', dpi=300, bbox_inches='tight')
plt.show()

# ============================================================================
# PLOT 3: SQL Improvement Percentage
# ============================================================================
fig, ax = plt.subplots(figsize=(10, 6))

colors_sql = ['#27ae60' if val > 0 else '#e74c3c' for val in sql_df['Improvement %']]
bars5 = ax.bar(x, sql_df['Improvement %'], color=colors_sql, alpha=0.8)

ax.set_xlabel('Test Cases', fontsize=12, fontweight='bold')
ax.set_ylabel('Performance Improvement (%)', fontsize=12, fontweight='bold')
ax.set_title('SQL Indexing Performance Impact (%)', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(sql_df['Test Case'], rotation=45, ha='right')
ax.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
ax.grid(axis='y', alpha=0.3)

# Add value labels
for i, bar in enumerate(bars5):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.1f}%', ha='center', 
            va='bottom' if height > 0 else 'top', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('sql_improvement_percentage.png', dpi=300, bbox_inches='tight')
plt.show()

# ============================================================================
# PLOT 4: NoSQL Improvement Percentage
# ============================================================================
fig, ax = plt.subplots(figsize=(10, 6))

colors_nosql = ['#27ae60' if val > 0 else '#e74c3c' for val in nosql_df['Improvement %']]
bars6 = ax.bar(x, nosql_df['Improvement %'], color=colors_nosql, alpha=0.8)

ax.set_xlabel('Test Cases', fontsize=12, fontweight='bold')
ax.set_ylabel('Performance Improvement (%)', fontsize=12, fontweight='bold')
ax.set_title('NoSQL Indexing Performance Impact (%)', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(nosql_df['Test Case'], rotation=45, ha='right')
ax.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
ax.grid(axis='y', alpha=0.3)

# Add value labels
for i, bar in enumerate(bars6):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.1f}%', ha='center', 
            va='bottom' if height > 0 else 'top', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('nosql_improvement_percentage.png', dpi=300, bbox_inches='tight')
plt.show()

print("All 4 visualizations created successfully!")
print("\nFiles generated:")
print("1. sql_response_time_comparison.png")
print("2. nosql_response_time_comparison.png")
print("3. sql_improvement_percentage.png")
print("4. nosql_improvement_percentage.png")

# THROUGHPUT VISUALIZATION
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Data preparation
sql_throughput_data = {
    'Test Case': [
        'B-Tree: Delivered', 'B-Tree: Shipped', 'B-Tree: Processing',
        'Composite: Low', 'Composite: Medium', 'Composite: High'
    ],
    'No Index': [2216, 3094, 3162, 1132, 531, 98],
    'With Index': [2494, 3383, 3359, 16904, 681, 73],
    'Improvement %': [12.5, 9.3, 6.2, 1393.3, 28.2, -25.5]
}

nosql_throughput_data = {
    'Test Case': [
        'B-Tree: Delivered', 'B-Tree: Shipped', 'B-Tree: Processing',
        'Composite: Low', 'Composite: Medium', 'Composite: High'
    ],
    'No Index': [59.24, 70.07, 72.35, 56.48, 57.10, 52.57],
    'With Index': [86.23, 228.26, 235.79, 209.82, 206.70, 210.84],
    'Improvement %': [45.6, 225.7, 225.9, 271.5, 262.0, 301.1]
}

sql_tp_df = pd.DataFrame(sql_throughput_data)
nosql_tp_df = pd.DataFrame(nosql_throughput_data)

x = np.arange(len(sql_tp_df))
width = 0.35

# ============================================================================
# PLOT 1: SQL Throughput Comparison
# ============================================================================
fig, ax = plt.subplots(figsize=(10, 6))

bars1 = ax.bar(x - width/2, sql_tp_df['No Index'], width, label='No Index', color='#e74c3c', alpha=0.8)
bars2 = ax.bar(x + width/2, sql_tp_df['With Index'], width, label='With Index', color='#3498db', alpha=0.8)

ax.set_xlabel('Test Cases', fontsize=12, fontweight='bold')
ax.set_ylabel('Throughput (queries per second)', fontsize=12, fontweight='bold')
ax.set_title('SQL Database Throughput Impact', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(sql_tp_df['Test Case'], rotation=45, ha='right')
ax.legend(fontsize=11)
ax.grid(axis='y', alpha=0.3)

# Add value labels on bars
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.0f}', ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig('sql_throughput_comparison.png', dpi=300, bbox_inches='tight')
plt.show()

# ============================================================================
# PLOT 2: NoSQL Throughput Comparison
# ============================================================================
fig, ax = plt.subplots(figsize=(10, 6))

bars3 = ax.bar(x - width/2, nosql_tp_df['No Index'], width, label='No Index', color='#e74c3c', alpha=0.8)
bars4 = ax.bar(x + width/2, nosql_tp_df['With Index'], width, label='With Index', color='#3498db', alpha=0.8)

ax.set_xlabel('Test Cases', fontsize=12, fontweight='bold')
ax.set_ylabel('Throughput (queries per second)', fontsize=12, fontweight='bold')
ax.set_title('NoSQL Database Throughput Impact', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(nosql_tp_df['Test Case'], rotation=45, ha='right')
ax.legend(fontsize=11)
ax.grid(axis='y', alpha=0.3)

# Add value labels on bars
for bars in [bars3, bars4]:
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}', ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig('nosql_throughput_comparison.png', dpi=300, bbox_inches='tight')
plt.show()

# ============================================================================
# PLOT 3: SQL Throughput Improvement Percentage
# ============================================================================
fig, ax = plt.subplots(figsize=(10, 6))

colors_sql = ['#27ae60' if val > 0 else '#e74c3c' for val in sql_tp_df['Improvement %']]
bars5 = ax.bar(x, sql_tp_df['Improvement %'], color=colors_sql, alpha=0.8)

ax.set_xlabel('Test Cases', fontsize=12, fontweight='bold')
ax.set_ylabel('Throughput Improvement (%)', fontsize=12, fontweight='bold')
ax.set_title('SQL Indexing Throughput Impact (%)', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(sql_tp_df['Test Case'], rotation=45, ha='right')
ax.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
ax.grid(axis='y', alpha=0.3)

# Add value labels
for i, bar in enumerate(bars5):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.1f}%', ha='center', 
            va='bottom' if height > 0 else 'top', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('sql_throughput_improvement.png', dpi=300, bbox_inches='tight')
plt.show()

# ============================================================================
# PLOT 4: NoSQL Throughput Improvement Percentage
# ============================================================================
fig, ax = plt.subplots(figsize=(10, 6))

colors_nosql = ['#27ae60' if val > 0 else '#e74c3c' for val in nosql_tp_df['Improvement %']]
bars6 = ax.bar(x, nosql_tp_df['Improvement %'], color=colors_nosql, alpha=0.8)

ax.set_xlabel('Test Cases', fontsize=12, fontweight='bold')
ax.set_ylabel('Throughput Improvement (%)', fontsize=12, fontweight='bold')
ax.set_title('NoSQL Indexing Throughput Impact (%)', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(nosql_tp_df['Test Case'], rotation=45, ha='right')
ax.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
ax.grid(axis='y', alpha=0.3)

# Add value labels
for i, bar in enumerate(bars6):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.1f}%', ha='center', 
            va='bottom' if height > 0 else 'top', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('nosql_throughput_improvement.png', dpi=300, bbox_inches='tight')
plt.show()

print("All 4 throughput visualizations created successfully!")
print("\nFiles generated:")
print("1. sql_throughput_comparison.png")
print("2. nosql_throughput_comparison.png")
print("3. sql_throughput_improvement.png")
print("4. nosql_throughput_improvement.png")


# INDEX STORAGE OVERHEAD VISUALIZATION
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Data preparation
storage_data = {
    'Index Type': ['B-tree Index', 'Composite Index'],
    'SQL Size (MB)': [0.31, 0.38],
    'NoSQL Size (MB)': [0.06, 0.21],
    'SQL Overhead (%)': [15.63, 18.75],
    'NoSQL Overhead (%)': [4.19, 13.87]
}

storage_df = pd.DataFrame(storage_data)

# Visualization 1: Storage Overhead Percentage Comparison
plt.figure(figsize=(10, 6))
ax1 = plt.gca()

x = np.arange(len(storage_df))
width = 0.35

# Overhead Percentage Chart
bars1 = ax1.bar(x - width/2, storage_df['SQL Overhead (%)'], width, label='SQL', color='#e74c3c', alpha=0.8)
bars2 = ax1.bar(x + width/2, storage_df['NoSQL Overhead (%)'], width, label='NoSQL', color='#3498db', alpha=0.8)

ax1.set_xlabel('Index Type', fontsize=12, fontweight='bold')
ax1.set_ylabel('Storage Overhead (%)', fontsize=12, fontweight='bold')
ax1.set_title('Index Storage Overhead Comparison', fontsize=14, fontweight='bold')
ax1.set_xticks(x)
ax1.set_xticklabels(storage_df['Index Type'], rotation=0)
ax1.legend()
ax1.grid(axis='y', alpha=0.3)

# Add value labels on bars
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('storage_overhead_comparison.png', dpi=300, bbox_inches='tight')
plt.show()

# Visualization 2: Index Size Comparison
plt.figure(figsize=(10, 6))
ax2 = plt.gca()

# Index Size Chart
bars3 = ax2.bar(x - width/2, storage_df['SQL Size (MB)'], width, label='SQL', color='#e74c3c', alpha=0.8)
bars4 = ax2.bar(x + width/2, storage_df['NoSQL Size (MB)'], width, label='NoSQL', color='#3498db', alpha=0.8)

ax2.set_xlabel('Index Type', fontsize=12, fontweight='bold')
ax2.set_ylabel('Index Size (MB)', fontsize=12, fontweight='bold')
ax2.set_title('Index Size Comparison', fontsize=14, fontweight='bold')
ax2.set_xticks(x)
ax2.set_xticklabels(storage_df['Index Type'], rotation=0)
ax2.legend()
ax2.grid(axis='y', alpha=0.3)

# Add value labels on bars
for bars in [bars3, bars4]:
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f} MB', ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('index_size_comparison.png', dpi=300, bbox_inches='tight')
plt.show()

print("Storage overhead visualizations created successfully!")


