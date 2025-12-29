# Comparative Analysis of Optimization Techniques in SQL and NoSQL Databases

# Project Overview
This project conducts an analysis of query optimization techniques in SQL (Oracle) and NoSQL (MongoDB) databases using a Brazilian e-commerce dataset. The research investigates three key optimization strategies: indexing, caching, and scalability mechanisms.

# Research Objectives
1. To investigate how different indexing strategies affect query performance in SQL vs NoSQL databases under varying selectivity.
2. To assess the effectiveness of Read-Through Caching in improving SQL and NoSQL database performance.
3. To identify infrastructure prerequisites and implementation barriers for scalability-oriented query optimization techniques in constrained environments

# Research Questions
1. How do different indexing strategies (single-column B-tree vs. multi-level composite) affect query response time, throughput and storage overhead in SQL and NoSQL databases under varying query selectivity?
2. How does Read-Through Caching impact query performance in SQL vs NoSQL databases?
3. What are the implementation barriers for deploying partitioning (SQL) and sharding (NoSQL) as scalability-oriented query optimization techniques in resource-constrained environments?

# Project Structure
The project is organized into numbered folders that should be executed sequentially:
```
G80_System_Files/
├── 1.0_Datasets/                    # Contains dataset information (data not in repo)
├── 2.0_Data_Preparation/           # Data sampling and preparation scripts
├── 3.0_RO1_Indexing/               # Research Objective 1: Indexing analysis
├── 4.0_RO2_Caching/                # Research Objective 2: Caching analysis
└── 5.0_RO3_Scalability/            # Research Objective 3: Scalability analysis

Important: Maintain the same folder hierarchy as shown above, or update all file paths in the Python scripts accordingly.
```

# Execution Instructions
# Part 1: Data Preparations
Execute in order:
1. Run ```2.1_sampling_data_csv.py``` in Python to sample 10% of the original dataset (due to Oracle APEX storage limits)
2. Execute SQL statements in ```2.2_create_tables_SQL.txt``` in Oracle APEX to create tables and load the sampled CSV data
3. Run ```2.3_convert_json.py``` in Python to convert the sampled data to JSON format

# Part 2: Indexing Analysis
Execute in order:
1. Run SQL statements in ```3.1_indexing_SQL.txt``` in Oracle APEX
2. Execute JavaScript commands in ```3.2_indexing_NoSQL.txt``` in MongoDB Shell

# Part 3: Caching Analysis
Execute in order:
1. Run SQL statements in ```4.1_caching_SQL.txt``` in Oracle APEX
2. Execute Python scripts in ```4.2_caching_NoSQL/ folder in numerical order (4.2.1, 4.2.2, 4.2.3, 4.2.4)```

# Part 4: Sharding and Partitioning Analysis
Execute in order:
1. Run SQL statements in ```5.1_partitioning_SQL.txt``` in Oracle APEX
2. Run ```5.2_sampling_data2.py``` in Python to sample another 10% of data
3. Run ```5.3_convert_json2.py``` in Python to convert new sample to JSON
4. Load new JSON files into MongoDB
5. Execute JavaScript commands in ```5.4_sharding_NoSQL.txt``` in MongoDB Shell

# Important Notes
1. File Paths: If you change the folder structure, update all file paths in Python scripts accordingly
2. Execution Order: Always follow the folder numbering sequence (2.0 → 3.0 → 4.0 → 5.0)
3. Results Variation: Results such as query execution times may vary between runs due to caching mechanisms at database and OS levels
4. Database Credentials: Update connection strings in Python scripts with your Oracle, MongoDB, and Redis credentials
5. Storage Limits: The 10% sampling is necessary due to Oracle APEX tablespace quotas

# Script Execution Guidelines
- SQL Scripts: Run statements individually or in blocks as indicated in comments
- Python Scripts: Execute from terminal/IDE with appropriate Python environment
- MongoDB Scripts: Run in MongoDB Shell or MongoDB Compass query interface
