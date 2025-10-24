# PySpark Lineage MVP

Files:

- job1.py, job2.py, job3.py, utils.py: sample PySpark-like scripts
- config.yml: configuration used by jobs
- analyzer.py: static analyzer for PySpark files (AST-based)
- requirements.txt: python deps

## Setup (local)

1. Create virtualenv:
   python3 -m venv venv
   source venv/bin/activate

2. Install:
   pip install -r requirements.txt

3. Run analyzer:
   python analyzer.py

Output:

- lineage_output.json
- lineage_graph.png
