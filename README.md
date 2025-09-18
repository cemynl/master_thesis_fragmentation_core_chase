# Master Thesis: Fragmentation Core Chase

This repository contains the implementation of a database fragmentation system using core chase algorithms for a master's thesis. The system processes medical database records through a multi-step pipeline involving TGD (Tuple Generating Dependencies) generation, security extraction, core chase computation, graph analysis, and data fragmentation.

## Overview

The system implements a complete pipeline for database fragmentation with the following key components:

1. **Database Creation and Population** (`a1_0_create_fill_db.py`)
2. **TGD Generation** (`a2_1_sensitive_tgd_information.py`)
3. **Security Extraction** (`a3_0_move_to_fo.py`)
4. **Core Chase Algorithm** (`a4_core_chase.py`)
5. **Graph Construction** (`a5_graph.py`)
6. **Path Traversal** (`a6_0_traversal.py`)
7. **Minimal Union Computation** (`a7_minimal_union.py`)
8. **Final Fragmentation** (`a8_fragmentation.py`)

## Quick Start with Benchmark Runner

### Using the Automated Benchmark Runner

The easiest way to run the complete pipeline is using the benchmark runner:

```bash
python bench_runner.py
```

This will automatically:
- Create base databases with different patient sizes (250,000, 500,000, 1,000,000)
- Generate TGDs with different rule counts (100, 200, 400)
- Execute the complete pipeline for each combination
- Track runtime performance for each step
- Save results to `bench_results.csv`
- Perform union verification checks

### Benchmark Configuration

You can modify the benchmark parameters by editing `bench_runner.py`:

```python
PATIENTS_LIST = [250_000, 500_000, 1_000_000]  # Patient counts to test
TGDS_LIST = [100, 200, 400]                    # TGD rule counts to test
```

### Output Structure

The benchmark runner creates the following directory structure:
```
runs/
├── p250000/           # Patient size 250,000
│   ├── fs_base.db     # Base source database
│   ├── fo_base.db     # Base target database
│   ├── t100/          # 100 TGDs
│   │   ├── fs.db      # Working source database
│   │   ├── fo.db      # Working target database
│   │   ├── chase.db   # Chase result database
│   │   ├── fs_copy.db # Backup for verification
│   │   └── ...        # Intermediate files
│   └── t200/          # 200 TGDs
└── bench_results.csv  # Performance results
```

## Running Individual Scripts

If you want to run the pipeline step by step or customize specific components:

### Step 1: Create and Populate Database

```bash
python a1_0_create_fill_db.py
```

This creates three databases:
- `fs_records.db` - Source database with medical records
- `fo_records.db` - Empty target database
- `ChaseTable.db` - Database for chase operations

### Step 2: Generate TGDs and Security Constraints

```bash
python a2_1_sensitive_tgd_information.py
```

Generates:
- `rules.txt` - TGD rules
- `C.txt` - Security constraints (sensitive information markers)

### Step 3: Extract Sensitive Data

```bash
python a3_0_move_to_fo.py
```

Moves sensitive data from `fs_records.db` to `fo_records.db` based on constraints in `C.txt`.

### Step 4: Execute Core Chase

```bash
python a4_core_chase.py
```

Applies TGD rules to derive new tuples until a fixpoint is reached.

### Step 5: Build Dependency Graphs

```bash
python a5_graph.py
```

Constructs dependency graphs and saves them to `graphs.txt`.

### Step 6: Traverse Paths

```bash
python a6_0_traversal.py
```

Analyzes graph paths and saves results to `paths.txt`.

### Step 7: Compute Minimal Union

```bash
python a7_minimal_union.py
```

Calculates the minimal hitting set and saves to `union_greedy.txt`.

### Step 8: Final Fragmentation

```bash
python a8_fragmentation.py
```

Performs final data transfer and deletion based on the minimal union.

## Database Verification Tool

### Using check_same_tbl.py

The `check_same_tbl.py` utility provides database analysis and verification capabilities.

#### Count Rows in a Database

```bash
# Count all tables
python check_same_tbl.py count fs_records.db

# Count specific tables only
python check_same_tbl.py count fs_records.db --tables Patient,Illness,Medicine
```

#### Verify Union Property

Check if the union of two databases equals a third database (FS ∪ FO == FS_original):

```bash
# Basic union check
python check_same_tbl.py union fs_records.db fo_records.db fs_copy.db

# Union check with specific options
python check_same_tbl.py union fs_records.db fo_records.db fs_copy.db \
    --set \
    --tables Patient,Illness \
    --sample 10
```

#### Options for check_same_tbl.py

- `--set`: Use set equality instead of multiset (ignores duplicates)
- `--tables TableA,TableB`: Only check specific tables
- `--sample N`: Show up to N examples when differences are found
- `--no-color`: Disable colored output
- `--no-emoji`: Disable emoji indicators

#### Example Output

```
Union check (MULTISET): (DB1 ∪ DB2) ?= DB3
  DB1 = fs_records.db (fs_records.db)
  DB2 = fo_records.db (fo_records.db)
  DB3 = fs_copy.db (fs_copy.db)
------------------------------------------------------------------------------
Table                                 DB1         DB2    DB3    Result
------------------------------------------------------------------------------
Patient                              1000           0   1000    ✅ MATCH
Illness                               850         150   1000    ✅ MATCH
Medicine                              750         250   1000    ✅ MATCH
------------------------------------------------------------------------------
PASS — The union of DB1 and DB2 exactly equals DB3.
```

## Database Schema

The system works with a medical database containing the following tables:

- **Patient**: Name, Age, Gender
- **Illness**: PatientName, Diagnosis
- **Medicine**: PatientName, MedName
- **Allergy**: PatientName, AllergyName
- **Insurance**: PatientName, InsuranceName
- **LabResult**: PatientName, TestName
- **Appointment**: PatientName, AppointmentDate
- **Hospital**: PatientName, HospitalName
- **Treatment**: PatientName, TreatmentName

## Performance Monitoring

The benchmark runner tracks the following metrics for each step:

- `generate_tgds`: Time to generate TGD rules
- `extract_to_fo`: Time to extract sensitive data
- `core_chase`: Time to execute chase algorithm
- `build_graphs`: Time to construct dependency graphs
- `paths_union`: Time to traverse paths and compute union
- `transfer_delete`: Time for final fragmentation
- `total_runtime`: Total execution time

Results are saved in `bench_results.csv` with timestamps for analysis.

## Requirements

- Python 3.7+
- SQLite3
- Standard library modules: `sqlite3`, `random`, `csv`, `time`, `pathlib`, `argparse`


## File Dependencies

The scripts must be run in sequence or use the benchmark runner. Each step depends on outputs from previous steps:

```
a1 → creates databases
a2 → creates rules.txt, C.txt
a3 → uses C.txt, modifies databases
a4 → uses rules.txt, chase.db
a5 → uses rules.txt, C.txt, chase.db → creates graphs.txt
a6 → uses graphs.txt, databases → creates paths.txt
a7 → uses paths.txt → creates union_greedy.txt
a8 → uses union_greedy.txt, modifies databases
```

## Contact

This implementation is part of a master's thesis on database fragmentation using core chase algorithms. For questions or issues, please refer to the thesis documentation or contact the author.
