#!/usr/bin/env python3
"""
Seed a minimal demo dataset into the FS (server) SQLite database:
- Two patients: Lukas and Klara
- Lukas: HIV_Positive + Tuberculosis (+ Hypertension)
- Klara: Influenza; Treatments MedA and MedB to trigger a 3-step chain
"""

import sqlite3
from contextlib import contextmanager

FS_DB_PATH = "fs_records.db"  # <-- adjust to your FS/server DB path

@contextmanager
def fast_conn(path: str):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    # performance pragmas for bulk inserts (adjust if you prefer stricter durability)
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=OFF;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-200000;")
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()

def seed_two_patients_tb(fs_db: str = FS_DB_PATH):

    patients = [
        ("Lukas", 34, "M"),
        ("Klara", 27, "F"),
    ]

    illnesses = [
        ("Lukas", "HIV_Positive"),
        ("Lukas", "Aids"),
        ("Lukas", "Tuberculosis"),
        ("Klara", "Influenza"),
    ]


    treatments = [
        ("Klara", "TreatA"),
        ("Lukas", "TreatA"),
        ("Lukas", "TreatB"),
        ("Lukas", "TreatY"),
        ("Klara", "TreatY"),

    ]

    medicines = [
        ("Klara",          "MedC")
    ]

    with fast_conn(fs_db) as conn:
        cur = conn.cursor()

        # Insert data idempotently (avoids duplicates on re-run)
        cur.executemany(
            "INSERT OR IGNORE INTO Patient (Name, Age, Gender) VALUES (?, ?, ?)",
            patients
        )
        cur.executemany(
            "INSERT OR IGNORE INTO Illness (PatientName, Diagnosis) VALUES (?, ?)",
            illnesses
        )
        cur.executemany(
            "INSERT OR IGNORE INTO Treatment (PatientName, TreatmentName) VALUES (?, ?)",
            treatments
        )
        cur.executemany(
            "INSERT OR IGNORE INTO Medicine (PatientName, MedName) VALUES (?, ?)",
            medicines
        )

        #quick sanity check
        for tbl in ("Patient", "Illness", "Treatment", "Doctor", "Medicine"):
            try:
                cnt = cur.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                print(f"{tbl}: {cnt} rows")
            except sqlite3.OperationalError as e:
                print(f"Warning: table {tbl} not found ({e}).")

if __name__ == "__main__":
    seed_two_patients_tb()
