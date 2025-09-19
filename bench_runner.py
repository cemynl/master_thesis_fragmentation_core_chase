#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Benchmark orchestrator for a1..a8:
- Builds base DB once per patient size
- For each (patients, tgds) makes working copies
- Runs: a2 (TGDs)->a3 (extract)->a4 (core chase)->a5 (graphs)->a6 (paths)
        ->a7 (minimal union)->a8 (move/delete)
- Tracks per-step runtimes in bench_results.csv (breites Format, eine Zeile pro Run)
- After fragmentation: performs union check (fs ∪ fo == fs_copy)
"""

import os
import csv
import time
import shutil
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# ---- your modules ----
import a1_0_create_fill_db as a1
import a2_1_sensitive_tgd_information as a2
import a3_0_move_to_fo as a3
import a4_core_chase as a4
import a5_graph as a5
import a6_0_traversal as a6
import a7_minimal_union as a7
import a8_fragmentation as a8
import check_same_tbl as chk   # <-- Union-Check
import sqlite3
import random


PATIENTS_LIST = [10_000,25_000,50_000,100_000,250_000,500_000,1_000_000]
TGDS_LIST     = [100, 200, 400]
MAX_ITER_CHASE = 100

ROOT = Path("runs")
RESULTS_CSV = Path("bench_results.csv")

# ---------- helpers ----------
def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

def timeit():
    start = time.perf_counter()
    def end():
        return time.perf_counter() - start
    return end

# ---------- base DB per patients ----------
def build_base_db(patients: int) -> Tuple[Path, Path]:
    base_dir = ensure_dir(ROOT / f"p{patients}")
    fs_base = base_dir / "fs_base.db"
    fo_base = base_dir / "fo_base.db"

    if fs_base.exists() and fo_base.exists():
        print(f"🟡 Base DBs exist for patients={patients} → reuse")
        return fs_base, fo_base

    print(f"🟢 Create base DBs for patients={patients}")
    conn = sqlite3.connect(str(fs_base))
    a1.create_schema(conn)
    a1.populate(conn, num_patients=patients)
    conn.close()

    conn = sqlite3.connect(str(fo_base))
    a1.create_schema(conn)
    conn.close()

    return fs_base, fo_base

# ---------- per-run working set ----------
def make_working_set(patients: int, tgds: int, fs_base: Path, fo_base: Path) -> Dict[str, Path]:
    run_dir = ensure_dir(ROOT / f"p{patients}" / f"t{tgds}")
    fs = run_dir / "fs.db"
    fo = run_dir / "fo.db"
    chase = run_dir / "chase.db"
    fs_copy = run_dir / "fs_copy.db"   # Kopie für späteren Union-Check
    rules = run_dir / f"rules_{tgds}.txt"
    cfile = run_dir / f"C_{tgds}.txt"
    graphs = run_dir / "graphs.txt"
    paths = run_dir / "paths.txt"
    hit = run_dir / "greedy_union.txt"

    shutil.copyfile(fs_base, fs)   # FS initial
    shutil.copyfile(fo_base, fo)   # FO leer
    shutil.copyfile(fs, fs_copy)   # Backup von FS vor Extract (für Union-Check)

    return {
        "dir": run_dir, "fs": fs, "fo": fo, "chase": chase, "fs_copy": fs_copy,
        "rules": rules, "c": cfile, "graphs": graphs, "paths": paths, "hit": hit
    }

# ---------- steps ----------
def step_generate_tgds(patients: int, tgds: int, paths: Dict[str, Path]) -> str:
    seed = 124 + patients + tgds
    tgds_list, heads = a2.generate_tgds(num_rules=tgds, seed=seed)
    paths["rules"].write_text("\n".join(tgds_list) + "\n", encoding="utf-8")
    heads_sorted = sorted(heads)
    random.seed(seed)
    k = max(1, tgds // 10)
    selection = random.sample(heads_sorted, min(k, len(heads_sorted)))
    with paths["c"].open("w", encoding="utf-8") as f:
        for rel, const in selection:
            f.write(f"{rel}['{const}']\n")
    return f"C={len(selection)}"

def step_extract_to_fo(patients: int, tgds: int, paths: Dict[str, Path]) -> str:
    extractor = a3.SecurityExtractor(str(paths["fs"]), str(paths["fo"]), str(paths["c"]))
    try:
        moved = extractor.extract()
    finally:
        extractor.close()

    # Jetzt den extrahierten Stand von FS nach Chase kopieren
    shutil.copyfile(paths["fs"], paths["chase"])

    return f"moved={moved}"

def step_core_chase(patients: int, tgds: int, paths: Dict[str, Path]) -> str:
    # Lade die TGDs aus rules.txt
    rules = a4.load_lines(str(paths["rules"]))
    conn = sqlite3.connect(str(paths["chase"]))
    try:
        sim = a4.TGDDslSimulator(conn, rules)
        sim.chase(max_iter=MAX_ITER_CHASE)
    finally:
        conn.close()
    return "chase_completed"

def step_build_graphs(patients: int, tgds: int, paths: Dict[str, Path]) -> str:
    extr = a5.RootsTGDSubgraphExtractor(
        chase_db=str(paths["chase"]),
        rules_file=str(paths["rules"]),
        roots_file=str(paths["c"])
    )
    extr.save(out_file=str(paths["graphs"]))
    extr.conn.close()
    txt = paths["graphs"].read_text(encoding="utf-8")
    return f"graphs={txt.count('graph = {')}"

def step_paths_and_union(patients: int, tgds: int, paths: Dict[str, Path]) -> str:
    graphs = a6.GraphTraversal.load_graphs(str(paths["graphs"]))
    C, I = a6.GraphTraversal.load_C_I(str(paths["chase"]), str(paths["fs"]))
    grouped_paths: List[List[List[str]]] = []
    for g in graphs:
        grouped_paths.append(a6.GraphTraversal.traverse_graph(g, C, I))
    paths["paths"].write_text(repr(grouped_paths), encoding="utf-8")
    greedy = a7.PathCombinator.generate_greedy_union(grouped_paths)
    paths["hit"].write_text(repr(greedy), encoding="utf-8")
    return f"groups={len(grouped_paths)};HS={len(greedy)}"

def step_transfer_delete(patients: int, tgds: int, paths: Dict[str, Path]) -> str:
    mover = a8.TransferAndDelete(
        main_db=str(paths["fs"]),
        fo_db=str(paths["fo"]),
        hs_file=str(paths["hit"])
    )
    deleted = mover.process()   # <-- nutzt jetzt den Rückgabewert
    return f"deleted={deleted}"
# ---------- main ----------
def main():
    ensure_dir(ROOT)

    for patients in PATIENTS_LIST:
        fs_base, fo_base = build_base_db(patients)

        for tgds in TGDS_LIST:
            paths = make_working_set(patients, tgds, fs_base, fo_base)

            timings = {}
            for step_name, func in [
                ("generate_tgds", step_generate_tgds),
                ("extract_to_fo", step_extract_to_fo),
                ("core_chase", step_core_chase),
                ("build_graphs", step_build_graphs),
                ("paths_union", step_paths_and_union),
                ("transfer_delete", step_transfer_delete),
            ]:
                t = timeit()
                try:
                    result = func(patients, tgds, paths)
                    runtime = t()
                    timings[step_name] = runtime
                    print(f"✅ {step_name} p={patients}, t={tgds} → {result} (runtime {runtime:.2f}s)")
                except Exception as e:
                    timings[step_name] = None
                    print(f"❌ ERROR in {step_name} (p={patients}, t={tgds}) → {e}\n{traceback.format_exc()}")

                # Pause nach jedem Schritt
                #input(f"⏸ Schritt '{step_name}' abgeschlossen. Weiter mit [Enter]...")

            # Gesamtzeit
            total_runtime = sum(v for v in timings.values() if v is not None)
            timings["total_runtime"] = total_runtime

            # CSV schreiben
            row = {
                "patients": patients,
                "tgds": tgds,
                **timings,
                "ts": now_iso()
            }
            write_header = not RESULTS_CSV.exists()
            with RESULTS_CSV.open("a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=row.keys(), quoting=csv.QUOTE_ALL)
                if write_header:
                    w.writeheader()
                w.writerow(row)

            # Union-Check
            try:
                styles = chk.make_styles(enable_color=True, enable_emoji=True)
                print(f"🔎 Union-Check gestartet für p={patients}, t={tgds}")
                chk.cmd_union(str(paths["fs"]), str(paths["fo"]), str(paths["fs_copy"]),
                              as_set=False, only_tables=None, sample=3, styles=styles)
            except Exception as e:
                print(f"❌ Union-Check Fehler: {e}")

if __name__ == "__main__":
    main()
