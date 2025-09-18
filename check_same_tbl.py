#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DB Reporter: counts and union checks with clean, screenshot-ready output.

Examples:
  # 1) Count rows per table in one DB
  python db_report.py count fs.db
  python db_report.py count fs.db --tables Patient,Illness

  # 2) Check whether DB1 ∪ DB2 == DB3 (table by table)
  python db_report.py union fs.db fo.db initial.db
  python db_report.py union fs.db fo.db initial.db --set --tables Patient,Illness --sample 10

Options:
  --no-color / --no-emoji  for neutral, plain text output
"""

import argparse
import sqlite3
from collections import Counter
from typing import List, Tuple

# ---------- Pretty helpers ----------

def make_styles(enable_color: bool, enable_emoji: bool):
    class S:
        OK  = "✅ " if enable_emoji else "[OK] "
        BAD = "❌ " if enable_emoji else "[X] "
        SEP = "-" * 78

        GREEN = "\033[92m" if enable_color else ""
        RED   = "\033[91m" if enable_color else ""
        CYAN  = "\033[96m" if enable_color else ""
        BOLD  = "\033[1m"  if enable_color else ""
        DIM   = "\033[2m"  if enable_color else ""
        RST   = "\033[0m"  if enable_color else ""

        @staticmethod
        def g(txt): return S.GREEN + txt + S.RST
        @staticmethod
        def r(txt): return S.RED   + txt + S.RST
        @staticmethod
        def c(txt): return S.CYAN  + txt + S.RST
        @staticmethod
        def b(txt): return S.BOLD  + txt + S.RST
        @staticmethod
        def d(txt): return S.DIM   + txt + S.RST
    return S

def tabline(cols: List[Tuple[str,int]]):
    # cols: list of (text, width); right align for numbers, left for text
    out = []
    for txt, w in cols:
        if txt.replace(",", "").replace("_","").isdigit():
            out.append(f"{txt:>{w}}")
        else:
            out.append(f"{txt:<{w}}")
    return "  ".join(out)

# ---------- SQLite helpers ----------

def list_tables(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return [r[0] for r in cur.fetchall()]

def table_columns(conn, table):
    try:
        cur = conn.execute(f'PRAGMA table_info("{table}")')
        rows = cur.fetchall()
        if not rows: return None
        return [r[1] for r in rows]  # physical order
    except sqlite3.OperationalError:
        return None

def count_rows(conn, table):
    try:
        cur = conn.execute(f'SELECT COUNT(*) FROM "{table}"')
        return cur.fetchone()[0]
    except sqlite3.OperationalError:
        return None

def fetch_rows(conn, table, cols):
    if cols is None: return []
    col_list = ", ".join([f'"{c}"' for c in cols])
    try:
        cur = conn.execute(f'SELECT {col_list} FROM "{table}"')
        return [tuple(row) for row in cur.fetchall()]
    except sqlite3.OperationalError:
        return []

# ---------- Commands ----------

def cmd_count(db_path, only_tables, styles):
    S = styles
    con = sqlite3.connect(db_path)
    try:
        tables = list_tables(con)
        if only_tables:
            want = {t.strip() for t in only_tables.split(",") if t.strip()}
            tables = [t for t in tables if t in want]
        tables = sorted(tables)
        if not tables:
            print("No user tables found.")
            return

        # header
        print(S.b(f"Row count per table — {db_path}"))
        print(S.SEP)
        header = [("Table", 28), ("Rows", 12)]
        print(tabline(header))
        print(S.SEP)

        total = 0
        for t in tables:
            n = count_rows(con, t)
            n_str = str(n) if n is not None else "n/a"
            print(tabline([(t, 28), (n_str, 12)]))
            total += (n or 0)
        print(S.SEP)
        print(tabline([("TOTAL", 28), (str(total), 12)]))
        print()
    finally:
        con.close()

def cmd_union(db1, db2, db3, as_set, only_tables, sample, styles):
    import os
    S = styles
    con1 = sqlite3.connect(db1)
    con2 = sqlite3.connect(db2)
    con3 = sqlite3.connect(db3)
    try:
        t1, t2, t3 = set(list_tables(con1)), set(list_tables(con2)), set(list_tables(con3))
        if only_tables:
            want = {t.strip() for t in only_tables.split(",") if t.strip()}
            targets = sorted(want)
        else:
            targets = sorted(t1 | t2 | t3)

        mode = "SET" if as_set else "MULTISET"

        # Titel
        print(S.b(f"Union check ({mode}): (DB1 ∪ DB2) ?= DB3"))
        print(f"  DB1 = {db1} ({os.path.basename(db1)})")
        print(f"  DB2 = {db2} ({os.path.basename(db2)})")
        print(f"  DB3 = {db3} ({os.path.basename(db3)})")
        print(S.SEP)

        # Kopfzeilen: eine Zeile
        print(tabline([
            ("Table", 36),
            ("DB1", 12),
            ("DB2", 12),
            ("DB3", 4),
            ("Result", 0)
        ]))
        print(S.SEP)

        ok_all = True
        total_db1 = total_db2 = total_db3 = 0
        total_left = total_right = 0

        for table in targets:
            # Referenzspalten wählen (bevorzugt DB3)
            cols = None
            if table in t3: cols = table_columns(con3, table)
            if cols is None and table in t1: cols = table_columns(con1, table)
            if cols is None and table in t2: cols = table_columns(con2, table)

            if cols is None:
                ok_all = False
                print(tabline([(table, 28), ("-",12), ("-",12), ("-",12), (S.r("NO SCHEMA"),14)]))
                continue

            # Zeilen lesen
            r1 = fetch_rows(con1, table, cols) if table in t1 else []
            r2 = fetch_rows(con2, table, cols) if table in t2 else []
            r3 = fetch_rows(con3, table, cols) if table in t3 else []

            # Summen für die Anzeige
            c1, c2, c3 = len(r1), len(r2), len(r3)
            total_db1 += c1
            total_db2 += c2
            total_db3 += c3

            # Vergleich links vs rechts
            if as_set:
                left_counter  = Counter(set(r1) | set(r2))
                right_counter = Counter(set(r3))
                left_count = len(left_counter)
                right_count = len(right_counter)
            else:
                left_counter  = Counter(r1) + Counter(r2)
                right_counter = Counter(r3)
                left_count = sum(left_counter.values())
                right_count = sum(right_counter.values())

            total_left  += left_count
            total_right += right_count

            missing = left_counter - right_counter
            extra   = right_counter - left_counter

            if not missing and not extra:
                res = S.g("MATCH")
                print(tabline([(table, 28), (str(c1),12), (str(c2),12), (str(c3),12), (res,14)]))
            else:
                ok_all = False
                res = S.r(S.BAD + "DIFF")
                print(tabline([(table, 28), (str(c1),12), (str(c2),12), (str(c3),12), (res,14)]))
                if missing:
                    print("   → missing in DB3 (up to {}):".format(sample))
                    for i, (row, cnt) in enumerate(missing.items()):
                        if i >= sample: break
                        print(f"      {row} ×{cnt}")
                if extra:
                    print("   → extra in DB3 (up to {}):".format(sample))
                    for i, (row, cnt) in enumerate(extra.items()):
                        if i >= sample: break
                        print(f"      {row} ×{cnt}")

        # Footer mit Totals
        print(S.SEP)
        print(tabline([("TOTAL DB1", 28), (str(total_db1), 12)]))
        print(tabline([("TOTAL DB2", 28), (str(total_db2), 12)]))
        print(tabline([("TOTAL DB3", 28), (str(total_db3), 12)]))
        print(S.SEP)
        print(tabline([("TOTAL LEFT (DB1 ∪ DB2)", 28), (str(total_left), 12)]))
        print(tabline([("TOTAL RIGHT (DB3)",       28), (str(total_right), 12)]))
        print(S.SEP)

        if ok_all and total_left == total_right:
            print(S.b("PASS — The union of DB1 and DB2 exactly equals DB3."))
        else:
            print(S.r(S.BAD) + S.b("FAIL — Differences detected."))

        print()
    finally:
        con1.close(); con2.close(); con3.close()

# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="SQLite DB reporter: counts and union checks")
    ap.add_argument("--no-color", action="store_true", help="disable ANSI colors")
    ap.add_argument("--no-emoji", action="store_true", help="disable emojis")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_count = sub.add_parser("count", help="Row counts per table")
    p_count.add_argument("db", help="SQLite DB path")
    p_count.add_argument("--tables", help="comma-separated subset of tables")

    p_union = sub.add_parser("union", help="Check if DB1 ∪ DB2 equals DB3")
    p_union.add_argument("db1", help="DB1 path")
    p_union.add_argument("db2", help="DB2 path")
    p_union.add_argument("db3", help="DB3 path")
    p_union.add_argument("--set", action="store_true", help="use set equality instead of multiset")
    p_union.add_argument("--tables", help="comma-separated subset of tables")
    p_union.add_argument("--sample", type=int, default=5, help="examples shown when differences")

    args = ap.parse_args()
    styles = make_styles(enable_color=not args.no_color, enable_emoji=not args.no_emoji)

    if args.cmd == "count":
        cmd_count(args.db, args.tables, styles)
    elif args.cmd == "union":
        cmd_union(args.db1, args.db2, args.db3, args.set, args.tables, args.sample, styles)

if __name__ == "__main__":
    main()
