#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import re
from typing import List, Dict, Tuple, Set

class GraphTraversal:
    """
    Reads graphs from 'graphs.txt', derives C and I dynamically from databases,
    and applies the given traversal algorithm to each component.
    """
    node_re = re.compile(r"^\s*'(?P<node>[^']+)': \[(?P<neigh>.*)\],?$")

    @staticmethod
    def load_graphs(filepath: str) -> List[Dict[str, List[str]]]:
        graphs: List[Dict[str, List[str]]] = []
        current: Dict[str, List[str]] = {}
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('graph'):
                    current = {}
                    continue
                if line == '}' or line == '};':
                    if current:
                        graphs.append(current)
                    continue
                m = GraphTraversal.node_re.match(line)
                if m:
                    node = m.group('node')
                    neighs = m.group('neigh')
                    if neighs.strip():
                        entries = [n.strip().strip("'") for n in neighs.split(',') if n.strip()]
                    else:
                        entries = []
                    current[node] = entries
        return graphs

    @staticmethod
    def _extract_nodes(conn: sqlite3.Connection) -> Set[str]:
        """Reads all tables and builds nodes in the form 'Table:pk:const' from (pk, text column)."""
        cur = conn.cursor()
        nodes: Set[str] = set()

        # Get table names
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cur.fetchall()]

        for table in tables:
            cur.execute(f"PRAGMA table_info({table})")
            cols = cur.fetchall()
            if not cols:
                continue

            # Determine primary key column
            pk_col = next((c[1] for c in cols if c[5] > 0), cols[0][1])

            # Candidate text columns
            text_cols = [c[1] for c in cols if "CHAR" in c[2].upper() or "TEXT" in c[2].upper()]
            if not text_cols:
                continue

            # Read all rows
            cur.execute(f"SELECT {pk_col}, {', '.join(text_cols)} FROM {table}")
            for row in cur.fetchall():
                pk_val = row[0]
                for val in row[1:]:
                    if val is not None:
                        nodes.add(f"{table}:{pk_val}:{val}")
        return nodes

    @staticmethod
    def load_C_I(chase_db: str, fs_db: str) -> Tuple[Set[str], Set[str]]:
        conn_c = sqlite3.connect(chase_db)
        C = GraphTraversal._extract_nodes(conn_c)
        conn_c.close()

        conn_i = sqlite3.connect(fs_db)
        I = GraphTraversal._extract_nodes(conn_i)
        conn_i.close()

        return C, I

    @staticmethod
    def traverse_graph(graph: Dict[str, List[str]], C: Set[str], I: Set[str]) -> List[List[str]]:
        all_paths: List[List[str]] = []

        def traverse(node: str, path_I: List[str]) -> bool:
            if node not in I and node not in C:
                return False
            if node in I:
                path_I = path_I + [node]
            any_printed = False
            for child in graph.get(node, []):
                if traverse(child, path_I):
                    any_printed = True
            if node in I and not any_printed:
                if path_I:  # only record non-empty paths
                    all_paths.append(path_I)
                return True
            return any_printed

        # Roots: nodes without incoming edges
        incoming = {n: [] for n in graph}
        for u, targets in graph.items():
            for v in targets:
                incoming.setdefault(v, []).append(u)
        roots = [n for n, ins in incoming.items() if not ins]

        for root in roots:
            traverse(root, [])

        # Remove exact duplicate paths
        unique_paths = []
        seen = set()
        for p in all_paths:
            tup = tuple(p)
            if tup not in seen:
                seen.add(tup)
                unique_paths.append(p)

        return unique_paths


if __name__ == '__main__':
    # Load graph components
    graphs = GraphTraversal.load_graphs('graphs.txt')
    # Load C and I (now sets)
    C, I = GraphTraversal.load_C_I('ChaseTable.db', 'fs_records.db')
    # Traverse each component
    grouped_paths: List[List[List[str]]] = []
    for idx, g in enumerate(graphs, start=1):
        paths = GraphTraversal.traverse_graph(g, C, I)
        if paths:  # add only if non-empty
            grouped_paths.append(paths)

    # Remove exact duplicate groups
    unique_grouped_paths: List[List[List[str]]] = []
    seen_groups = set()
    for group in grouped_paths:
        tup = tuple(tuple(p) for p in group)  # represent each group as a tuple of tuples
        if tup not in seen_groups:
            seen_groups.add(tup)
            unique_grouped_paths.append(group)

    # Save results
    with open('paths.txt', 'w', encoding='utf-8') as f:
        f.write(repr(unique_grouped_paths))
    print("All unique path groups per graph have been saved to 'paths.txt'.")
