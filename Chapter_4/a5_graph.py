#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import re
from typing import List, Tuple, Dict, Set
from itertools import product
import sys
import logging
import os
import traceback

# --- Configure logging ---
logging.basicConfig(
    level=logging.INFO,  # use DEBUG for even more detail
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    force=True
)

# Increase recursion depth
sys.setrecursionlimit(1_000_000)
MAX_DEPTH = 5_000_000  # safeguard against blowup


class RootsTGDSubgraphExtractor:
    """
    Reads roots in the format Table['Const'] from C.txt and applies TGDs recursively,
    but only if the tuples actually exist in the DB.
    """

    atom_re = re.compile(r"^(?P<table>\w+)\(n,\s*'(?P<const>[^']+)'\)$")
    root_re = re.compile(r"^(?P<table>\w+)\['(?P<const>[^']+)'\]$")

    def __init__(self,
                 chase_db: str = 'ChaseTable.db',
                 rules_file: str = 'rules.txt',
                 roots_file: str = 'C.txt'):
        self.conn = sqlite3.connect(chase_db)
        self.cur = self.conn.cursor()
        self.head_map = self._load_and_index_rules(rules_file)
        self.roots = self._load_roots(roots_file)
        logging.info(f"Loaded {len(self.head_map)} distinct heads")
        logging.info(f"Loaded {len(self.roots)} roots: {self.roots}")

    # ----------------------------------------------------------
    def _load_and_index_rules(self, path: str) -> Dict[Tuple[str, str], List[List[Tuple[str, str]]]]:
        """
        Reads TGDs from rules.txt and indexes them by head.
        """
        head_map: Dict[Tuple[str, str], List[List[Tuple[str, str]]]] = {}
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f, 1):
                text = line.strip()
                if not text or '->' not in text:
                    continue
                body_s, head_s = [p.strip() for p in text.split('->', 1)]
                m2 = self.atom_re.match(head_s)
                if not m2:
                    logging.warning(f"Line {i}: head not recognized: {head_s}")
                    continue
                head = (m2.group('table'), m2.group('const'))
                parts = re.split(r'∧|AND', body_s)
                body: List[Tuple[str, str]] = []
                for atom in parts:
                    atom_clean = atom.strip()
                    if not atom_clean:
                        continue
                    m = self.atom_re.match(atom_clean)
                    if m:
                        body.append((m.group('table'), m.group('const')))
                    else:
                        logging.warning(f"Line {i}: body atom not recognized: {atom_clean}")
                if body:
                    head_map.setdefault(head, []).append(body)
        return head_map

    # ----------------------------------------------------------
    def _load_roots(self, path: str) -> List[Tuple[str, str]]:
        roots: List[Tuple[str, str]] = []
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                m = self.root_re.match(line)
                if m:
                    roots.append((m.group('table'), m.group('const')))
                else:
                    logging.warning(f"Root not recognized (line {i}): {line}")
        return roots

    # ----------------------------------------------------------
    def _get_pk_const(self, table: str) -> Tuple[str, str]:
        """
        Returns the primary key column and the column holding the constant value.
        """
        self.cur.execute(f"PRAGMA table_info({table});")
        info = self.cur.fetchall()
        if not info:
            logging.error(f"Table {table} does not exist in DB")
            return "id", "value"
        pk = next((r[1] for r in info if r[5] > 0), info[0][1])
        const_col = next((r[1] for r in info if r[1] != pk), pk)
        return pk, const_col

    # ----------------------------------------------------------
    def _expand_full(self, start_node: str) -> List[Dict[str, List[str]]]:
        """
        Starts backward expansion from a start node.
        Rules are applied only if all body atoms with the same n exist in the DB.
        """

        def recurse(node: str, depth=0, visited=None) -> List[Dict[str, Set[str]]]:
            if visited is None:
                visited = set()
            if node in visited:
                logging.debug(f"[DEPTH={depth}] Cycle detected at {node}, aborting this branch")
                return [{node: set()}]
            if depth > MAX_DEPTH:
                logging.warning(f"[DEPTH={depth}] Maximum depth reached at {node}, aborting this branch")
                return [{node: set()}]

            visited.add(node)

            try:
                parts = node.split(':', 2)
                if len(parts) != 3:
                    logging.error(f"[DEPTH={depth}] Invalid node: {node}")
                    return []
                tbl, key, const = parts
                bodies = self.head_map.get((tbl, const), [])

                if not bodies:
                    return [{node: set()}]

                all_graphs: List[Dict[str, Set[str]]] = []

                # Try all rules for this head
                for body in bodies:
                    children = []
                    ok = True
                    # Validate each body atom against the DB
                    for bt, bconst in body:
                        pk, constcol = self._get_pk_const(bt)
                        self.cur.execute(
                            f"SELECT 1 FROM {bt} WHERE {pk}=? AND {constcol}=?",
                            (key, bconst)
                        )
                        if self.cur.fetchone():
                            children.append(f"{bt}:{key}:{bconst}")
                        else:
                            ok = False
                            logging.debug(f"[DEPTH={depth}] Rule invalid: {bt}({key},{bconst}) missing in DB")
                            break  # the entire rule cannot fire

                    if not ok:
                        continue  # skip this rule

                    # If the rule is valid, expand children
                    child_lists: List[List[Dict[str, Set[str]]]] = []
                    for child in children:
                        child_lists.append(recurse(child, depth+1, visited.copy()))

                    for combo in product(*child_lists):
                        merged: Dict[str, Set[str]] = {}
                        for g in combo:
                            for n, vs in g.items():
                                merged.setdefault(n, set()).update(vs)
                        merged.setdefault(node, set())
                        for child in children:
                            merged.setdefault(child, set()).add(node)
                        all_graphs.append(merged)

                return all_graphs if all_graphs else [{node: set()}]

            except RecursionError:
                logging.error(f"[DEPTH={depth}] RecursionError at {node}")
                return []
            except Exception as e:
                logging.error(f"[DEPTH={depth}] Error at node {node}: {e}")
                logging.error(traceback.format_exc())
                return []

        raw_graphs = recurse(start_node)

        result: List[Dict[str, List[str]]] = []
        for g in raw_graphs:
            conv: Dict[str, List[str]] = {n: sorted(vs) for n, vs in g.items()}
            result.append(conv)
        return result

    # ----------------------------------------------------------
    def save(self, out_file: str = 'graphs.txt') -> None:
        total_written = 0
        with open(out_file, 'w', encoding='utf-8') as f:
            for idx, (tbl, const) in enumerate(self.roots, 1):
                pk, constcol = self._get_pk_const(tbl)
                self.cur.execute(f"SELECT {pk} FROM {tbl} WHERE {constcol} = ?", (const,))
                rows = self.cur.fetchall()

                root_written = 0
                #logging.info(f"[{idx}/{len(self.roots)}] Root {tbl}:{const} → {len(rows)} DB matches")
                #logging.info(f"Head ({tbl},{const}) has {len(self.head_map.get((tbl,const), []))} rules")

                for row_idx, (key,) in enumerate(rows, 1):
                    root = f"{tbl}:{key}:{const}"
                    logging.debug(f"  -> Expand {root} ({row_idx}/{len(rows)})")

                    try:
                        subs = self._expand_full(root)
                    except RecursionError:
                        logging.error(f"RecursionError at {root} – expansion too deep")
                        continue
                    except Exception as e:
                        logging.error(f"Error during expansion {root}: {e}")
                        logging.error(traceback.format_exc())
                        continue

                    if not subs:
                        logging.info(f"  {root} → no subgraphs expanded")

                    for sg in subs:
                        f.write('graph = {\n')
                        for n, vs in sg.items():
                            entries = ', '.join(f"'{x}'" for x in vs)
                            f.write(f"  '{n}': [{entries}],\n")
                        f.write('}\n\n')
                        f.flush()
                        os.fsync(f.fileno())

                        root_written += 1
                        total_written += 1

                logging.info(f"Root {tbl}:{const} → wrote {root_written} graphs")

        logging.info(f"DONE: wrote {total_written} graphs to '{out_file}'")


# ----------------------------------------------------------
# Main
# ----------------------------------------------------------
if __name__ == '__main__':
    extractor = RootsTGDSubgraphExtractor()
    extractor.save()
    extractor.conn.close()
