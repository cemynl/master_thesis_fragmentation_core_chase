import sqlite3
import re
from typing import List, Tuple, Dict, Any, Optional

class SecurityExtractor:
    """
    Verschiebt Datensätze aus der Main-DB in die FO-DB,
    wenn deren (Nicht-PK-)Spalten exakt eine in C.txt genannte Konstante enthalten.
    Erwartetes C.txt-Format:   Table['Const']
    """

    root_re = re.compile(r"^(?P<table>\w+)\['(?P<const>[^']+)'\]$")

    def __init__(self,
                 main_db_path: str,
                 fo_db_path: str,
                 roots_file: str = 'C.txt'):
        # Zwei getrennte Verbindungen
        self.main_conn = sqlite3.connect(main_db_path, isolation_level=None)
        self.fo_conn   = sqlite3.connect(fo_db_path,   isolation_level=None)

        # Performance-PRAGMAs
        for conn in (self.main_conn, self.fo_conn):
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=OFF;")
            cur.execute("PRAGMA temp_store=MEMORY;")
            cur.execute("PRAGMA cache_size=-800000;")

        self.main_cur = self.main_conn.cursor()
        self.fo_cur   = self.fo_conn.cursor()
        self.roots_file = roots_file

    # ---------- Hilfsfunktionen ----------

    def list_tables(self) -> List[str]:
        self.main_cur.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """)
        return [r[0] for r in self.main_cur.fetchall()]

    def load_roots(self) -> List[Tuple[str, str]]:
        roots: List[Tuple[str, str]] = []
        with open(self.roots_file, 'r', encoding='utf-8', errors='ignore') as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith('#'):
                    continue
                m = self.root_re.match(line)
                if m:
                    roots.append((m.group('table'), m.group('const')))
        return roots

    def _table_sql(self, table: str) -> str:
        self.main_cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
        row = self.main_cur.fetchone()
        return row[0] if row and row[0] else ""

    def _is_without_rowid(self, table: str) -> bool:
        sql = self._table_sql(table).lower()
        return "without rowid" in sql

    def _get_schema(self, table: str) -> Tuple[List[Tuple[str, str, int]], List[str], List[str]]:
        """
        Liefert:
          - schema_rows: Liste (name, type, pkflag)
          - cols:        alle Spaltennamen in Tabellen-Reihenfolge
          - pk_cols:     Primärschlüsselspalten (leere Liste möglich)
        """
        self.main_cur.execute(f"PRAGMA table_info({table});")
        info = self.main_cur.fetchall()
        if not info:
            raise ValueError(f"Schema leer oder Tabelle nicht vorhanden: {table}")
        schema_rows = [(row[1], row[2], int(row[5])) for row in info]  # name, type, pkflag
        cols    = [row[0] for row in schema_rows]
        pk_cols = [row[0] for row in schema_rows if row[2] > 0]
        return schema_rows, cols, pk_cols

    def _ensure_index(self, table: str, col: str):
        idx_name = f"idx_{table}_{col}"
        self.main_cur.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({col});")

    def _ensure_fo_table(self, table: str, schema_rows: List[Tuple[str, str, int]]):
        """Legt in der FO-DB die Tabelle mit gleichen Spalten (Namen & Typen) an."""
        cols_def = ", ".join(f"{name} {typ or 'TEXT'}" for (name, typ, _pk) in schema_rows)
        self.fo_cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols_def});")

    # ---------- Kern: verschieben ----------

    def _move_one(self, table: str, const: str) -> int:
        """
        Verschiebt alle Zeilen aus 'table' (Main-DB) in die FO-DB,
        deren irgendeine Nicht-PK-Spalte == const ist. Jede Zeile wird nur 1× verschoben.
        Rückgabe: Anzahl verschobener Zeilen.
        """
        schema_rows, cols, pk_cols = self._get_schema(table)
        self._ensure_fo_table(table, schema_rows)

        # Kandidatenspalten: alle Nicht-PK-Spalten (falls keine PK: dann alle Spalten)
        non_pk_cols = [name for (name, _typ, pk) in schema_rows if pk == 0]
        candidate_cols = non_pk_cols if non_pk_cols else cols

        # Indexe auf den Kandidatenspalten (Quelle)
        for c in candidate_cols:
            self._ensure_index(table, c)

        # WHERE-Klausel: (c1=? OR c2=? OR ...)
        where_or = " OR ".join([f"{c} = ?" for c in candidate_cols])
        params = tuple([const] * len(candidate_cols))

        # 1) Kandidaten einmalig identifizieren
        rows_to_move = []
        rowids = []
        without_rowid = self._is_without_rowid(table)

        if not without_rowid:
            self.main_cur.execute(f"SELECT rowid, {', '.join(cols)} FROM {table} WHERE {where_or}", params)
            fetched = self.main_cur.fetchall()
            if not fetched:
                return 0
            rowids = [r[0] for r in fetched]
            rows_to_move = [tuple(r[1:]) for r in fetched]
        else:
            if not pk_cols:
                self.main_cur.execute(f"SELECT {', '.join(cols)} FROM {table} WHERE {where_or}", params)
                rows_to_move = self.main_cur.fetchall()
                if not rows_to_move:
                    return 0
            else:
                self.main_cur.execute(f"SELECT {', '.join(pk_cols)} FROM {table} WHERE {where_or}", params)
                pks = list(dict.fromkeys(self.main_cur.fetchall()))
                if not pks:
                    return 0
                where_pk = " AND ".join([f"{k} = ?" for k in pk_cols])
                for chunk_start in range(0, len(pks), 500):
                    chunk = pks[chunk_start:chunk_start+500]
                    for pk_vals in chunk:
                        self.main_cur.execute(
                            f"SELECT {', '.join(cols)} FROM {table} WHERE {where_pk}",
                            tuple(pk_vals) if isinstance(pk_vals, tuple) else (pk_vals,)
                        )
                        r = self.main_cur.fetchone()
                        if r:
                            rows_to_move.append(r)

        if not rows_to_move:
            return 0

        # 2) In FO-DB einfügen
        placeholders = ",".join(["?"] * len(cols))
        insert_sql = f"INSERT OR IGNORE INTO {table}({', '.join(cols)}) VALUES ({placeholders})"
        self.fo_cur.executemany(insert_sql, rows_to_move)

        # 3) In Main-DB löschen
        if not without_rowid and rowids:
            for i in range(0, len(rowids), 1000):
                chunk = rowids[i:i+1000]
                qmarks = ",".join(["?"] * len(chunk))
                self.main_cur.execute(f"DELETE FROM {table} WHERE rowid IN ({qmarks})", tuple(chunk))
        else:
            if pk_cols:
                where_pk = " AND ".join([f"{k} = ?" for k in pk_cols])
                col_index: Dict[str, int] = {name: idx for idx, name in enumerate(cols)}
                pk_indices = [col_index[k] for k in pk_cols]
                pk_rows = []
                for r in rows_to_move:
                    pk_rows.append(tuple(r[i] for i in pk_indices))
                pk_rows = list(dict.fromkeys(pk_rows))
                for t in pk_rows:
                    self.main_cur.execute(f"DELETE FROM {table} WHERE {where_pk}", t)
            else:
                where_full = " AND ".join([f"{c} IS ?" for c in cols])  # null-sicher
                for r in rows_to_move:
                    self.main_cur.execute(f"DELETE FROM {table} WHERE {where_full}", r)

        return len(rows_to_move)

    # ---------- öffentlich ----------

    def extract(self) -> int:
        roots = self.load_roots()
        known = set(self.list_tables())
        total_moved = 0

        for table, const in roots:
            if table not in known:
                continue
            try:
                moved = self._move_one(table, const)
            except (sqlite3.OperationalError, ValueError) as e:
                moved = 0

            if moved > 0:
                total_moved += moved
         

        self.fo_conn.commit()
        self.main_conn.commit()
        return total_moved

    def close(self):
        try:
            self.main_conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        except Exception:
            pass
        try:
            self.fo_conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        except Exception:
            pass
        self.main_conn.close()
        self.fo_conn.close()


def main():
    # HIER werden die Daten aus fs_records_big.db gelesen
    # und in fo_records_big.db GESPEICHERT.
    extractor = SecurityExtractor('fs_records.db', 'fo_records.db', 'C.txt')
    count = extractor.extract()
    print(f"Total records moved: {count}")
    extractor.close()

    # (Optional) Wenn du parallel noch eine Kopie für den Chase möchtest:
    import shutil
    shutil.copyfile('fs_records.db', 'ChaseTable.db')

if __name__ == '__main__':
    main()
