import sqlite3
import ast
from typing import List, Tuple

class TransferAndDelete:

    def __init__(self, main_db: str, fo_db: str, hs_file: str):
        self.main_db = main_db
        self.fo_db = fo_db
        self.hs_file = hs_file

    @staticmethod
    def load_minimal_union(filepath: str) -> List[str]:
        with open(filepath, 'r', encoding='utf-8') as f:
            return ast.literal_eval(f.read())

    @staticmethod
    def _get_table_info(cur: sqlite3.Cursor, table: str) -> List[str]:
        """
        Returns a list of all columns.
        """
        cur.execute(f"PRAGMA table_info({table})")
        info = cur.fetchall()
        cols = [row[1] for row in info]
        return cols


    def process(self) -> int:
        nodes = self.load_minimal_union(self.hs_file)
        conn_main = sqlite3.connect(self.main_db)
        cur_main = conn_main.cursor()
        conn_fo = sqlite3.connect(self.fo_db)
        cur_fo = conn_fo.cursor()

        total_inserted = 0
        total_deleted = 0

        for node in nodes:
            try:
                table, val1, val2 = node.split(':', 2)
            except ValueError:
                print(f"Invalid format: {node}")
                continue

            # Determine columns (without PK logic)
            try:
                cols = self._get_table_info(cur_main, table)
            except sqlite3.OperationalError:
                print(f"Table not found: {table}")
                continue

            if len(cols) < 2:
                print(f"Not enough columns in table {table} (need at least 2)")
                continue

            key1, key2 = cols[0], cols[1]

            # Select records
            cur_main.execute(
                f"SELECT {', '.join(cols)} FROM {table} WHERE {key1} = ? AND {key2} = ?",
                (val1, val2)
            )
            rows = cur_main.fetchall()
            if not rows:
                print(f"No entries in {table} for {key1}={val1}, {key2}={val2}")
                continue

            # Create FO table if needed
            col_defs = ", ".join(f"{c} TEXT" for c in cols)
            cur_fo.execute(f"CREATE TABLE IF NOT EXISTS {table} ({col_defs})")

            # Insert into FO
            placeholder = ", ".join('?' for _ in cols)
            for row in rows:
                cur_fo.execute(
                    f"INSERT OR IGNORE INTO {table}({', '.join(cols)}) VALUES({placeholder})",
                    row
                )
            total_inserted += len(rows)

            # Delete from main DB and count
            cur_main.execute(
                f"DELETE FROM {table} WHERE {key1} = ? AND {key2} = ?",
                (val1, val2)
            )
            total_deleted += cur_main.rowcount

        conn_fo.commit()
        conn_main.commit()
        conn_fo.close()
        conn_main.close()

        print(f"Total inserted into FO: {total_inserted}")
        print(f"Total deleted from FS: {total_deleted}")

        return total_deleted



if __name__ == '__main__':
    mover = TransferAndDelete(
        main_db='fs_records.db',
        fo_db='fo_records.db',
        hs_file='union_greedy.txt'
    )
    mover.process()
