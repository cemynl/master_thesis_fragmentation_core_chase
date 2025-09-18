import sqlite3
import re
from typing import List, Dict, Any

class TGDDslSimulator:
    def __init__(self, conn: sqlite3.Connection, rules: List[str]):
        self.conn = conn
        self.cur = conn.cursor()
        self.rules = rules
        self.tgds = [self._compile_rule(r) for r in rules]

    def _get_pk_and_const_col(self, table: str) -> (str, str):
        self.cur.execute(f"PRAGMA table_info({table})")
        info = self.cur.fetchall()
        if not info:
            raise ValueError(f"Tabelle {table} existiert nicht oder hat keine Spalten")

        pk_col = next((col[1] for col in info if col[5] > 0), info[0][1])
        
        const_col = next((col[1] for col in info if col[1] != pk_col), None)
        if not const_col:
            raise ValueError(f"Keine geeignete konstante Spalte in Tabelle {table} gefunden")
        return pk_col, const_col


    def _compile_rule(self, rule: str) -> Dict[str, Any]:
        body_str, head_str = [p.strip() for p in rule.split('->')]
        def parse(atom: str):
            m = re.match(r"^(?P<table>\w+)\(n,\s*'(?P<const>[^']+)'\)$", atom)
            if not m:
                raise ValueError(f"Ungültiges Atom: {atom}")
            tbl, const = m.group('table'), m.group('const')
            pk_col, const_col = self._get_pk_and_const_col(tbl)
            return tbl, pk_col, const_col, const

    
        atoms = [a.strip() for a in re.split(r'\s*(?:∧|&|AND|\band\b)\s*', body_str)]

        select_clauses = []
        select_params: List[Any] = []
        for atom in atoms:
            tbl, pk_col, const_col, const = parse(atom)
            select_clauses.append(f"SELECT {pk_col} FROM {tbl} WHERE {const_col} = ?")
            select_params.append(const)
        select_sql = ' INTERSECT '.join(select_clauses)

        head_tbl, head_pk, head_col, head_const = parse(head_str)
        insert_sql = f"INSERT OR IGNORE INTO {head_tbl}({head_pk}, {head_col}) VALUES(?,?)"

        return {
            'rule': rule,
            'select_sql': select_sql,
            'select_params': select_params,
            'insert_sql': insert_sql,
            'insert_vals': (head_const,),  # head_const to be appended
            'head_tbl': head_tbl,
            'head_pk': head_pk,
            'head_col': head_col
        }

    def apply_tgd(self, t: Dict[str, Any]) -> int:
        self.cur.execute(t['select_sql'], tuple(t['select_params']))
        pks = [row[0] for row in self.cur.fetchall()]
        count = 0

        for pk in pks:
            # Werte fürs INSERT zusammenstellen
            vals = (pk, *t['insert_vals'])
            clean_vals = tuple('UNKNOWN' if v is None else v for v in vals)

            # Prüfen, ob der Eintrag schon existiert
            insert_cols = [t['head_pk'], t['head_col']]
            where_clauses = [f"{col} = ?" for col in insert_cols]
            check_sql = f"""
                SELECT 1 FROM {t['head_tbl']} WHERE {' AND '.join(where_clauses)}
            """
            self.cur.execute(check_sql, clean_vals)
            if not self.cur.fetchone():
                # Insert ausführen
                insert_sql = f"""
                    INSERT INTO {t['head_tbl']}({', '.join(insert_cols)})
                    VALUES ({', '.join(['?'] * len(insert_cols))})
                """
                self.cur.execute(insert_sql, clean_vals)
                self.conn.commit()  # sofort speichern, um Query darunter konsistent zu haben
                count += self.cur.rowcount

                # Nochmals auslesen, um den kompletten neuen Datensatz zu zeigen
                sel_all = f"SELECT * FROM {t['head_tbl']} WHERE {' AND '.join(where_clauses)}"
                self.cur.execute(sel_all, clean_vals)
                new_row = self.cur.fetchone()
                #print(f"  → TGD `{t['rule']}` angewendet, neuer Eintrag in `{t['head_tbl']}`: {new_row}")

        return count




    def chase(self, max_iter: int = 10) -> None:
        for i in range(1, max_iter + 1):
            counts = [self.apply_tgd(t) for t in self.tgds]
            self.conn.commit()
            total = sum(counts)
            if total > 0:
                print(f"Iteration {i}: +{total} new tuples")
            else:
                print(f"Iteration {i}: (no new tuples) – fixpoint reached.")
                break

def load_lines(filepath: str) -> List[str]:
    with open(filepath, 'r', encoding='utf-8') as f:
        return [l.strip() for l in f if l.strip() and not l.startswith('#')]

if __name__ == '__main__':
    import shutil
    orig, chase_db = 'fs_records.db', 'ChaseTable.db'
    shutil.copyfile(orig, chase_db)

    rules = load_lines('rules.txt')
    conn = sqlite3.connect(chase_db)
    sim = TGDDslSimulator(conn, rules)
    sim.chase(max_iter=10)
    conn.close()
