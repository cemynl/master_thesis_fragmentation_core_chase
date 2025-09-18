import ast
from typing import List, Set


class PathCombinator:
    @staticmethod
    def load_paths(filepath: str) -> List[List[List[str]]]:
        """Lädt die gruppierten Pfade (Liste von Pfad-Listen) aus der Datei."""
        with open(filepath, 'r', encoding='utf-8') as f:
            return ast.literal_eval(f.read())

    @staticmethod
    def generate_optimal_union(groups: List[List[List[str]]]) -> List[str]:
        """
        Exakte Lösung: durchläuft alle Kombinationen (nur bei kleinen Instanzen praktikabel).
        Gibt die minimale Union zurück.
        """
        from itertools import product

        # Leere Unterlisten entfernen
        filtered_groups = []
        for group in groups:
            non_empty = [sub for sub in group if sub]
            if non_empty:
                filtered_groups.append(non_empty)

        best_union: Set[str] = set()
        best_size = None

        for combo in product(*filtered_groups):
            union_set = set()
            for sub in combo:
                union_set.update(sub)
            u_size = len(union_set)
            if best_size is None or u_size < best_size:
                best_size = u_size
                best_union = union_set.copy()

        return list(best_union)

    @staticmethod
    def generate_greedy_union(groups: List[List[List[str]]]) -> List[str]:
        """
        Greedy-Heuristik: pro Gruppe die Unterliste wählen,
        die die aktuelle Union minimal vergrößert.
        """
        current_union: Set[str] = set()

        for group in groups:
            non_empty = [sub for sub in group if sub]
            if not non_empty:
                continue

            # wähle die beste Menge bzgl. Union-Vergrößerung
            best = min(non_empty, key=lambda s: len(current_union.union(s)))
            current_union.update(best)

        return list(current_union)


if __name__ == '__main__':
    # Gruppierte Pfade laden
    groups = PathCombinator.load_paths('paths.txt')

    # Exakte Lösung (nur kleine Instanzen!)
    # optimal_set = PathCombinator.generate_optimal_union(groups)
    # print('Optimales Hitting Set:', optimal_set)

    # Greedy Lösung (skalierbarer)
    greedy_set = PathCombinator.generate_greedy_union(groups)
    print('Minimal Union Set:', greedy_set)

    # Ergebnisse speichern
    # with open('union_optimal.txt', 'w', encoding='utf-8') as f:
    #     f.write(repr(optimal_set))
    with open('union_greedy.txt', 'w', encoding='utf-8') as f:
        f.write(repr(greedy_set))

