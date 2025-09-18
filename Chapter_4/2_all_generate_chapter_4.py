#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
import string
from typing import List, Dict, List as TList, Tuple, Set

# ---- reproducibility & config ----
SEED = 46
NUM_RULES = 100
BODY_MIN, BODY_MAX = 1,3

# ---- domains ----
MEDICINE_LIST = [
    "Aspirin", "Ibuprofen", "Paracetamol", "Diclofenac", "Naproxen", "Metamizol",
    "Celecoxib", "Etoricoxib", "Indometacin", "Ketoprofen",
    "Amoxicillin", "Penicillin", "Azithromycin", "Clarithromycin", "Doxycyclin",
    "Ciprofloxacin", "Levofloxacin", "Metronidazol", "Ceftriaxon", "Vancomycin",
    "Atorvastatin", "Simvastatin", "Rosuvastatin", "Ramipril", "Enalapril",
    "Lisinopril", "Losartan", "Valsartan", "Amlodipin", "Bisoprolol",
    "Metformin", "Insulin", "Glimepirid", "Sitagliptin", "Empagliflozin",
    "Dulaglutid", "Pioglitazon", "Linagliptin", "Gliclazid", "Canagliflozin",
    "Omeprazole", "Pantoprazole", "Esomeprazole", "Lansoprazole", "Ranitidin",
    "Famotidin", "Domperidon", "Ondansetron", "Mesalazin", "Budesonid",
    "Levothyroxine", "Liothyronin", "Hydrocortison", "Prednisolon", "Dexamethason",
    "Estradiol", "Progesteron", "Testosteron", "Finasterid", "Tamoxifen",
    "Zidovudine", "Lamivudine", "Abacavir", "Tenofovir", "Emtricitabine",
    "Efavirenz", "Nevirapine", "Etravirine", "Dolutegravir", "Raltegravir",
    "Darunavir", "Atazanavir", "Ritonavir", "Lopinavir", "Indinavir"
]

DIAGNOSIS_LIST = [
    "Flu", "Common Cold", "COVID-19", "Tuberculosis", "Hepatitis B", "Hepatitis C",
    "Aids", "Malaria", "Pneumonia", "Measles",
    "Hypertension", "Coronary Artery Disease", "Heart Failure", "Atrial Fibrillation", "Stroke",
    "Diabetes", "Obesity", "Metabolic Syndrome", "Hyperlipidemia", "Gout",
    "Asthma", "COPD", "Bronchitis", "Pulmonary Fibrosis", "Sleep Apnea",
    "Migraine", "Epilepsy", "Parkinsons Disease", "Alzheimers Disease", "Multiple Sclerosis",
    "Arthritis", "Rheumatoid Arthritis", "Lupus", "Psoriasis", "Crohns Disease",
    "Breast Cancer", "Lung Cancer", "Prostate Cancer", "Colorectal Cancer", "Leukemia",
    "Gastritis", "Ulcerative Colitis", "Pancreatitis", "Celiac Disease",
    "Depression", "Anxiety Disorder", "Bipolar Disorder", "Schizophrenia", "PTSD"
]

# ----------------------------------------------------------
def random_treatment() -> str:
    letters = ''.join(random.choices(string.ascii_uppercase, k=3))
    return f"Treat_{letters}"

REL_TO_POOL: Dict[str, TList[str]] = {
    "Treatment": [],  # wird dynamisch erzeugt
    "Medicine":  MEDICINE_LIST,
    "Illness":   DIAGNOSIS_LIST,
}
RELATIONS: List[str] = list(REL_TO_POOL.keys())

LEVELS = {
    "Treatment": 0,
    "Medicine":  1,
    "Illness":   2,
}

# ----------------------------------------------------------
def random_atom(rels: List[str] = None) -> str:
    if rels is None:
        rel = random.choice(RELATIONS)
    else:
        rel = random.choice(rels)
    if rel == "Treatment":
        val = random_treatment()
    else:
        val = random.choice(REL_TO_POOL[rel])
    return f"{rel}(n,'{val}')"

def parse_atom(atom: str) -> Tuple[str, str]:
    rel, rest = atom.split("(", 1)
    const = rest.split("'")[1]
    return rel, const

# ----------------------------------------------------------
def path_exists(graph: Dict[Tuple[str,str], Set[Tuple[str,str]]],
                start: Tuple[str,str],
                target: Tuple[str,str],
                visited=None) -> bool:
    if visited is None:
        visited = set()
    if start == target:
        return True
    if start in visited:
        return False
    visited.add(start)
    for nxt in graph.get(start, []):
        if path_exists(graph, nxt, target, visited):
            return True
    return False

# ----------------------------------------------------------
def generate_rule(graph: Dict[Tuple[str,str], Set[Tuple[str,str]]]) -> Tuple[str, Tuple[str,str]]:
    while True:
        head_rel = random.choice(list(LEVELS.keys()))
        head = random_atom([head_rel])
        head_rel, head_const = parse_atom(head)

        allowed_body_rels = [r for r in RELATIONS if LEVELS[r] <= LEVELS[head_rel]]
        k = random.randint(BODY_MIN, BODY_MAX)
        body_atoms: List[str] = []
        used = set()

        while len(body_atoms) < k:
            atom = random_atom(allowed_body_rels)
            if atom not in used and atom != head:
                used.add(atom)
                body_atoms.append(atom)

        ok = True
        for atom in body_atoms:
            b_rel, b_const = parse_atom(atom)
            src = (b_rel, b_const)
            dst = (head_rel, head_const)
            if src == dst or path_exists(graph, dst, src):
                ok = False
                break

        if ok:
            for atom in body_atoms:
                b_rel, b_const = parse_atom(atom)
                src = (b_rel, b_const)
                dst = (head_rel, head_const)
                graph.setdefault(src, set()).add(dst)
            return " ∧ ".join(body_atoms) + " -> " + f"{head_rel}(n,'{head_const}')", (head_rel, head_const)

# ----------------------------------------------------------
def generate_tgds(num_rules: int = NUM_RULES, seed: int = SEED) -> Tuple[List[str], Set[Tuple[str,str]]]:
    random.seed(seed)
    tgds: List[str] = []
    graph: Dict[Tuple[str,str], Set[Tuple[str,str]]] = {}
    heads: Set[Tuple[str,str]] = set()
    for _ in range(num_rules):
        rule, head = generate_rule(graph)
        tgds.append(rule)
        heads.add(head)
    return tgds, heads

# ----------------------------------------------------------
def main():
    tgds, heads = generate_tgds()

    # schreibe rules.txt
    with open("rules.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(tgds))
    print("rules.txt written ✅")

    # zufällig ein Zehntel der Heads in C.txt
    num_c = max(1, NUM_RULES // 10)
    selected_heads = random.sample(sorted(heads), min(num_c, len(heads)))

    with open("C.txt", "w", encoding="utf-8") as f:
        for rel, const in selected_heads:
            f.write(f"{rel}['{const}']\n")
    print(f"C.txt written ✅ with {len(selected_heads)} entries")

if __name__ == "__main__":
    main()
