import random
from typing import List, Dict, List as TList, Tuple, Set

SEED = 48
NUM_RULES = 100
BODY_MIN, BODY_MAX = 2, 4

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
    "Flu", "Common_Cold", "COVID-19", "Tuberculosis", "Hepatitis_B", "Hepatitis_C",
    "Aids", "Malaria", "Pneumonia", "Measles",
    "Hypertension", "Coronary_Artery_Disease", "Heart_Failure", "Atrial_Fibrillation", "Stroke",
    "Diabetes", "Obesity", "Metabolic_Syndrome", "Hyperlipidemia", "Gout",
    "Asthma", "COPD", "Bronchitis", "Pulmonary_Fibrosis", "Sleep_Apnea",
    "Migraine", "Epilepsy", "Parkinsons_Disease", "Alzheimers_Disease", "Multiple_Sclerosis",
    "Arthritis", "Rheumatoid_Arthritis", "Lupus", "Psoriasis", "Crohns_Disease",
    "Breast_Cancer", "Lung_Cancer", "Prostate_Cancer", "Colorectal_Cancer", "Leukemia",
    "Gastritis", "Ulcerative_Colitis", "Pancreatitis", "Celiac_Disease",
    "Depression", "Anxiety_Disorder", "Bipolar_Disorder", "Schizophrenia", "PTSD"
]


ALLERGY_LIST = [
    "Peanuts", "Gluten", "Lactose", "Shellfish", "Pollen", "Dust", "Latex",
    "Soy", "Eggs", "Fish", "Wheat", "Insect_Sting", "Nickel", "Animal_Dander"
]

INSURANCE_LIST = ["AOK", "TK", "Barmer", "DAK", "IKK", "Private", "Debeka", "Allianz"]

LAB_TESTS = [
    "Complete_Blood_Count_CBC",
    "White_Blood_Cell_Count",
    "Red_Blood_Cell_Count",
    "Hemoglobin",
    "Hematocrit",
    "Platelet_Count",

    "Blood_Glucose_Fasting",
    "HbA1c",
    "Insulin",
    "Electrolytes_Na_K_Cl_Ca_Mg",
    "Cholesterol",
    "LDL_Cholesterol",
    "HDL_Cholesterol",
    "Triglycerides",
    "Liver_Function_Test_LFT",
    "Kidney_Function_Test_KFT",
    "Urea",
    "Creatinine",
    "Uric_Acid",
    "Bilirubin",
    "Albumin",

    "C-Reactive_Protein_CRP",
    "Erythrocyte_Sedimentation_Rate_ESR",
    "Procalcitonin",
    "Blood_Culture",
    "Urine_Culture",
    "Sputum_Culture",
    "Stool_Culture",

    "Thyroid_Stimulating_Hormone_TSH",
    "T3_Triiodothyronine",
    "T4_Thyroxine",
    "Cortisol",
    "Estrogen",
    "Progesterone",
    "Testosterone",
    "Prolactin",
    "Parathyroid_Hormone_PTH",

    "Vitamin_D",
    "Vitamin_B12",
    "Folate",
    "Iron",
    "Ferritin",
    "Transferrin",

    "Prothrombin_Time_PT",
    "INR_International_Normalized_Ratio",
    "aPTT_Activated_Partial_Thromboplastin_Time",
    "D-Dimer",

    "ECG",
    "Echocardiogram",
    "Troponin",
    "BNP_B-type_Natriuretic_Peptide",
    "CK-MB",

    "EEG",
    "MRI_Brain",
    "CT-Scan_Head",
    "Lumbar_Puncture_CSF_Analysis",

    "X-Ray_Chest",
    "X-Ray_Abdomen",
    "Ultrasound",
    "CT-Scan_Abdomen",
    "PET-CT",
    "MRI_Spine",

    "PSA",
    "CA-125",
    "CEA",
    "AFP",
    "BRCA_Genetic_Test",

    "ANA_Antinuclear_Antibody",
    "Rheumatoid_Factor",
    "HIV_Test",
    "Hepatitis_B_Surface_Antigen",
    "Hepatitis_C_Antibody",
    "Syphilis_Test_VDRL",
    "Tuberculin_Skin_Test",
    "COVID-19_PCR",
    "COVID-19_Antigen",
    "COVID-19_Antibody",

    "Urine_Routine_Test",
    "Urine_Microscopy",
    "Urine_Protein",
    "Urine_Ketones",
    "Urine_Glucose",
    "Stool_Routine_Test",
    "Stool_Occult_Blood",
    "Stool_Ova_and_Parasite",

    "Karyotyping",
    "PCR_Test",
    "DNA_Sequencing",
    "HLA_Typing",
]


HOSPITAL_LIST = [
    "Charité_Berlin",
    "Universitätsklinikum_Heidelberg",
    "LMU_Klinikum_München",
    "UKE_Hamburg",
    "Uniklinik_Köln",
    "Uniklinik_Frankfurt",
    "UK_Essen",
    "UK_Freiburg",
    "Klinikum_rechts_der_Isar_München",
    "Uniklinik_Leipzig"
]

DEPARTMENTS = [
    "General_Medicine",
    "Internal_Medicine",
    "Family_Medicine",
    "Geriatrics",
    "Cardiology",
    "Vascular_Surgery",
    "Cardiothoracic_Surgery",
    "Neurology",
    "Neurosurgery",
    "Oncology",
    "Radiation_Oncology",
    "Hematology",
    "Pediatrics",
    "Neonatology",
    "Obstetrics_and_Gynecology",
    "Reproductive_Medicine",
    "Orthopedics",
    "Trauma_Surgery",
    "Plastic_Surgery",
    "Transplant_Surgery",
    "General_Surgery",
    "Emergency_Medicine",
    "Intensive_Care_Unit_ICU",
    "Anesthesiology",
    "Pain_Management",
    "Radiology",
    "Pathology",
    "Nuclear_Medicine",
    "Laboratory_Medicine",
    "Psychiatry",
    "Psychology",
    "Addiction_Medicine",
    "Dermatology",
    "Ophthalmology",
    "Otolaryngology_ENT",
    "Endocrinology",
    "Gastroenterology",
    "Nephrology",
    "Pulmonology",
    "Rheumatology",
    "Infectious_Disease",
    "Urology",
    "Sports_Medicine",
    "Allergy_and_Immunology",
    "Occupational_Medicine",
    "Palliative_Care"
]




# ----------------------------------------------------------
def random_treatment() -> str:
    letters = ''.join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=3))
    return f"Treat_{letters}"

# ---- Relation -> Wert-Pool ----
REL_TO_POOL: Dict[str, TList[str]] = {
    "Treatment": [],  # dynamisch
    "Medicine":  MEDICINE_LIST,
    "Illness":   DIAGNOSIS_LIST,
    "Allergy":   ALLERGY_LIST,
    "Insurance": INSURANCE_LIST,
    "LabResult": LAB_TESTS,
    "Hospital":  HOSPITAL_LIST,
}
RELATIONS: List[str] = list(REL_TO_POOL.keys())

LEVELS = {rel: i for i, rel in enumerate(RELATIONS)}

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

def generate_rule(graph: Dict[Tuple[str,str], Set[Tuple[str,str]]]) -> Tuple[str, Tuple[str,str]]:
    while True:
        head_rel = random.choice(RELATIONS)
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

def main():
    tgds, heads = generate_tgds()

    # schreibe rules.txt
    with open("rules.txt", "w", encoding="utf-8", newline="\n") as f:
        for rule in tgds:
            f.write(rule + "\n")
    print("rules.txt written ✅")

    num_c = max(1, NUM_RULES // 10)
    selected_heads = random.sample(sorted(heads), min(num_c, len(heads)))

    with open("C.txt", "w", encoding="utf-8") as f:
        for rel, const in selected_heads:
            f.write(f"{rel}['{const}']\n")
    print(f"C.txt written ✅ with {len(selected_heads)} entries")

if __name__ == "__main__":
    main()
