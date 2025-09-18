import sqlite3
import random
import string

# ----------------------------
# Datenlisten
# ----------------------------

MEDICINE_LIST = [
    # Schmerz & Entzündung
    "Aspirin", "Ibuprofen", "Paracetamol", "Diclofenac", "Naproxen", "Metamizol",
    "Celecoxib", "Etoricoxib", "Indometacin", "Ketoprofen",

    # Antibiotika
    "Amoxicillin", "Penicillin", "Azithromycin", "Clarithromycin", "Doxycyclin",
    "Ciprofloxacin", "Levofloxacin", "Metronidazol", "Ceftriaxon", "Vancomycin",

    # Herz-Kreislauf
    "Atorvastatin", "Simvastatin", "Rosuvastatin", "Ramipril", "Enalapril",
    "Lisinopril", "Losartan", "Valsartan", "Amlodipin", "Bisoprolol",

    # Stoffwechsel / Diabetes
    "Metformin", "Insulin", "Glimepirid", "Sitagliptin", "Empagliflozin",
    "Dulaglutid", "Pioglitazon", "Linagliptin", "Gliclazid", "Canagliflozin",

    # Magen & Darm
    "Omeprazole", "Pantoprazole", "Esomeprazole", "Lansoprazole", "Ranitidin",
    "Famotidin", "Domperidon", "Ondansetron", "Mesalazin", "Budesonid",

    # Hormone
    "Levothyroxine", "Liothyronin", "Hydrocortison", "Prednisolon", "Dexamethason",
    "Estradiol", "Progesteron", "Testosteron", "Finasterid", "Tamoxifen",

    # HIV/AIDS Medikamente
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

SPECIALTIES = [
    "General Practice", "Internal Medicine", "Cardiology", "Neurology", "Infectious Disease",
    "Pediatrics", "Oncology", "Dermatology", "Endocrinology", "Gastroenterology",
    "Hematology", "Nephrology", "Pulmonology", "Rheumatology", "Geriatrics",
    "Ophthalmology", "Urology", "Orthopedics", "Emergency Medicine",
    "Radiology", "Pathology", "Anesthesiology", "Psychiatry",
    "Obstetrics and Gynecology", "Sports Medicine", "Plastic Surgery"
]

# ----------------------------
# Hilfsfunktionen
# ----------------------------

def rand_string(length=6):
    return ''.join(random.choices(string.ascii_letters, k=length))

def rand_gender():
    return random.choice(['M', 'F', 'D'])

def rand_specialty():
    return random.choice(SPECIALTIES)

def rand_diagnosis():
    return random.choice(DIAGNOSIS_LIST)

def rand_medicine():
    return random.choice(MEDICINE_LIST)

# ----------------------------
# Schema erstellen
# ----------------------------

def create_schema(conn):
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS Patient (
        Name    TEXT,
        Age     INTEGER,
        Gender  TEXT,
        UNIQUE(Name, Age, Gender)
    );

    CREATE TABLE IF NOT EXISTS Illness (
        PatientName TEXT NOT NULL,
        Diagnosis   TEXT NOT NULL,
        UNIQUE(PatientName, Diagnosis)
    );

    CREATE TABLE IF NOT EXISTS Treatment (
        PatientName   TEXT NOT NULL,
        TreatmentName TEXT NOT NULL,
        UNIQUE(PatientName, TreatmentName)
    );

    CREATE TABLE IF NOT EXISTS Doctor (
        PatientName TEXT NOT NULL,
        DoctorName  TEXT NOT NULL,
        Specialty   TEXT NOT NULL,
        UNIQUE(PatientName, DoctorName, Specialty)
    );

    CREATE TABLE IF NOT EXISTS Medicine (
        PatientName TEXT NOT NULL,
        MedName     TEXT NOT NULL,
        UNIQUE(PatientName, MedName)
    );
    """)
    conn.commit()

# ----------------------------
# Daten füllen
# ----------------------------

def populate(conn,
             num_patients=1000,
             min_illness=1, max_illness=3,
             min_treat=1, max_treat=2,
             min_doctors=1, max_doctors=2,
             min_meds=0, max_meds=3):
    cur = conn.cursor()

    patients = []

    # Patienten
    for _ in range(num_patients):
        name = rand_string(8)
        age = random.randint(1, 90)
        gender = rand_gender()
        cur.execute("INSERT OR IGNORE INTO Patient VALUES(?,?,?)", (name, age, gender))
        patients.append(name)

    for name in patients:
        for _ in range(random.randint(min_illness, max_illness)):
            diag = rand_diagnosis()
            cur.execute("INSERT OR IGNORE INTO Illness VALUES(?,?)", (name, diag))

    for name in patients:
        for _ in range(random.randint(min_treat, max_treat)):
            treat = "Treat_" + ''.join(random.choices(string.ascii_uppercase, k=3))
            cur.execute("INSERT OR IGNORE INTO Treatment VALUES(?,?)", (name, treat))

    for name in patients:
        for _ in range(random.randint(min_doctors, max_doctors)):
            docname = "Dr_" + rand_string(5)
            spec = rand_specialty()
            cur.execute("INSERT OR IGNORE INTO Doctor VALUES(?,?,?)", (name, docname, spec))

    for name in patients:
        num_meds = random.randint(min_meds, max_meds)
        chosen = random.sample(MEDICINE_LIST, num_meds)
        for med in chosen:
            cur.execute("INSERT OR IGNORE INTO Medicine VALUES(?,?)", (name, med))

    conn.commit()

# ----------------------------
# Main
# ----------------------------

def main():
    fs_db = 'fs_records.db'
    fo_db = 'fo_records.db'
    chase_db = 'ChaseTable.db'

    conn_fs = sqlite3.connect(fs_db)
    conn_fo = sqlite3.connect(fo_db)
    conn_chase = sqlite3.connect(chase_db)

    for c in [conn_fs, conn_fo, conn_chase]:
        create_schema(c)

    populate(conn_fs, num_patients=1000)

    for db_name, c in [('FS', conn_fs), ('FO', conn_fo), ('Chase', conn_chase)]:
        print(f"\n{db_name}_DB:")
        for tbl in ['Patient','Illness','Treatment','Doctor','Medicine']:
            cnt = c.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  {tbl}: {cnt} rows")
    print("")
    conn_fs.close()
    conn_fo.close()
    conn_chase.close()

if __name__ == "__main__":
    main()
