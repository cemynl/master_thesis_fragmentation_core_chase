import sqlite3
import random
import string
import datetime

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




def rand_string(length=6):
    return ''.join(random.choices(string.ascii_letters, k=length))

def rand_string(length=6):
    return ''.join(random.choices(string.ascii_letters, k=length))


def rand_gender():
    return random.choice(['M', 'F', 'D'])

def rand_diagnosis():
    return random.choice(DIAGNOSIS_LIST)

def rand_medicine():
    return random.choice(MEDICINE_LIST)

def rand_allergy():
    return random.choice(ALLERGY_LIST)

def rand_insurance():
    return random.choice(INSURANCE_LIST)

def rand_labresult():
    return random.choice(LAB_TESTS)

def rand_hospital():
    return random.choice(HOSPITAL_LIST)

def rand_appointment():
    start = datetime.date(2020, 1, 1)
    end = datetime.date(2025, 12, 31)
    delta = (end - start).days
    random_day = start + datetime.timedelta(days=random.randint(0, delta))
    return str(random_day)

def rand_treatment():
    letters = ''.join(random.choices(string.ascii_uppercase, k=3))
    return f"Treat_{letters}"


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

    CREATE TABLE IF NOT EXISTS Medicine (
        PatientName TEXT NOT NULL,
        MedName     TEXT NOT NULL,
        UNIQUE(PatientName, MedName)
    );

    CREATE TABLE IF NOT EXISTS Allergy (
        PatientName TEXT NOT NULL,
        AllergyName TEXT NOT NULL,
        UNIQUE(PatientName, AllergyName)
    );

    CREATE TABLE IF NOT EXISTS Insurance (
        PatientName   TEXT NOT NULL,
        InsuranceName TEXT NOT NULL,
        UNIQUE(PatientName, InsuranceName)
    );

    CREATE TABLE IF NOT EXISTS LabResult (
        PatientName TEXT NOT NULL,
        TestName    TEXT NOT NULL,
        UNIQUE(PatientName, TestName)
    );

    CREATE TABLE IF NOT EXISTS Appointment (
        PatientName     TEXT NOT NULL,
        AppointmentDate TEXT NOT NULL,
        UNIQUE(PatientName, AppointmentDate)
    );

    CREATE TABLE IF NOT EXISTS Hospital (
        PatientName TEXT NOT NULL,
        HospitalName TEXT NOT NULL,
        UNIQUE(PatientName, HospitalName)
    );
    CREATE TABLE IF NOT EXISTS Treatment (
        PatientName   TEXT NOT NULL,
        TreatmentName TEXT NOT NULL,
        UNIQUE(PatientName, TreatmentName)
    );

                      
    """)
    conn.commit()

def populate(conn,
             num_patients=1000,
             min_illness=0, max_illness=3,
             min_meds=0, max_meds=3,
             min_allergy=0, max_allergy=2,
             min_insurance=0, max_insurance=1,
             min_lab=0, max_lab=3,
             min_appt=0, max_appt=2,
             min_hosp=0, max_hosp=2,
             min_treat=0, max_treat=2):
    cur = conn.cursor()

    patients = []

    for _ in range(num_patients):
        name = rand_string(8)
        age = random.randint(1, 90)
        gender = rand_gender()
        cur.execute("INSERT OR IGNORE INTO Patient VALUES(?,?,?)", (name, age, gender))
        patients.append(name)

    for name in patients:
        for _ in range(random.randint(min_illness, max_illness)):
            cur.execute("INSERT OR IGNORE INTO Illness VALUES(?,?)", (name, rand_diagnosis()))

    for name in patients:
        for med in random.sample(MEDICINE_LIST, random.randint(min_meds, max_meds)):
            cur.execute("INSERT OR IGNORE INTO Medicine VALUES(?,?)", (name, med))

    for name in patients:
        for _ in range(random.randint(min_allergy, max_allergy)):
            cur.execute("INSERT OR IGNORE INTO Allergy VALUES(?,?)", (name, rand_allergy()))

    for name in patients:
        cur.execute("INSERT OR IGNORE INTO Insurance VALUES(?,?)", (name, rand_insurance()))

    for name in patients:
        for _ in range(random.randint(min_lab, max_lab)):
            cur.execute("INSERT OR IGNORE INTO LabResult VALUES(?,?)", (name, rand_labresult()))

    for name in patients:
        for _ in range(random.randint(min_appt, max_appt)):
            cur.execute("INSERT OR IGNORE INTO Appointment VALUES(?,?)", (name, rand_appointment()))

    for name in patients:
        for _ in range(random.randint(min_hosp, max_hosp)):
            cur.execute("INSERT OR IGNORE INTO Hospital VALUES(?,?)", (name, rand_hospital()))

    for name in patients:
        for _ in range(random.randint(min_treat, max_treat)):
            cur.execute("INSERT OR IGNORE INTO Treatment VALUES(?,?)", (name, rand_treatment()))

    conn.commit()



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
        for tbl in ['Patient','Illness','Medicine','Allergy',
                    'Insurance','LabResult','Appointment','Hospital','Treatment']:
            cnt = c.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  {tbl}: {cnt} rows")
    print("")
    conn_fs.close()
    conn_fo.close()
    conn_chase.close()

if __name__ == "__main__":
    main()
