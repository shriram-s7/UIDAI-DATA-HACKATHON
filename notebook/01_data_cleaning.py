import os
import pandas as pd

# ======================
# PATHS
# ======================
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
OUTPUT_DIR = os.path.join(ROOT_DIR, "outputs")

ENROLMENT_DIR = os.path.join(DATA_DIR, "enrolment")
DEMOGRAPHIC_DIR = os.path.join(DATA_DIR, "demographic")
BIOMETRIC_DIR = os.path.join(DATA_DIR, "biometric")

REF_SD_PATH = os.path.join(DATA_DIR, "reference_states_districts.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ======================
# HELPERS
# ======================
def normalize_text(s):
    if pd.isna(s):
        return None
    s = str(s).upper().strip()
    s = s.replace("&", "AND")
    s = s.replace("THE ", "")
    s = " ".join(s.split())
    return s


def standardize_columns(df):
    rename_map = {
        "District": "district",
        "district_name": "district",
        "districtName": "district",
        "district_nm": "district",
        "State": "state",
        "state_name": "state",
        "stateName": "state",
    }
    return df.rename(columns=rename_map)


def load_all_csvs(folder, name):
    dfs = []
    for f in os.listdir(folder):
        if not f.endswith(".csv"):
            continue

        df = pd.read_csv(os.path.join(folder, f))
        df = standardize_columns(df)
        df["source_file"] = f

        if "district" not in df.columns or "state" not in df.columns:
            raise RuntimeError(f"[FATAL] {name}: '{f}' missing state/district")

        dfs.append(df)

    if not dfs:
        raise RuntimeError(f"[FATAL] No CSVs loaded for {name}")

    return pd.concat(dfs, ignore_index=True)


# ======================
# LOAD RAW DATA
# ======================
print("Loading raw datasets...")
enrolment_df = load_all_csvs(ENROLMENT_DIR, "ENROLMENT")
demographic_df = load_all_csvs(DEMOGRAPHIC_DIR, "DEMOGRAPHIC")
biometric_df = load_all_csvs(BIOMETRIC_DIR, "BIOMETRIC")

# ======================
# LOAD REFERENCE
# ======================
print("Loading reference states+districts...")
ref_sd = pd.read_csv(REF_SD_PATH)
ref_sd = ref_sd.rename(columns={"State": "state", "District": "district"})

ref_sd["state_norm"] = ref_sd["state"].apply(normalize_text)
ref_sd["district_norm"] = ref_sd["district"].apply(normalize_text)
ref_sd = ref_sd.drop_duplicates(subset=["state_norm", "district_norm"])

ref_states = ref_sd[["state_norm"]].drop_duplicates()

# ======================
# STATE CLEANING
# ======================
def clean_state(df):
    df["state_norm_tmp"] = df["state"].apply(normalize_text)

    df = df.merge(
        ref_states,
        left_on="state_norm_tmp",
        right_on="state_norm",
        how="left",
        indicator=True
    )

    df["state_normalized"] = df["state"]
    df.loc[df["_merge"] == "both", "state_normalized"] = df["state_norm"]

    df["state_match_status"] = df["_merge"].map({
        "both": "AUTO",
        "left_only": "UNMAPPED"
    })

    return df.drop(columns=["_merge", "state_norm_tmp", "state_norm"])


print("Cleaning states...")
enrolment_df = clean_state(enrolment_df)
demographic_df = clean_state(demographic_df)
biometric_df = clean_state(biometric_df)

# ======================
# DISTRICT CLEANING
# ======================
def clean_district(df):
    df["district_norm_tmp"] = df["district"].apply(normalize_text)

    df = df.merge(
        ref_sd,
        left_on=["state_normalized", "district_norm_tmp"],
        right_on=["state_norm", "district_norm"],
        how="left",
        indicator=True
    )

    df["district_cleaned"] = df["district_x"]
    df.loc[df["_merge"] == "both", "district_cleaned"] = df["district_y"]

    df["district_match_status"] = df["_merge"].map({
        "both": "AUTO",
        "left_only": "UNMAPPED"
    })

    return df.drop(columns=[
        "_merge",
        "district_norm_tmp",
        "district_x",
        "district_y",
        "state_norm",
        "district_norm"
    ])


print("Cleaning districts...")
enrolment_df = clean_district(enrolment_df)
demographic_df = clean_district(demographic_df)
biometric_df = clean_district(biometric_df)

# ======================
# AGGREGATION (AGE-AWARE)
# ======================
def aggregate_enrolment(df):
    cols = ["age_0_5", "age_5_17", "age_18_greater"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Enrolment missing columns: {missing}")

    return df.groupby(
        ["date", "state_normalized", "district_cleaned"],
        as_index=False
    )[cols].sum()


def aggregate_biometric(df):
    cols = [c for c in df.columns if c.startswith("bio_age_")]
    if not cols:
        raise RuntimeError("No biometric age columns found")

    return df.groupby(
        ["date", "state_normalized", "district_cleaned"],
        as_index=False
    )[cols].sum()


def aggregate_demographic(df):
    cols = [c for c in df.columns if c.startswith("demo_age_")]
    if not cols:
        raise RuntimeError("No demographic age columns found")

    return df.groupby(
        ["date", "state_normalized", "district_cleaned"],
        as_index=False
    )[cols].sum()


print("Aggregating...")
enrolment_df = aggregate_enrolment(enrolment_df)
biometric_df = aggregate_biometric(biometric_df)
demographic_df = aggregate_demographic(demographic_df)

# ======================
# SAVE OUTPUTS
# ======================
enrolment_df.to_csv(os.path.join(OUTPUT_DIR, "enrolment_clean.csv"), index=False)
biometric_df.to_csv(os.path.join(OUTPUT_DIR, "biometric_clean.csv"), index=False)
demographic_df.to_csv(os.path.join(OUTPUT_DIR, "demographic_clean.csv"), index=False)

print("✅ Data cleaning completed successfully.")
