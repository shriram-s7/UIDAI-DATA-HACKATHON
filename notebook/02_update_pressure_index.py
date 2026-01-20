import os
import pandas as pd

# ======================
# PATHS
# ======================
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT_DIR = os.path.join(ROOT_DIR, "outputs")

ENROL_PATH = os.path.join(OUTPUT_DIR, "enrolment_clean.csv")
DEMO_PATH = os.path.join(OUTPUT_DIR, "demographic_clean.csv")
BIO_PATH = os.path.join(OUTPUT_DIR, "biometric_clean.csv")

OUT_PATH = os.path.join(OUTPUT_DIR, "district_update_pressure_index.csv")

# ======================
# LOAD CLEAN DATA
# ======================
print("Loading clean datasets...")

enrol = pd.read_csv(ENROL_PATH)
demo = pd.read_csv(DEMO_PATH)
bio = pd.read_csv(BIO_PATH)

print("✓ Clean CSVs loaded")

# ======================
# ALIGN AGE GROUPS
# ======================
def align_age_groups(df, kind):
    if kind == "enrolment":
        df["child"] = df["age_0_5"]
        df["youth"] = df["age_5_17"]
        df["adult"] = df["age_18_greater"]

    elif kind == "demographic":
        df["child"] = 0
        df["youth"] = df["demo_age_5_17"]
        df["adult"] = df["demo_age_17_"]

    elif kind == "biometric":
        df["child"] = 0
        df["youth"] = df["bio_age_5_17"]
        df["adult"] = df["bio_age_17_"]

    return df[[
        "state_normalized",
        "district_cleaned",
        "child",
        "youth",
        "adult"
    ]]

print("Aligning age groups...")

enrol_a = align_age_groups(enrol, "enrolment")
demo_a = align_age_groups(demo, "demographic")
bio_a = align_age_groups(bio, "biometric")

# ======================
# AGGREGATE TO DISTRICT
# ======================
def district_sum(df):
    return df.groupby(
        ["state_normalized", "district_cleaned"],
        as_index=False
    )[["child", "youth", "adult"]].sum()

enrol_d = district_sum(enrol_a).rename(columns={
    "child": "enrol_child",
    "youth": "enrol_youth",
    "adult": "enrol_adult"
})

demo_d = district_sum(demo_a).rename(columns={
    "child": "demo_child",
    "youth": "demo_youth",
    "adult": "demo_adult"
})

bio_d = district_sum(bio_a).rename(columns={
    "child": "bio_child",
    "youth": "bio_youth",
    "adult": "bio_adult"
})

print("✓ District aggregation complete")

# ======================
# MERGE ALL SIGNALS
# ======================
df = (
    enrol_d
    .merge(demo_d, on=["state_normalized", "district_cleaned"], how="left")
    .merge(bio_d, on=["state_normalized", "district_cleaned"], how="left")
    .fillna(0)
)

print("✓ Unified analytical table created")

# ======================
# CORE METRICS
# ======================
df["total_enrolment"] = (
    df["enrol_child"] +
    df["enrol_youth"] +
    df["enrol_adult"]
)

df["total_updates"] = (
    df["demo_youth"] +
    df["demo_adult"] +
    df["bio_youth"] +
    df["bio_adult"]
)

df["update_intensity"] = df["total_updates"] / (df["total_enrolment"] + 1)

df["youth_pressure"] = (
    (df["demo_youth"] + df["bio_youth"]) /
    (df["total_updates"] + 1)
)

df["adult_pressure"] = (
    (df["demo_adult"] + df["bio_adult"]) /
    (df["total_updates"] + 1)
)

df["biometric_ratio"] = (
    (df["bio_youth"] + df["bio_adult"]) /
    (df["total_updates"] + 1)
)

# ======================
# NORMALIZATION
# ======================
def normalize(series):
    return (series - series.min()) / (series.max() - series.min() + 1e-6)

# ======================
# UPDATE PRESSURE INDEX
# ======================
df["upi"] = (
    0.4 * normalize(df["update_intensity"]) +
    0.3 * normalize(df["youth_pressure"]) +
    0.2 * normalize(df["biometric_ratio"]) +
    0.1 * normalize(df["adult_pressure"])
)

def pressure_level(x):
    if x >= 0.75:
        return "CRITICAL"
    elif x >= 0.5:
        return "HIGH"
    elif x >= 0.25:
        return "MODERATE"
    else:
        return "LOW"

df["pressure_level"] = df["upi"].apply(pressure_level)

# ======================
# SAVE OUTPUT
# ======================
df.to_csv(OUT_PATH, index=False)

print("====================================")
print("✅ UPDATE PRESSURE INDEX GENERATED")
print("📍 Output:", OUT_PATH)
print("====================================")
