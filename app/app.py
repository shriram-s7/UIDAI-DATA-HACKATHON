import streamlit as st
import pandas as pd
import os
st.set_page_config(
    page_title="Aadhaar Update Pressure Intelligence",
    layout="wide"
)
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT_DIR = os.path.join(ROOT_DIR, "outputs")
UPI_PATH = os.path.join(OUTPUT_DIR, "district_update_pressure_index.csv")
def pretty_district(name):
    """
    Display-safe district name.
    Does NOT modify underlying data.
    """
    if isinstance(name, str):
        return (
            name.replace("?", " / ")
                .replace(";", " / ")
                .replace("–", " - ")
        )
    return name
if "page" not in st.session_state:
    st.session_state.page = "district_view"
@st.cache_data
def load_data():
    if not os.path.exists(UPI_PATH):
        st.error("❌ district_update_pressure_index.csv not found in /outputs")
        st.stop()
    return pd.read_csv(UPI_PATH)

df = load_data()
st.title("🆔 Aadhaar Update Pressure & Data Quality Intelligence System")

st.markdown("""
**Decision-support analytics for UIDAI**

This system identifies **where**, **why**, and **how urgently**
Aadhaar update demand is building — using age-aware, region-wise signals.
""")
col_main, col_action = st.columns([6, 2])

with col_action:
    if st.button("🚨 National Pressure Ranking"):
        st.session_state.page = "ranking_view"
if st.session_state.page == "district_view":

    st.sidebar.header("🔎 Filter Region")

    states = sorted(df["state_normalized"].unique())
    selected_state = st.sidebar.selectbox("Select State", states)

    df_state = df[df["state_normalized"] == selected_state]

    districts = sorted(df_state["district_cleaned"].unique())
    selected_district = st.sidebar.selectbox("Select District", districts)

    row = df_state[df_state["district_cleaned"] == selected_district].iloc[0]

    st.subheader(f"📍 {pretty_district(selected_district)}, {selected_state}")


    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Update Pressure Index (UPI)", f"{row['upi']:.2f}")
    k2.metric("Pressure Level", row["pressure_level"])
    k3.metric("Total Updates", int(row["total_updates"]))
    k4.metric("Total Enrolments", int(row["total_enrolment"]))

    st.subheader("📊 Why is this region under pressure?")

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("**Age-driven pressure**")
        st.json({
            "Youth Update Pressure (5–17)": round(row["youth_pressure"], 2),
            "Adult Update Pressure (18+)": round(row["adult_pressure"], 2)
        })

    with c2:
        st.markdown("**Update composition**")
        st.json({
            "Biometric Update Ratio": round(row["biometric_ratio"], 2),
            "Update-to-Enrolment Ratio": round(row["update_intensity"], 2)
        })

    st.subheader("🛠 UIDAI Recommended Action")

    def recommend(level):
        if level == "CRITICAL":
            return "🚨 Deploy mobile update units, emergency staffing, extended hours"
        if level == "HIGH":
            return "📈 Increase update capacity, youth-focused awareness"
        if level == "MODERATE":
            return "🧭 Monitor trends, optimise scheduling"
        return "✅ Normal operations, promote enrolment"

    st.success(recommend(row["pressure_level"]))

    st.subheader("📄 State-wide District Comparison")
    df_state_display = df_state.copy()
    df_state_display["district_cleaned"] = df_state_display["district_cleaned"].apply(pretty_district)

    st.dataframe(
        df_state_display.sort_values("upi", ascending=False),
        use_container_width=True
    )
elif st.session_state.page == "ranking_view":

    st.subheader("🚨 National Aadhaar Update Pressure Ranking")

    if st.button("⬅ Back to District View"):
        st.session_state.page = "district_view"

    ranked_df = (
        df
        .sort_values("upi", ascending=False)
        .reset_index(drop=True)
    )
    ranked_df["Rank"] = ranked_df.index + 1
    ranked_df["district_display"] = ranked_df["district_cleaned"].apply(pretty_district)


    st.markdown("""
    **Purpose of this view**

    This ranking helps UIDAI administrators quickly identify  
    **where intervention is needed first**, based on update pressure.
    """)

    st.dataframe(
        ranked_df[
            [
                "Rank",
                "state_normalized",
                "district_display",
                "upi",
                "pressure_level",
                "total_updates",
                "total_enrolment"
            ]
        ],
        use_container_width=True,
        height=550
    )
    st.markdown("""
    **How to use**
    - Focus on CRITICAL and HIGH pressure districts
    - Deploy capacity proactively
    - Avoid reactive firefighting
    """)
st.markdown("---")
st.caption("""
⚠️ Advisory analytics only.  
No personal Aadhaar data used.  
Fully compliant with UIDAI governance principles.
""")
