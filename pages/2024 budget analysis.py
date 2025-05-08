import streamlit as st
import pandas as pd
import sqlite3
import pydeck as pdk
from datetime import datetime

st.title("📊 2024 Bangkok District: Budget vs Complaints")

# --- Load budget from SQLite ---
conn_budget = sqlite3.connect("data/budgets.db")
budget_df = pd.read_sql("SELECT * FROM budgets", conn_budget)
conn_budget.close()

budget_df.rename(columns={"เขต": "district"}, inplace=True)
budget_df["district"] = budget_df["district"].str.replace("สำนักงานเขต", "", regex=False).str.strip()

# --- Load Traffy reports ---
conn_traffy = sqlite3.connect("data/traffy.db")
traffy_df = pd.read_sql("SELECT * FROM traffy", conn_traffy)
conn_traffy.close()

# Parse timestamps and keep only 2024
traffy_df["timestamp"] = pd.to_datetime(traffy_df["timestamp"], errors='coerce')
traffy_df = traffy_df[traffy_df["timestamp"].dt.year == 2024]
print(len(traffy_df), "Traffy records loaded")
traffy_df["district"] = traffy_df["district"].fillna("ไม่ระบุ").str.strip()
print(len(traffy_df[traffy_df["district"] == "ไม่ระบุ"]), "Districts not specified")

# --- Month slider ---
st.sidebar.subheader("📅 Filter by Month")
min_month = traffy_df["timestamp"].min().to_pydatetime().replace(day=1)
max_month = traffy_df["timestamp"].max().to_pydatetime().replace(day=1)


start_month, end_month = st.sidebar.slider(
    "Select Month Range",
    min_value=min_month,
    max_value=max_month,
    value=(min_month, max_month),
    format="MMM YYYY"
)


# --- Filter data and count complaints ---
start = pd.to_datetime(start_month).replace(day=1)
end = (pd.to_datetime(end_month) + pd.offsets.MonthEnd(0))  # last day of the end month

filtered_df = traffy_df[
    (traffy_df["timestamp"] >= start) &
    (traffy_df["timestamp"] <= end)
]
complaints_count = filtered_df.groupby("district").size().reset_index(name="complaints")

# --- Merge with budget ---
merged_df = pd.merge(budget_df, complaints_count, on="district", how="left")
merged_df["complaints"] = merged_df["complaints"].fillna(0).astype(int)
merged_df["budget_total"] = merged_df["รวมงบประมาณทั้งสิ้น"]
merged_df["budget_per_complaint"] = merged_df["budget_total"] / merged_df["complaints"].replace(0, 1)

# --- Display table ---
st.subheader("📑 Table View")
st.dataframe(
    merged_df[["district", "budget_total", "complaints", "budget_per_complaint"]]
    .sort_values("budget_per_complaint", ascending=False),
    use_container_width=True
)

# --- Placeholder map ---
st.subheader("🗺️ Map View (Placeholder Coordinates)")
merged_df["lon"] = 100.5  # TODO: Replace with real coordinates
merged_df["lat"] = 13.75

layer = pdk.Layer(
    "ScatterplotLayer",
    merged_df,
    get_position='[lon, lat]',
    get_radius=30000,
    get_fill_color='[255 - complaints, 100, complaints * 2, 160]',
    pickable=True
)

view_state = pdk.ViewState(latitude=13.75, longitude=100.5, zoom=9)

st.pydeck_chart(pdk.Deck(
    map_style='mapbox://styles/mapbox/dark-v9',
    initial_view_state=view_state,
    layers=[layer],
    tooltip={"text": "District: {district}\nComplaints: {complaints}\nBudget: {budget_total:,.0f}"}
))


unmatched = set(traffy_df["district"].unique()) - set(budget_df["district"].unique())
st.write("🧩 Unmatched districts:", unmatched)
