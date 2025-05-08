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

import geopandas as gpd
import json

# Load GADM GeoJSON and filter for Bangkok
gdf = gpd.read_file("data/gadm41_THA_2.json")
bangkok_gdf = gdf[gdf["NAME_1"].str.contains("Bangkok|กรุงเทพ", case=False, na=False)].copy()

# Normalize names to match `merged_df`
bangkok_gdf["district"] = (
    bangkok_gdf["NL_NAME_2"]
    .str.strip()
    .str.replace(" ", "")
    .str.replace(")", "", regex=False)
    .str.replace("บางกะป", "บางกะปิ")
)

# Normalize merged_df names too
merged_df["district"] = merged_df["district"].str.replace(" ", "")

# Merge shapes with complaint/budget data
geo_merged = bangkok_gdf.merge(merged_df, on="district", how="left")

# Normalize budget_per_complaint to 0–255 red intensity
max_val = geo_merged["budget_per_complaint"].quantile(0.95)
geo_merged["norm_val"] = (
    geo_merged["budget_per_complaint"]
    .clip(upper=max_val)
    .fillna(0)
    / max_val
).clip(0, 1)

# Compute red-green spectrum
geo_merged["r"] = (geo_merged["norm_val"] * 255).astype(int)
geo_merged["g"] = ((1 - geo_merged["norm_val"]) * 255).astype(int)

# Convert to GeoJSON
geojson_data = json.loads(geo_merged.to_json())

polygon_layer = pdk.Layer(
    "GeoJsonLayer",
    data=geojson_data,
    pickable=True,
    stroked=True,
    filled=True,
    get_fill_color='[properties.r, properties.g, 0, 180]',
    get_line_color=[0, 0, 0],
    line_width_min_pixels=1,
)

view_state = pdk.ViewState(latitude=13.75, longitude=100.5, zoom=9)

# Display in Streamlit
st.subheader("🗺️ Bangkok District Polygons — Red = More Budget Per Complaint")
st.pydeck_chart(pdk.Deck(
    map_style="mapbox://styles/mapbox/dark-v9",
    initial_view_state=view_state,
    layers=[polygon_layer],
    tooltip={"text": "District: {district}\nComplaints: {complaints}\nBudget: {budget_total}\nBudget per complaint: {budget_per_complaint}"}
))



unmatched = set(traffy_df["district"].unique()) - set(budget_df["district"].unique())
st.write("🧩 Unmatched districts:", unmatched)
