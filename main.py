import streamlit as st
import pandas as pd
import pydeck as pdk
from datetime import timedelta, date
from sklearn.cluster import DBSCAN, KMeans
import numpy as np
import random
import sqlite3

st.set_page_config(layout="wide")

def is_valid_coords(val):
    try:
        lon, lat = val.split(",")
        float(lon)
        float(lat)
        return True
    except:
        return False

@st.cache_data
def load_data():
    conn = sqlite3.connect("data/traffy.db")
    df = pd.read_sql_query("SELECT * FROM traffy", conn)
    conn.close()
    
    df = df[df['coords'].apply(is_valid_coords)].copy()
    df[['lon', 'lat']] = df['coords'].str.split(",", expand=True).astype(float)
    df['type'] = df['type'].astype(str).str.strip('{}').str.strip()
    df['type_main'] = df['type'].str.split(',').str[0].str.strip()
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', errors='coerce')
    df = df.dropna(subset=['timestamp'])
    df["radius"] = df["count_reopen"].fillna(0).astype(float).apply(lambda x: 10 + x)
    return df

@st.cache_data
def load_places():
    places_df = pd.read_csv("data/bangkok_locations.csv")
    return places_df

@st.cache_data
def run_dbscan(latlon_array, eps_km, min_samples):
    coords_rad = np.radians(latlon_array)
    eps_rad = eps_km / 6371.0
    model = DBSCAN(eps=eps_rad, min_samples=min_samples, metric='haversine')
    return model.fit_predict(coords_rad)

@st.cache_data
def run_kmeans(latlon_array, n_clusters):
    model = KMeans(n_clusters=n_clusters, random_state=42)
    return model.fit_predict(latlon_array)

df = load_data()


st.sidebar.header("Filter Options")
type_options = sorted(df["type_main"].dropna().unique())
selected_types = st.sidebar.multiselect("Select Problem Types", type_options, default=["ถนน"])

min_date = df["timestamp"].min().date()
max_date = df["timestamp"].max().date()
default_start = max(min_date, max_date - timedelta(days=180))
start_date, end_date = st.sidebar.date_input("Date Range", [default_start, max_date], min_value=min_date, max_value=max_date)

filtered_df = df[
    df['type_main'].isin(selected_types) &
    (df['timestamp'].dt.date.between(start_date, end_date))
].copy()

st.sidebar.markdown("---")
st.sidebar.subheader("Clustering Options")
enable_clustering = st.sidebar.checkbox("Enable Clustering")
cluster_algo = st.sidebar.selectbox("Clustering Algorithm", ["DBSCAN", "KMeans"])

if enable_clustering and not filtered_df.empty:
    sampling_frac = st.sidebar.slider("Sampling Fraction", min_value=0.01, max_value=1.0, step=0.01, value=0.3)
    sample_df = filtered_df.sample(frac=sampling_frac, random_state=42).copy()
    coords = sample_df[["lat", "lon"]].to_numpy()

    if cluster_algo == "DBSCAN":
        eps_km = st.sidebar.slider("eps (km)", 0.1, 5.0, 1.0, step=0.1)
        min_samples = st.sidebar.slider("Min Points per Cluster", 1, 20, 5)
        cluster_labels = run_dbscan(coords, eps_km, min_samples)
    else:
        k = st.sidebar.slider("Number of Clusters (k)", 1, 20, 5)
        cluster_labels = run_kmeans(coords, k)

    sample_df['cluster'] = cluster_labels
    filtered_df = filtered_df.merge(sample_df[['ticket_id', 'cluster']], on='ticket_id', how='left')
    filtered_df['cluster'] = filtered_df['cluster'].fillna(-1).astype(int)

    unique_clusters = filtered_df['cluster'].unique()
    cluster_colors = {
        cid: [random.randint(50, 255), random.randint(50, 255), random.randint(50, 255), 160]
        for cid in unique_clusters if cid != -1
    }
    cluster_colors[-1] = [120, 120, 120, 50]
    filtered_df['cluster_color'] = filtered_df['cluster'].map(cluster_colors)
else:
    filtered_df['cluster_color'] = None

# map_layer_type = st.sidebar.radio('Map Type', ["ScatterplotLayer", "HeatmapLayer"])
map_style = st.sidebar.radio("Map Style", ["dark", "light", "streets", "satellite"])
map_style = f"mapbox://styles/mapbox/{map_style}-v9"
opacity = st.sidebar.slider('Opacity', 0.0, 1.0, 0.5)
radius_scale = st.sidebar.slider('Radius Scale', 1.0, 10.0, 5.0)
st.sidebar.markdown("---")
st.sidebar.subheader("Place Types to Show")
place_type_options = ["7-Eleven", "RapidTransitStation", "BusStop", "place_of_worship"]
# Initialize session state if not present
if "selected_place_types" not in st.session_state:
    st.session_state.selected_place_types = []

# Sidebar UI
col1, col2 = st.sidebar.columns([1, 1])
with col1:
    if st.button("Select All"):
        st.session_state.selected_place_types = place_type_options.copy()
with col2:
    if st.button("Clear All"):
        st.session_state.selected_place_types = []

# Multiselect without a conflicting default
selected_place_types = st.sidebar.multiselect(
    "Select Place Types",
    place_type_options,
    key="selected_place_types"
)


places_df = load_places()
places_df = places_df[places_df["type"].isin(selected_place_types)].copy()

icon_url_map = {
    "7-Eleven": "https://img.icons8.com/color/48/000000/shop.png",
    "RapidTransitStation": "https://img.icons8.com/color/48/000000/train.png",
    "BusStop": "https://img.icons8.com/color/48/000000/bus.png",
    "place_of_worship": "https://img.icons8.com/color/48/000000/church.png"
}

places_df["icon_data"] = places_df["type"].map(icon_url_map)
places_df["icon"] = places_df["icon_data"].apply(lambda url: {
    "url": url,
    "width": 48,
    "height": 48,
    "anchorY": 48
})


def build_tooltip(row):
    tooltip = f"""
    <div style="max-width: 300px; z-index: 9999;">
        <b>Type:</b> {row['type_main']}<br/>
        <b>Comment:</b> {row['comment'] or ''}<br/>
    """
    if pd.notna(row['photo']):
        tooltip += f'<img src="{row["photo"]}" style="width:100%; height:auto; margin-top:5px;" />'
    tooltip += "</div>"
    return tooltip


filtered_df["tooltip_text"] = filtered_df.apply(build_tooltip, axis=1)


places_df["tooltip_text"] = (
    "<b>Name:</b> " + places_df["name"] +
    "<br/><b>Type:</b> " + places_df["type"]
)


color_mapping = {
    "Reds": [255, 0, 0, 140],
    "Blues": [0, 0, 255, 140],
    "Greens": [0, 255, 0, 140],
    "Purples": [128, 0, 128, 140],
    "Oranges": [255, 165, 0, 140],
    "YlGn": [255, 255, 0, 140],
    "YlOrBr": [255, 215, 0, 140],
}
default_color = color_mapping["YlOrBr"]

def map_zoom_scale(view_zoom):
    # Adjust multiplier to suit how sensitive you want icon scaling
    return view_zoom * 1.2

def create_scatter_map(dataframe, places_df=None):
    if dataframe.empty:
        return None

    layers = []

    layers.append(pdk.Layer(
        "ScatterplotLayer",
        dataframe,
        get_position=["lon", "lat"],
        get_color="cluster_color" if enable_clustering else default_color,
        get_radius="radius",
        radius_scale=radius_scale,
        opacity=opacity,
        pickable=True
    ))

    if places_df is not None and not places_df.empty:
        layers.append(pdk.Layer(
            "IconLayer",
            data=places_df,
            get_icon="icon",
            get_size=1,
            size_scale=map_zoom_scale(view_zoom=10),
            get_position=["lon", "lat"],
            pickable=True
        ))

    view_state = pdk.ViewState(
        longitude=dataframe["lon"].mean(),
        latitude=dataframe["lat"].mean(),
        zoom=10
    )

    tooltip = {
        "html": "{tooltip_text}",
        "style": {
            "backgroundColor": "white",
            "color": "black",
            "fontSize": "12px",
            "padding": "10px",
            "maxWidth": "300px"
        }
    }

    return pdk.Deck(layers=layers, initial_view_state=view_state, map_style=map_style, tooltip=tooltip)


def create_heatmap_map(dataframe, places_df=None):
    if dataframe.empty:
        return None

    layers = []

    layers.append(pdk.Layer(
        "HeatmapLayer",
        dataframe,
        get_position=["lon", "lat"],
        opacity=opacity
    ))

    if places_df is not None and not places_df.empty:
        layers.append(pdk.Layer(
            "IconLayer",
            data=places_df,
            get_icon="icon",
            get_size=1,
            size_scale=map_zoom_scale(view_zoom=10),
            get_position=["lon", "lat"],
            pickable=True
        ))

    view_state = pdk.ViewState(
        longitude=dataframe["lon"].mean(),
        latitude=dataframe["lat"].mean(),
        zoom=10
    )

    tooltip = {
        "html": "{tooltip_text}",
        "style": {
            "backgroundColor": "white",
            "color": "black",
            "fontSize": "12px",
            "padding": "10px",
            "maxWidth": "300px"
        }
    }

    return pdk.Deck(layers=layers, initial_view_state=view_state, map_style=map_style, tooltip=tooltip)




st.title("📍 Traffy Bangkok Complaints Map + Clustering")
st.markdown("Filter by type and date. Choose clustering algorithm and visualize spatial patterns.")

if filtered_df.empty:
    st.warning("No data matches your filters.")
else:
    if enable_clustering:
        st.subheader("🔥 Clustered Heatmap View")
        st.pydeck_chart(create_heatmap_map(filtered_df, places_df), use_container_width=True)
        
        st.subheader("🟢 Clustered Scatterplot View")
        st.pydeck_chart(create_scatter_map(filtered_df, places_df), use_container_width=True)
    else:
        st.subheader("🗺️ General View")
        st.pydeck_chart(create_scatter_map(filtered_df, places_df), use_container_width=True)





