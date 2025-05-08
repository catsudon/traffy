import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Step 1: Load your dataset
df = pd.read_csv("data/busStop.csv")  # Replace with your actual file name

# Step 2: Convert to GeoDataFrame
geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")  # WGS84

# Step 3: Load Thailand boundaries and extract Bangkok
thailand = gpd.read_file("gadm41_THA_1.json")
bangkok = thailand[thailand['NAME_1'].str.contains("Bangkok", case=False)]

# Step 4: Spatial filtering — keep only points inside Bangkok
gdf_bangkok = gdf[gdf.within(bangkok.unary_union)]


# Step 5: Save filtered data (optional)
gdf_bangkok.drop(columns="geometry").to_csv("data/bangkok_busStop.csv", index=False)
