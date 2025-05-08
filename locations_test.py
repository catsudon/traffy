import pydeck as pdk
import pandas as pd

# Sample data
places_df = pd.DataFrame({
    "lat": [13.7563, 13.7583],
    "lon": [100.5018, 100.5038],
    "name": ["7-eleven", "bus stop"],
    "type": ["7-eleven", "bus stop"],
    "icon_name": ["marker", "marker"]
})

# This uses the built-in sprite
icon_data = {
    "marker": {
        "x": 0,
        "y": 0,
        "width": 128,
        "height": 128,
        "anchorY": 128
    }
}


places_df["icon"] = places_df["icon_name"]

icon_layer = pdk.Layer(
    type="IconLayer",
    data=places_df,
    get_icon="icon",
    get_position=["lon", "lat"],
    get_size=4,
    size_scale=15,
    icon_atlas="https://raw.githubusercontent.com/visgl/deck.gl-data/master/website/icon-atlas.png",
    icon_mapping=icon_data,
    pickable=True
)

view_state = pdk.ViewState(latitude=13.7563, longitude=100.5018, zoom=12)

r = pdk.Deck(
    layers=[icon_layer],
    initial_view_state=view_state,
    tooltip={"text": "{name} ({type})"}
)
r.show()
