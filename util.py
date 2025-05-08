from math import radians, sin, cos, sqrt, atan2

def haversine(lat1, lon1, lat2, lon2):
    # Radius of the Earth in kilometers
    R = 6371.0  

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Differences in coordinates
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    # Distance in kilometers
    distance = R * c
    return distance

# Example: Siam BTS Station (13.7456, 100.5346) to Asok BTS Station (13.7362, 100.5601)
lat1, lon1 = 13.7456, 100.5346  # Siam BTS
lat2, lon2 = 13.7362, 100.5601  # Asok BTS

distance_km = haversine(lat1, lon1, lat2, lon2)
print(f"Distance: {distance_km:.2f} km")
