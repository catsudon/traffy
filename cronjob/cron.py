import requests
import pandas as pd
from datetime import datetime, timedelta

def get_hour_range():
    now = datetime.now()
    end = now.replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    return start.isoformat(), end.isoformat()

def fetch_data(start, end):
    url = f"https://publicapi.traffy.in.th/teamchadchart-stat-api/geojson/v1?start={start}&end={end}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def clean_data(raw_json):
    features = raw_json.get("features", [])

    records = []
    for f in features:
        props = f.get("properties", {})
        coords = f.get("geometry", {}).get("coordinates", [None, None])
        props["lon"] = coords[0]
        props["lat"] = coords[1]
        records.append(props)

    df = pd.DataFrame(records)

    # Fill missing values
    df['comment'] = df.get('description', '').fillna('')
    df['type'] = df.get('type', 'unknown').fillna('unknown')
    # df['organization'] = df.get('organization', 'unknown').fillna('unknown')
    df['district'] = df.get('district', 'unknown').fillna('unknown')
    df['subdistrict'] = df.get('subdistrict', 'unknown').fillna('unknown')
    df['address'] = df.get('address', 'unknown').fillna('unknown')
    

    # Drop rows where both lat/lon are missing
    df['valid_coords'] = df[['lat', 'lon']].notna().all(axis=1)
    df = df[df['valid_coords']].reset_index(drop=True)

    # Drop missing or empty type
    df = df[~(df['type'].astype(str).str.strip() == '{}')].reset_index(drop=True)

    return df

def remap_df(df):
    df['photo'] = df.get('photo_url', '')
    df['photo_after'] = df.get('after_photo', '')
    df['coords'] = df.apply(lambda row: f"{row['lon']},{row['lat']}" if pd.notna(row['lon']) and pd.notna(row['lat']) else '', axis=1)
    df['type_clean'] = df['problem_type_fondue'].apply(lambda x: eval(x) if isinstance(x, str) else x)
    df['type_clean'] = df['type_clean'].apply(format_type_clean)

    # บางที org เป็น str list เช่น "['A', 'B']" → ต้อง eval
    def safe_first(x):
        try:
            return eval(x)[0] if isinstance(x, str) else (x[0] if isinstance(x, list) else 'unknown')
        except:
            return 'unknown'

    df['organization'] = df['org'].apply(safe_first)

    # สร้าง DataFrame ใหม่แบบคอลัมน์เรียงตามต้องการ
    final_df = df[[
        'ticket_id', 'type', 'organization', 'comment', 'photo', 'photo_after', 'coords',
        'address', 'subdistrict', 'district', 'province', 'timestamp', 'state', 'star',
        'count_reopen', 'last_activity', 'valid_coords', 'lat', 'lon', 'type_clean'
    ]].copy()

    return final_df



def save_to_csv(df, start):
    fname = f"data/traffy_{start[:13].replace(':','')}.csv"
    df.to_csv(fname, index=False)
    print(f"Saved {len(df)} rows to {fname}")

import sqlite3

def format_type_clean(x):
    if isinstance(x, str):
        try:
            x = eval(x)
        except:
            return "{}"
    if isinstance(x, list):
        return "{" + ", ".join(str(i) for i in x) + "}"
    return "{}"


def save_to_sqlite(df, db_path="data/traffy.db"):
    conn = sqlite3.connect(db_path)
    df.to_sql("traffy", conn, if_exists="append", index=False,
              dtype={
                  "ticket_id": "TEXT",
                  "type": "TEXT",
                  "organization": "TEXT",
                  "comment": "TEXT",
                  "photo": "TEXT",
                  "photo_after": "TEXT",
                  "coords": "TEXT",
                  "address": "TEXT",
                  "subdistrict": "TEXT",
                  "district": "TEXT",
                  "province": "TEXT",
                  "timestamp": "TEXT",
                  "state": "TEXT",
                  "star": "INTEGER",
                  "count_reopen": "INTEGER",
                  "last_activity": "TEXT",
                  "valid_coords": "BOOLEAN",
                  "lat": "REAL",
                  "lon": "REAL",
                  "type_clean": "TEXT"
              })
    
    # Remove duplicates by ticket_id (keep latest)
    conn.execute("""
        DELETE FROM traffy
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM traffy
            GROUP BY ticket_id
        )
    """)
    conn.commit()
    conn.close()
    print(f"Appended {len(df)} rows to traffy.db")


if __name__ == "__main__":
    start, end = get_hour_range()
    print(f"Fetching data from {start} to {end}")
    raw = fetch_data(start, end)
    df = clean_data(raw)
    df = remap_df(df)
    save_to_csv(df, start)
    save_to_sqlite(df)
