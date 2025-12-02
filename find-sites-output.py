# Create a list of USGS site IDs and location coordinates for sites within a rectangular polygon.

import requests
import shapely.geometry as geom

# ---- USER INPUTS ----
polygon_coords = [
    (-120.5, 38.0),
    (-120.0, 38.0),
    (-120.0, 38.5),
    (-120.5, 38.5),
]

output_file = "find-sites-output.txt"

site_type = ""        # e.g. "GW" or "ST"; leave "" for all
agency_code = "USGS"

# ---- BUILD POLYGON AND BBOX ----
poly = geom.Polygon(polygon_coords)

lons = [p[0] for p in polygon_coords]
lats = [p[1] for p in polygon_coords]
bBox = f"{min(lons)},{min(lats)},{max(lons)},{max(lats)}"

# ---- CALL USGS SITE SERVICE ----
base_url = "https://waterservices.usgs.gov/nwis/site/"

params = {
    "format": "rdb",          # tab‑delimited legacy format [web:7]
    "bBox": bBox,             # west,south,east,north [web:7]
    "siteType": "GW",    # optional filter (ST = stream)
#    "agencyCd": agency_code,  # optional filter
    "hasDataTypeCd": "gw"    # only sites with data (all = all data types)
}

resp = requests.get(base_url, params=params, timeout=60)
resp.raise_for_status()
text = resp.text

# ---- PARSE RDB AND FILTER BY POLYGON ----
sites = []  # list of (site_no, lat, lon)

header_found = False
col_index = {}

for line in text.splitlines():
    if not line or line.startswith("#"):
        continue

    if not header_found:
        cols = line.split("\t")
        col_index = {name: i for i, name in enumerate(cols)}
        header_found = True
        continue

    # skip format/definition row that follows header
    if line.startswith("agency_cd") or line.startswith("5s"):
        continue

    parts = line.split("\t")
    try:
        site_no = parts[col_index["site_no"]]
        lat = float(parts[col_index["dec_lat_va"]])
        lon = float(parts[col_index["dec_long_va"]])
        datum = parts[col_index.get("dec_coord_datum_cd", "")] if len(parts) > col_index.get("dec_coord_datum_cd", -1) >= 0 else ""
    except (KeyError, IndexError, ValueError):
        continue

    # Only keep NAD83 (or blank, if you want to be strict you can check == "NAD83")
    if datum and datum != "NAD83":
        continue  # skip non‑NAD83 coordinates if desired [web:7][web:128]

    point = geom.Point(lon, lat)
    if not poly.contains(point):
        continue

    sites.append((site_no, lat, lon))

# ---- WRITE OUTPUT FILE ----
# One site per line: site_no,lat,lon
with open(output_file, "w", encoding="utf-8") as f:
    f.write("site_no,lat_nad83,lon_nad83\n")
    for site_no, lat, lon in sorted(set(sites)):
        f.write(f"{site_no},{lat},{lon}\n")

print(f"Wrote {len(set(sites))} sites with NAD83 coordinates to {output_file}")

