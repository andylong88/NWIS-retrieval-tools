# Read the list of sites created by "find-sites-output.py" and retrieve selected data for those sites. 

import pandas as pd
from dataretrieval import nwis

# -------------------------------------------------------------------
# User inputs
# -------------------------------------------------------------------
SITES_FILE = "find-sites-output.csv"          # one USGS site_no per line
OUT_CSV1    = "get-gw-levels-out.csv" # output file
OUT_CSV2    = "get-gw-levels-out-NAVD88.csv" # output file
START_DT   = None  # e.g., '1900-01-01' if you want a date range
END_DT     = None  # e.g., '2100-01-01'

# -------------------------------------------------------------------
# Read site numbers from CSV (with header and extra columns)
# -------------------------------------------------------------------
sites_df = pd.read_csv(SITES_FILE)  # file has columns: site_no, lat_nad83, lon_nad83
if "site_no" not in sites_df.columns:
    raise ValueError(f"'site_no' column not found in {SITES_FILE}")

# Extract the site IDs as a simple Python list
sites = sites_df["site_no"].astype(str).tolist()

if not sites:
    raise ValueError(f"No site numbers found in {SITES_FILE}")

# -------------------------------------------------------------------
# Get groundwater levels for parameter 62611 (NAVD88, feet)
#   Service: gwlevels
#   Parameter: 62611 = groundwater level above NAVD88, feet
# -------------------------------------------------------------------
gw_kwargs = {
    "sites": sites,
    "parameterCd": "62611",
}

if START_DT is not None:
    gw_kwargs["startDT"] = START_DT

if END_DT is not None:
    gw_kwargs["endDT"] = END_DT

gw_result = nwis.get_gwlevels(**gw_kwargs)
gw_df = gw_result[0]  # first element is the DataFrame

# -------------------------------------------------------------------
# Get site info, including land-surface elevation
#   alt_va        = land-surface altitude (elevation)
#   alt_datum_cd  = vertical datum for altitude (e.g., NAVD88)
# -------------------------------------------------------------------
site_result = nwis.get_info(sites=sites)
site_df = site_result[0]

site_elev = (
    site_df[["site_no", "alt_va", "alt_datum_cd"]]
    .rename(columns={
        "alt_va": "land_surface_elev",
        "alt_datum_cd": "land_surface_elev_datum"
    })
)

# -------------------------------------------------------------------
# Merge elevation info into groundwater levels
# -------------------------------------------------------------------
out = gw_df.merge(site_elev, on="site_no", how="left")

# -------------------------------------------------------------------
# OPTIONAL: Write to CSV with NAVD88 and also NGVD29 
# -------------------------------------------------------------------
#out.to_csv(OUT_CSV1, index=False)
#print(f"Wrote {len(out)} rows to {OUT_CSV1}")


# -------------------------------------------------------------------
# Create CSV with only parameter codes 62611 and 72019 (omit NGVD29)
# -------------------------------------------------------------------
PCODES_TO_KEEP = ["62611", "72019"]

# Some gwlevels outputs use 'parm_cd' for the parameter code column.
# Adjust the column name here if your frame uses something different.
pcode_col = "parm_cd"
if pcode_col not in out.columns:
    # Try alternate common names
    for alt in ["parameter_cd", "param_cd", "pcode"]:
        if alt in out.columns:
            pcode_col = alt
            break
    else:
        raise KeyError(
            "Could not find a parameter-code column in groundwater data. "
            "Look at 'out.columns' and update 'pcode_col' accordingly."
        )

out_2 = out[out[pcode_col].isin(PCODES_TO_KEEP)].copy()
out_2.to_csv(OUT_CSV2, index=False)
print(f"Wrote {len(out_2)} rows to {OUT_CSV2}")

