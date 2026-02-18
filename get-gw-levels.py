# Read the list of sites created by "find-sites-output.py" and
# retrieve selected groundwater-level data for those sites, in chunks.

import pandas as pd
from dataretrieval import nwis

# -------------------------------------------------------------------
# User inputs
# -------------------------------------------------------------------

SITES_FILE = "find-sites-output.csv"            # input from find-sites_Pierce.py
OUT_CSV1   = "get-gw-levels-out.csv"           # optional full output
OUT_CSV2   = "get-gw-levels-out-NAVD88.csv"    # filtered output
START_DT   = None                              # e.g., '1900-01-01'
END_DT     = None                              # e.g., '2100-01-01'

# Break up the retrieval into smaller chunks to work around download size.
# Chunk size is the number of sites downloaded in one chunck. 
CHUNK_SIZE = 100   # adjust as needed to keep URL length reasonable

# -------------------------------------------------------------------
# Read site numbers from CSV
# -------------------------------------------------------------------

sites_df = pd.read_csv(SITES_FILE)  # columns: site_no, lat_nad83, lon_nad83
if "site_no" not in sites_df.columns:
    raise ValueError(f"'site_no' column not found in {SITES_FILE}")

sites = sites_df["site_no"].astype(str).tolist()
if not sites:
    raise ValueError(f"No site numbers found in {SITES_FILE}")

# -------------------------------------------------------------------
# Helper: split list into chunks
# -------------------------------------------------------------------

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# -------------------------------------------------------------------
# Helper: ensure a DataFrame has a 'site_no' column
# -------------------------------------------------------------------

def ensure_site_no_col(df):
    # If site_no already present, return as-is
    if "site_no" in df.columns:
        return df
    # Otherwise, treat the index as the site number
    df = df.reset_index()
    first_col = df.columns[0]
    if first_col != "site_no":
        df = df.rename(columns={first_col: "site_no"})
    return df

# -------------------------------------------------------------------
# Loop over chunks and retrieve data
# -------------------------------------------------------------------

gw_frames = []
site_frames = []

for site_chunk in chunks(sites, CHUNK_SIZE):
    # Groundwater levels (parameter 62611 = NAVD88, feet)
    gw_kwargs = {
        "sites": site_chunk,
        "parameterCd": "62611",
    }
    if START_DT is not None:
        gw_kwargs["startDT"] = START_DT
    if END_DT is not None:
        gw_kwargs["endDT"] = END_DT

    gw_result = nwis.get_gwlevels(**gw_kwargs)
    gw_df_chunk = gw_result[0]
    if not gw_df_chunk.empty:
        gw_frames.append(gw_df_chunk)

    # Site info
    site_result = nwis.get_info(sites=site_chunk)
    site_df_chunk = site_result[0]
    if not site_df_chunk.empty:
        site_frames.append(site_df_chunk)

# -------------------------------------------------------------------
# Concatenate chunks and standardize 'site_no'
# -------------------------------------------------------------------

if not gw_frames:
    raise ValueError("No groundwater data returned for any site chunk.")

gw_df = pd.concat(gw_frames, ignore_index=False)
gw_df = ensure_site_no_col(gw_df)

if not site_frames:
    raise ValueError("No site-info data returned for any site chunk.")

site_df = pd.concat(site_frames, ignore_index=False)
site_df = ensure_site_no_col(site_df)

# -------------------------------------------------------------------
# Land-surface elevation and datum
# -------------------------------------------------------------------

# Some site-info records may lack alt_va or alt_datum_cd; select safely
cols_for_elev = [c for c in ["site_no", "alt_va", "alt_datum_cd"] if c in site_df.columns]
if len(cols_for_elev) < 2:
    raise KeyError(
        "Site-info frame is missing 'alt_va' or 'alt_datum_cd' columns needed "
        "for land-surface elevation."
    )

site_elev = (
    site_df[cols_for_elev]
    .rename(
        columns={
            "alt_va": "land_surface_elev",
            "alt_datum_cd": "land_surface_elev_datum",
        }
    )
)

# -------------------------------------------------------------------
# Well depth (if available)
# -------------------------------------------------------------------

well_depth_cols = [
    c for c in site_df.columns
    if "well_depth" in c.lower() or c.lower() == "well_depth_va"
]

if well_depth_cols:
    wd_col = well_depth_cols[0]
    site_well_depth = (
        site_df[["site_no", wd_col]]
        .rename(columns={wd_col: "well_depth"})
    )
else:
    # No well depth in this sitefile; create an empty column
    site_well_depth = pd.DataFrame(
        {
            "site_no": site_df["site_no"],
            "well_depth": pd.NA,
        }
    )

# -------------------------------------------------------------------
# Merge elevation and well depth into groundwater levels
# -------------------------------------------------------------------

out = gw_df.merge(site_elev, on="site_no", how="left")
out = out.merge(site_well_depth, on="site_no", how="left")

# -------------------------------------------------------------------
# OPTIONAL: write full CSV (all parameters returned)
# -------------------------------------------------------------------

# out.to_csv(OUT_CSV1, index=False)
# print(f"Wrote {len(out)} rows to {OUT_CSV1}")

# -------------------------------------------------------------------
# Filter to parameter codes 62611 and 72019, then write final CSV
# -------------------------------------------------------------------

PCODES_TO_KEEP = ["62611", "72019"]

pcode_col = "parm_cd"
if pcode_col not in out.columns:
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
