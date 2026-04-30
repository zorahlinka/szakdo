
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime


# 1. KONFIGURÁCIÓK

def get_agg_config():
    base = {
        "EU_osszkoltseg": "sum", "osszes_forras": "sum", "kf_tevekenyseg": "sum",
        "uj_technologia": "sum", "osszterulet": "sum", "beepitett_ter": "sum",
        "vallalkozasok_szama": "sum", "vallalkozasok_foglalkoztatott": "sum",
        "arbevetel": "sum", "exportarany": "mean",
    }
    extra = {
        "sajat_szolg_arany": "mean", "kiszervezett_szolg_arany": "mean",
        "sajat_forras": "sum", "allami_forras": "sum", "onkormanyzati_forras": "sum",
        "EU_forras": "sum", "bankhitel": "sum", "tagi_kolcson": "sum",
        "tokeemeles": "sum", "egyeb_forras": "sum", "fejlesztesi_ugynokseg": "sum",
        "export_ugynokseg": "sum", "kulfoldi_ip": "sum", "nemzetkozi_projekt": "sum",
        "oktatas_felso": "sum", "kutatointezet": "sum", "berbeadott_ter_arany": "mean",
        "eladott_ter_arany": "mean", "kkv_szam": "sum", "nagyvall_szam": "sum",
        "egyeb_vall_szam": "sum"
    }
    return {"small": base, "full": {**base, **extra}}


# 2. SEGÉDFÜGGVÉNYEK

def safe_div(numerator, denominator):
    """
    Safe division that handles division by zero.
    Returns percentage (multiplied by 100).
    """
    with np.errstate(divide='ignore', invalid='ignore'):
        result = np.true_divide(numerator, denominator) * 100

    # Replace inf and nan with 0
    result = np.where(np.isfinite(result), result, 0)
    return pd.Series(result).round(2)

def round_by_structure(df):
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].round(2)
    return df

def apply_filters(df, years=None, filter_col=None, filter_values=None):
    temp_df = df.copy()
    if years: 
        temp_df = temp_df[temp_df["alapadat_ev"].isin(years)]
    if filter_col and filter_values and filter_col in temp_df.columns:
        temp_df = temp_df[temp_df[filter_col].isin(filter_values)]
    
    return temp_df

def log(msg):
    print(f"[REPORT] {msg}")

def generate_report_name(cfg):
    parts = [cfg.get("type", "report")]
    if "metric" in cfg: 
        parts.append(str(cfg["metric"]))
    if "year" in cfg: 
        parts.append(str(cfg["year"]))
    elif "years" in cfg: 
        parts.append(f"{min(cfg['years'])}-{max(cfg['years'])}")
    base_name = "_".join(parts).replace(" ", "_").lower()
    return f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M')}"

def export_to_ods(result, name):
    file_name = f"{name}.ods"

    with pd.ExcelWriter(file_name, engine="odf") as writer:

        # normalize output
        if isinstance(result, dict):
            sheets = result
        else:
            sheets = {name[:31]: result}

        for sheet_name, df in sheets.items():
            safe_name = str(sheet_name)[:31]
            df.to_excel(writer, sheet_name=safe_name, index=True)

    print(f"Mentve: {file_name}")

def run_report(report, df, agg_config):
    name = generate_report_name(report["cfg"])
    
    log(f"Running: {report['cfg']['type']}")

    result = report["func"](df, report["cfg"], agg_config)

    export_to_ods(result, name)

    log(f"Finished: {name}")


# 3. ADATBETÖLTÉS

def load_data(db_path):
    views = ["park_base", "vallalkozasok_latest", "terulet_latest", "infrastrukturafejlesztes_latest", "EU_tamogatas_latest", "kapcsolatok_latest"]
    
    df = pd.read_sql("SELECT * FROM park_base", conn)
    
    for v in views[1:]:
        tmp = pd.read_sql(f"SELECT * FROM {v}", conn)
        tmp = tmp.groupby(["park_ID", "alapadat_ev"], as_index=False).first()
        df = df.merge(tmp, on=["park_ID", "alapadat_ev"], how="left", validate="one_to_one")
            #suffixes=('', '_drop')
            #df = df.loc[:, ~df.columns.str.contains('_drop')]
    
    
    numeric_cols = list(get_agg_config()["full"].keys())
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df
       
# 4. REPORT FÜGGVÉNYEK

def report_emails(df, cfg, mgmt=None):
    f_df = apply_filters(df, filter_col=cfg.get("filter_col"), filter_values=cfg.get("filter_values"))
    
    if mgmt is not None:
        f_df = f_df.merge(mgmt, on="park_ID", how="left")
    
    emails = pd.concat([f_df["park_email"], f_df["management_email"]], ignore_index=True)
    emails = (
        emails.dropna()
              .drop_duplicates()
              .sort_values()
              .reset_index(drop=True)
    )

    # per-park view
    details = (
        f_df[["park_ID", "park_nev", "park_email", "management_email"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    return {
        "email_lista": pd.DataFrame({"Email": emails}),
        "reszletes": details
    }
    
    
    
    #emails = pd.concat([f_df["park_email"].dropna(), f_df["management_email"].dropna()])
    #emails = emails.drop_duplicates().sort_values().reset_index(drop=True)
    #details = f_df[["park_nev", "park_email", "management_email"]].drop_duplicates().reset_index(drop=True)
    #return {"lista": pd.DataFrame({"Email": emails}), "reszletes": details}







def report_missing(df, cfg, agg_map=None):
    year = cfg["year"]

    # Get all unique parks
    all_parks = df[["park_nev", "park_email", "management_email"]].drop_duplicates()

    # Get parks that have data for this year
    parks_with_year = apply_filters(df, years=[year], filter_col=cfg.get("filter_col"), filter_values=cfg.get("filter_values"))
    parks_with_data = parks_with_year[["park_nev"]].drop_duplicates()

    # Parks that DON'T have data for this year
    missing = all_parks[~all_parks["park_nev"].isin(parks_with_data["park_nev"])].reset_index(drop=True)

    # Group by park to consolidate emails
    consolidated = missing.groupby('park_nev').agg({
        'park_email': 'first',  # Take first park email (should be the same for all rows of same park)
        'management_email': lambda x: '; '.join(sorted(x.dropna().unique()))  # Combine all management emails
    }).reset_index()

    return {"hianyzo": consolidated}


"""def report_missing(df, cfg, agg_map=None, mgmt=None):

    year = cfg["year"]

    # all parks (dimension-like)
    all_parks = (
        df[["park_ID", "park_nev", "park_email"]]
        .drop_duplicates()
    )

    # parks with data in given year
    parks_with_data = (
        apply_filters(df, years=[year])
        [["park_ID"]]
        .drop_duplicates()
    )

    # missing parks
    missing = all_parks[
        ~all_parks["park_ID"].isin(parks_with_data["park_ID"])
    ].copy()

    # attach management emails safely
    if mgmt is not None:
        missing = missing.merge(mgmt, on="park_ID", how="left")

    missing = missing.reset_index(drop=True)

    return {
        "hianyzo": missing
    }"""






def report_pivot(df, cfg, agg_map_all):
    agg_dict = agg_map_all[cfg["agg"]]
    df = apply_filters(df, cfg.get("years"), cfg.get("group_col"), cfg.get("filter_values"))
    
    df['alapadat_ev'] = df['alapadat_ev'].astype(int)

    # Handle both single metric and multiple metrics
    if cfg.get("metric"):
        metrics_to_use = [cfg.get("metric")]
    elif cfg.get("metrics"):
        metrics_to_use = cfg.get("metrics")
        if not isinstance(metrics_to_use, list):
            metrics_to_use = [metrics_to_use]
    else:
        metrics_to_use = list(agg_dict.keys())
        
    agg_dict_filtered = {m: agg_dict[m] for m in metrics_to_use if m in agg_dict}

    pivot = df.pivot_table(
        values=metrics_to_use,
        index=cfg["group_col"],
        columns="alapadat_ev",
        aggfunc={m: agg_dict_filtered[m] for m in metrics_to_use},
        fill_value=0
    )
    
    pivot.columns = [f"{metric}_{year}" for metric, year in pivot.columns]
    
    # Convert all columns to numeric
    for col in pivot.columns:
        pivot[col] = pd.to_numeric(pivot[col], errors='coerce')
    
    # éveket a pivotból veszi
    years = sorted({int(str(col).split("_")[-1]) for col in pivot.columns})
    park_counts = df.groupby(cfg["group_col"])['park_ID'].nunique()

#The issue is an index mismatch. pivot is indexed by region, but park_counts is also indexed by region, and they don't align properly during division.
#Use .values to convert both Series to numpy arrays, which bypasses the index alignment issue.
    park_counts = park_counts.reindex(pivot.index).fillna(0)

    for metric in agg_dict_filtered:
        agg_type = agg_dict_filtered[metric]    
            
        for year in years:
            col_name = f"{metric}_{year}"
            per_park_col = f"{metric}_{year}_Per_Park"
            if col_name not in pivot.columns:
                continue
            
            if agg_type == "sum":
                pivot[per_park_col] = (pivot[col_name] / park_counts) if park_counts.sum() != 0 else 0    
            
            elif agg_type == "mean":
                pivot[per_park_col] = pivot[col_name]
            else:
                pivot[per_park_col] = np.nan
            
                        
    # Változás számítása csak akkor, ha legalább 2 év van
    if len(years) >= 2:
        y1, y2 = years[0], years[-1]
        for metric in agg_dict_filtered:
    
            per_park_y1 = f"{metric}_{y1}_Per_Park"
            per_park_y2 = f"{metric}_{y2}_Per_Park"
            pivot[f"{metric}_Változás_per_park"] = pivot[per_park_y2] - pivot[per_park_y1]
            pivot[f"{metric}_Változás_%_per_park"] = pivot[f"{metric}_Változás_per_park"].values / pivot[per_park_y1].replace(0, np.nan).values * 100 
            pivot[f"{metric}_Változás_%_per_park"] = pivot[f"{metric}_Változás_%_per_park"].fillna(0).round(2)
            
          
    return round_by_structure(pivot)

def report_totals(df, cfg, agg_map_all):
    agg_dict = agg_map_all[cfg["agg"]]
    work_df = apply_filters(df, years=cfg.get("years"), filter_col=cfg.get("filter_col"), filter_values=cfg.get("filter_values"))
    
    # Convert year column to int
    work_df['alapadat_ev'] = work_df['alapadat_ev'].astype(int)
    
    # Get years from config or from data
    if cfg.get("years"):
        years = sorted(cfg.get("years"))
    else:
        years = sorted(work_df['alapadat_ev'].unique())
    
    years = [int(y) for y in years]

    counts = work_df.groupby('alapadat_ev')['park_ID'].nunique()

    all_parks_agg = work_df.groupby('alapadat_ev').agg(agg_dict)
    
    # Convert all values to numeric, transpose
    all_parks_agg = all_parks_agg.apply(pd.to_numeric, errors='coerce').transpose()

    out = all_parks_agg


    for y in all_parks_agg.columns:
        cnt = counts.get(y, 0)

        for metric in out.index:
            agg_type = agg_dict.get(metric)
            col_name = y
            avg_col_name = f"{y} Átlag"
            
            if agg_type == "sum":
                out.loc[metric, avg_col_name] = out.loc[metric, col_name] / cnt if cnt != 0 else 0
            elif agg_type == "mean":
                out.loc[metric, avg_col_name] = out.loc[metric, col_name]
            else:
                out.loc[metric, avg_col_name] = np.nan
                
    
    if len(years) >= 2:
        first_year_col = f"{int(years[0])} Átlag"
        last_year_col = f"{int(years[-1])} Átlag"
    
        out["Növekedés"] = out[last_year_col] - out[first_year_col]
        #Use .values to convert Series to numpy arrays for element-wise division
        out["Növekedés (%)"] = (out["Növekedés"].values / out[first_year_col].replace(0, np.nan).values * 100)
        out["Növekedés (%)"] = out["Növekedés (%)"].fillna(0).round(2)
    
    #out["Növekedés"] = out[f"{int(years[-1])} Átlag"] - out[f"{int(years[0])} Átlag"]
    #out["Növekedés (%)"] = ((out[f"{int(years[-1])} Átlag"] * 100 / out[f"{int(years[0])} Átlag"].replace(0, np.nan)) - 100).fillna(0)
    #out["Növekedés (%%)"] = (out["Növekedés"] * 100 / out[f"{int(years[0])} Átlag"].replace(0, np.nan)).fillna(0)
    
    #safe_div() expects Series with the same length, but you're passing a Series and a column from a DataFrame with different indices.
    
    return round_by_structure(out)
           

# 5. MAIN

def main():
    DB_PATH = "IP"
    AGG_CONFIG = get_agg_config()
    with sqlite3.connect(DB_PATH) as conn:
        df = load_data(conn)

        management_df = pd.read_sql("SELECT park_ID, management_email FROM management_latest", conn)
        #management_df = management_df.groupby("park_ID", as_index=False).first()
    conn.close()

    reporting = [
        {"func": report_totals, "cfg": {"type": "totals", "agg": "small", "years": [2021, 2022, 2023], "filter_col": "park_regio", "filter_values": ["Közép-Dunántúl"]}},
        {"func": report_pivot, "cfg": {"type": "novekedes", "group_col": "park_regio", "agg": "small", "years": [2023]}},
        {"func": report_emails, "cfg": {"type": "emails", "filter_col": "park_varmegye", "filter_values": ["Vas"]}},
        {"func": report_missing, "cfg": {"type": "missing", "year": 2024}},
    ]
    for report in reporting:
        try:
            run_report(report, df, AGG_CONFIG)
        #except Exception as e:
            #log(f"ERROR in {report['cfg']['type']}: {e}")
        except Exception as e:
            import traceback
            log(f"ERROR in {report['cfg']['type']}: {e}")
            traceback.print_exc()   

if __name__ == "__main__":
    main()