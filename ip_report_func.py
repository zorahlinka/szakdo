
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
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql("SELECT * FROM park_base", conn)
        for v in views[1:]:
            tmp = pd.read_sql(f"SELECT * FROM {v}", conn)
            df = df.merge(tmp, on=["park_ID", "alapadat_ev"], how="left", suffixes=('', '_drop'))
            df = df.loc[:, ~df.columns.str.contains('_drop')]
    return df


# 4. REPORT FÜGGVÉNYEK

def report_emails(df, cfg, agg_map=None):
    f_df = apply_filters(df, filter_col=cfg.get("filter_col"), filter_values=cfg.get("filter_values"))
    emails = pd.concat([f_df["park_email"].dropna(), f_df["management_email"].dropna()])
    emails = emails.drop_duplicates().sort_values().reset_index(drop=True)
    details = f_df[["park_nev", "park_email", "management_email"]].drop_duplicates().reset_index(drop=True)
    return {"lista": pd.DataFrame({"Email": emails}), "reszletes": details}

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

def report_pivot(df, cfg, agg_map_all):
    agg_dict = agg_map_all[cfg["agg"]]
    df = apply_filters(df, cfg.get("years"), cfg.get("group_col"), cfg.get("filter_values"))
    
    pivot = df.pivot_table(
        values=cfg.get("metric") if cfg.get("metric") else list(agg_dict.keys()), 
        index=cfg["group_col"], 
        columns="alapadat_ev", 
        aggfunc=agg_dict, 
        fill_value=0
    )
    
    pivot.columns = [f"{m}_{y}" for m, y in pivot.columns]

    # éveket a pivotból veszi
    years = sorted({str(col).split("_")[-1] for col in pivot.columns})

    # Add Diff columns
    for i in range(1, len(years)):
        y1, y2 = years[i-1], years[i]
        for metric in agg_dict:
            c1 = f"{metric}_{y1}"
            c2 = f"{metric}_{y2}"
            if c1 in pivot.columns and c2 in pivot.columns:
                pivot[f"{metric}_Diff_{y1}_{y2}"] = pivot[c2] - pivot[c1]

    # Add TOTAL columns
    if len(years) > 2:
        y1, y2 = years[0], years[-1]
        for metric in agg_dict:
            c1 = f"{metric}_{y1}"
            c2 = f"{metric}_{y2}"
            if c1 in pivot.columns and c2 in pivot.columns:
                diff_col = f"{metric}_Diff_{y1}_{y2}"
                if diff_col in pivot.columns:
                    pivot[f"{metric}_Pct_{y1}_{y2}"] = safe_div(
                        pivot[diff_col],
                        pivot[c1]
                    )

    return round_by_structure(pivot)

def report_totals(df, cfg, agg_map_all):
    agg_dict = agg_map_all[cfg["agg"]]
    work_df = apply_filters(df, years=cfg.get("years"))
    
    
    years = sorted(cfg.get("years", work_df['alapadat_ev'].unique()))

    counts = work_df.groupby('alapadat_ev')['park_ID'].nunique()

    all_parks_agg = work_df.groupby('alapadat_ev').agg(agg_dict)
    
    # Convert all values to numeric
    all_parks_agg = all_parks_agg.apply(pd.to_numeric, errors='coerce').transpose()

    out = all_parks_agg

    for y in all_parks_agg.columns:
        cnt = counts[y] if y in counts else 0
        series = all_parks_agg[y]
        avg = []
        for total in series:
            try:
                total_val = float(total) if pd.notna(total) else 0.0
                avg_val = total_val / cnt if cnt != 0 else 0.0
                avg.append(avg_val)
            except (ValueError, TypeError):
                avg.append(0.0)
        out[f"{int(y)} Átlag"] = avg

    out["Növekedés"] = out[f"{int(years[-1])} Átlag"] - out[f"{int(years[0])} Átlag"]
    return round_by_structure(out)
           

# 5. MAIN

def main():
    DB_PATH = "IP"
    AGG_CONFIG = get_agg_config()
    
    df = load_data(DB_PATH)

    reporting = [
        {"func": report_totals, "cfg": {"type": "totals", "agg": "small", "years": [2021, 2022, 2023]}},
        {"func": report_pivot, "cfg": {"type": "novekedes", "group_col": "park_regio", "metric": "arbevetel", "agg": "small", "years": [2021, 2022, 2023]}},
        {"func": report_emails, "cfg": {"type": "emails", "filter_col": "park_regio", "filter_values": ["Észak-Magyarország"]}},
        {"func": report_missing, "cfg": {"type": "missing", "year": 2024}},
    ]
    for report in reporting:
        try:
            run_report(report, df, AGG_CONFIG)
        except Exception as e:
            log(f"ERROR in {report['cfg']['type']}: {e}")
            

if __name__ == "__main__":
    main()