python
import pandas as pd
import sqlite3
import os
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
    res = numerator.div(denominator.replace(0, float('nan'))) * 100
    return res.fillna(0).round(2)

def round_by_structure(df):
    df = df.copy()
    for col in df.columns:
        col_str = str(col)

        if "_Avg_" in col_str:
            df[col] = df[col].round(2)
        elif "_Pct_" in col_str:
            df[col] = df[col].round(2)
        elif "_Diff_" in col_str:
            df[col] = df[col].round(0)
        else:
            df[col] = df[col].round(0)
        
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
            df.to_excel(writer, sheet_name=safe_name, index=False)

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
    emails = pd.concat([f_df["park_email"].dropna(), f_df["management_email"].dropna()]).drop_duplicates().sort_values()
    return {"lista": pd.DataFrame({"Email": emails}), "reszletes": f_df[["park_nev", "park_email", "management_email"]].drop_duplicates()}

def report_missing(df, cfg, agg_map=None):
    work_df = apply_filters(df, years=[cfg["year"]], filter_col=cfg.get("filter_col"), filter_values=cfg.get("filter_values"))
    missing = work_df[(work_df["arbevetel"].isna()) | (work_df["arbevetel"] == 0)].copy()
    return {"hianyzo": missing[["park_nev", "park_email", "management_email"]].drop_duplicates()}

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
    if len(years) >= 2:
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
    """Összesített riport évek közötti átlag-különbséggel és Total változással."""
    agg_dict = agg_map_all[cfg["agg"]]
    work_df = apply_filters(df, years=cfg.get("years"))
    
    totals = work_df.groupby('alapadat_ev').agg(agg_dict)
    counts = work_df.groupby('alapadat_ev')['park_ID'].nunique()
    
    years = sorted(totals.index)
    
    # Sum és Avg kiszámítása
    result = {}
    for y in years:
        for metric in agg_dict:
            result[f"{metric}_Sum_{y}"] = totals.loc[y, metric]
            result[f"{metric}_Avg_{y}"] = totals.loc[y, metric] / counts[y]
        
    final_df = pd.DataFrame(result)
    
    # Évek közötti különbségek (Avg alapján, azaz per park)
    for i in range(1, len(years)):
        y1, y2 = years[i-1], years[i]
        for metric in agg_dict:
            c1 = f"{metric}_Avg_{y1}"
            c2 = f"{metric}_Avg_{y2}"
            final_df[f"{metric}_Diff_{y1}_{y2}"] = final_df[c2] - final_df[c1]
        
    # TOTAL különbség (első és utolsó év között, Avg alapján)
    if len(years) >= 2:
        y1, y2 = years[0], years[-1]
        for metric in agg_dict:
            diff_col = f"{metric}_Diff_{y1}_{y2}"
            base_col = f"{metric}_Avg_{y1}"

            final_df[f"{metric}_Pct_{y1}_{y2}"] = safe_div(
                final_df[diff_col],
                final_df[base_col]
            )

    return round_by_structure(final_df)
        

# 5. MAIN

def main():
    DB_PATH = "ip.db"
    AGG_CONFIG = get_agg_config()
    
    df = load_data(DB_PATH)

    reporting = [
        {"func": report_totals, "cfg": {"type": "totals", "agg": "small", "years": [2022, 2023, 2024]}},
        {"func": report_pivot, "cfg": {"type": "novekedes", "group_col": "park_regio", "metric": "arbevetel", "agg": "full", "years": [2022, 2023, 2024]}},
        {"func": report_emails, "cfg": {"type": "emails", "filter_col": "park_regio", "filter_values": ["Közép-Magyarország"]}},
        {"func": report_missing, "cfg": {"type": "missing", "year": 2024}},
    ]
    for report in reporting:
        try:
            run_report(report, df, AGG_CONFIG)
        except Exception as e:
            log(f"ERROR in {report['cfg']['type']}: {e}")
            

if __name__ == "__main__":
    main()