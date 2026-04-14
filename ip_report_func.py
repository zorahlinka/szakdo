python
import pandas as pd
import numpy as np
import sqlite3
import os
from datetime import datetime

# =========================
# 1. KONFIGURÁCIÓK
# =========================
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

# =========================
# 2. SEGÉDFÜGGVÉNYEK
# =========================
def safe_div(numerator, denominator):
    with np.errstate(divide='ignore', invalid='ignore'):
        res = (numerator / denominator) * 100
    return pd.Series(res).replace([np.inf, -np.inf], 0).fillna(0).round(2)

def round_by_logic(df, agg_map):
    df = df.copy()
    for col in df.columns:
        col_str = str(col)
        base_col = next((k for k in agg_map.keys() if k in col_str), None)
        if "Pct" in col_str or (base_col and agg_map.get(base_col) == "mean") or "Avg" in col_str:
            df[col] = df[col].round(2)
        else:
            df[col] = df[col].round(0)
    return df

def apply_filters(df, years=None, filter_col=None, filter_values=None):
    temp_df = df.copy()
    if years: temp_df = temp_df[temp_df["alapadat_ev"].isin(years)]
    if filter_col and filter_values and filter_col in temp_df.columns:
        temp_df = temp_df[temp_df[filter_col].isin(filter_values)]
    return temp_df

def generate_report_name(cfg):
    parts = [cfg.get("type", "report")]
    if "metric" in cfg: parts.append(str(cfg["metric"]))
    if "year" in cfg: parts.append(str(cfg["year"]))
    elif "years" in cfg: parts.append(f"{min(cfg['years'])}-{max(cfg['years'])}")
    base_name = "_".join(parts).replace(" ", "_").lower()
    return f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M')}"

# =========================
# 3. ADATKEZELÉS
# =========================
def load_data(db_path):
    views = ["park_base", "vallalkozasok_latest", "terulet_latest", "infrastrukturafejlesztes_latest", "EU_tamogatas_latest", "kapcsolatok_latest"]
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql("SELECT * FROM park_base", conn)
        for v in views[1:]:
            tmp = pd.read_sql(f"SELECT * FROM {v}", conn)
            df = df.merge(tmp, on=["park_ID", "alapadat_ev"], how="left", suffixes=('', '_drop'))
            df = df.loc[:, ~df.columns.str.contains('_drop')]
    return df

# =========================
# 4. RIPORT LOGIKÁK
# =========================
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
    pivot = df.pivot_table(values=cfg.get("metric") if cfg.get("metric") else list(agg_dict.keys()), index=cfg["group_col"], columns="alapadat_ev", aggfunc=agg_dict, fill_value=0).sort_index(axis=1)
    years = pivot.columns.unique()
    if len(years) < 2: return round_by_logic(pivot, agg_dict)
    
    res_list = []
    for i, y in enumerate(years):
        res_list.append(pivot[[y]])
        if i > 0:
            diff = (pivot[y] - pivot[years[i-1]]).to_frame(f"Diff_{years[i-1]}_{y}")
            res_list.append(diff)
    
    final = pd.concat(res_list, axis=1)
    final[f"TOTAL_Diff_{years[0]}_{years[-1]}"] = pivot[years[-1]] - pivot[years[0]]
    final[f"TOTAL_Pct_{years[0]}_{years[-1]}"] = safe_div(pivot[years[-1]] - pivot[years[0]], pivot[years[0]])
    return round_by_logic(final, agg_dict)

def report_totals(df, cfg, agg_map_all):
    """Összesített riport évek közötti átlag-különbséggel és Total változással."""
    agg_dict = agg_map_all[cfg["agg"]]
    work_df = apply_filters(df, years=cfg.get("years"))
    
    totals = work_df.groupby('alapadat_ev').agg(agg_dict)
    counts = work_df.groupby('alapadat_ev')['park_ID'].nunique()
    years = totals.index.unique()
    
    # Alapadatok (Sum és Avg) kiszámítása
    data = {}
    for y in years:
        data[(y, "Sum")] = totals.loc[y]
        data[(y, "Avg")] = totals.loc[y] / counts[y]
    
    final_df = pd.DataFrame(data)
    
    # Évek közötti különbségek (Avg alapján)
    for i in range(1, len(years)):
        curr, prev = years[i], years[i-1]
        final_df[(f"Diff_{prev}_{curr}", "Avg_Delta")] = final_df[(curr, "Avg")] - final_df[(prev, "Avg")]

    # TOTAL különbség (első és utolsó év között, Avg alapján)
    if len(years) >= 2:
        first, last = years[0], years[-1]
        final_df[("TOTAL", "Diff_Avg")] = final_df[(last, "Avg")] - final_df[(first, "Avg")]
        final_df[("TOTAL", "Pct_Avg")] = safe_div(final_df[("TOTAL", "Diff_Avg")], final_df[(first, "Avg")])
    
    return round_by_logic(final_df.sort_index(axis=1), agg_dict)

# =========================
# 5. EXPORT & RUN
# =========================
def export_to_ods(result, name):
    with pd.ExcelWriter(f"{name}.ods", engine="odf") as writer:
        sheets = result if isinstance(result, dict) else {name[:31]: result}
        for s_name, data in sheets.items(): data.to_excel(writer, sheet_name=str(s_name)[:31])
    print(f"Mentve: {name}.ods")

def main():
    DB_PATH = "ip.db"
    AGG_CONFIG = get_agg_config()
    try:
        df = load_data(DB_PATH)
        JOBS = [
            {"func": report_totals, "cfg": {"type": "totals", "agg": "small", "years": [2022, 2023, 2024]}},
            {"func": report_pivot, "cfg": {"type": "evolucio", "group_col": "park_regio", "metric": "arbevetel", "agg": "full", "years": [2022, 2023, 2024]}}
            {"func": report_emails, "cfg": {"type": "emails", "filter_col": "park_regio", "filter_values": ["Közép-Magyarország"]}},
            {"func": report_missing, "cfg": {"type": "missing", "year": 2024}},
        ]
        for job in JOBS:
            res = job["func"](df, job["cfg"], AGG_CONFIG)
            export_to_ods(res, generate_report_name(job["cfg"]))
    except Exception as e: print(f"Hiba: {e}")

if __name__ == "__main__":
    main()