import pandas as pd
import numpy as np
import sqlite3
import os

# =========================
# AGGREGATION CONFIG
# =========================

AGG_BASE = {
    "EU_osszkoltseg": "sum",
    "osszes_forras": "sum",
    "kf_tevekenyseg": "sum",
    "uj_technologia": "sum",
    "osszterulet": "sum",
    "beepitett_ter": "sum",
    "vallalkozasok_szama": "sum",
    "vallalkozasok_foglalkoztatott": "sum",
    "arbevetel": "sum",
    "exportarany": "mean",
}

AGG_EXTRA = {
    "sajat_szolg_arany": "mean",
    "kiszervezett_szolg_arany": "mean",
    "sajat_forras": "sum",
    "allami_forras": "sum",
    "onkormanyzati_forras": "sum",
    "EU_forras": "sum",
    "bankhitel": "sum",
    "tagi_kolcson": "sum",
    "tokeemeles": "sum",
    "egyeb_forras": "sum",
    "fejlesztesi_ugynokseg": "sum",
    "export_ugynokseg": "sum",
    "kulfoldi_ip": "sum",
    "nemzetkozi_projekt": "sum",
    "oktatas_felso": "sum",
    "kutatointezet": "sum",
    "berbeadott_ter_arany": "mean",
    "eladott_ter_arany": "mean",
    "kkv_szam": "sum",
    "nagyvall_szam": "sum",
    "egyeb_vall_szam": "sum"
}

agg_small = AGG_BASE
agg_full = {**AGG_BASE, **AGG_EXTRA}

AGG_MAP = {
    "small": agg_small,
    "full": agg_full
}

# =========================
# HELPERS
# =========================

def safe_div(numerator, denominator):
    with np.errstate(divide='ignore', invalid='ignore'):
        res = (numerator / denominator) * 100
        return pd.Series(res).replace([np.inf, -np.inf], 0).fillna(0)

def get_conn(db):
    if not os.path.exists(db):
        raise FileNotFoundError(f"Adatbázis nem található: {db}")
    return sqlite3.connect(db)

def load_data(db):
    views = [
        "park_base",
        "vallalkozasok_latest",
        "terulet_latest",
        "infrastrukturafejlesztes_latest",
        "EU_tamogatas_latest",
        "kapcsolatok_latest"
    ]

    with get_conn(db) as conn:
        df = pd.read_sql("SELECT * FROM park_base", conn)

        for v in views[1:]:
            tmp = pd.read_sql(f"SELECT * FROM {v}", conn)
            df = df.merge(tmp, on=["park_ID", "alapadat_ev"], how="left")

    return df

def apply_filters(df, years=None, filter_col=None, filter_values=None):
    df = df.copy()

    if years is not None:
        df = df[df["alapadat_ev"].isin(years)]

    if filter_col and filter_values:
        df = df[df[filter_col].isin(filter_values)]

    return df

def aggregate(df, group_cols, agg_dict):
    return df.groupby(group_cols).agg(agg_dict).reset_index()

def round_by_agg(df, agg_dict):
    for col, method in agg_dict.items():
        if col not in df.index and col not in df.columns:
            continue

        if method == "sum":
            df.loc[col] = df.loc[col].round(0)
        else:
            df.loc[col] = df.loc[col].round(2)

    return df

def export_to_ods(file_name, sheets: dict):
    with pd.ExcelWriter(file_name, engine="odf") as writer:
        for name, df in sheets.items():
            safe_name = str(name)[:31]
            df.to_excel(writer, sheet_name=safe_name, index=True)

# =========================
# CORE ENGINE LOGIC
# =========================

def build_pivot_report(
    df,
    group_cols,
    metric=None,
    agg_dict=None,
    years=None,
    add_diff=True
):
    df = apply_filters(df, years)

    base = df.groupby(group_cols + ["alapadat_ev"]).agg(agg_dict).reset_index()

    if metric:
        pivot = base.pivot_table(
            values=metric,
            index=group_cols,
            columns="alapadat_ev",
            fill_value=0
        )
    else:
        pivot = base.pivot_table(
            values=list(agg_dict.keys()),
            index=group_cols,
            columns="alapadat_ev",
            fill_value=0
        )

    pivot = pivot.sort_index(axis=1)

    if not add_diff or pivot.shape[1] < 2:
        return pivot

    first, last = pivot.columns[0], pivot.columns[-1]

    diff = pivot.diff(axis=1).iloc[:, 1:]
    pct = safe_div(diff, pivot.iloc[:, :-1].values)

    result = pd.concat([pivot, diff, pct], axis=1)

    result[f"Total diff ({first}-{last})"] = pivot[last] - pivot[first]
    result[f"Total % ({first}-{last})"] = safe_div(
        pivot[last] - pivot[first], pivot[first]
    )

    return result

# =========================
# REPORT ENGINE
# =========================

class ReportEngine:

    def __init__(self, df):
        self.df = df

    def run(self, cfg):
        t = cfg["type"]

        if t == "single_metric":
            return self._single_metric(cfg)

        elif t == "multi_metric":
            return self._multi_metric(cfg)

        elif t == "entity":
            return self._entity(cfg)

        elif t == "totals":
            return self._totals(cfg)

        else:
            raise ValueError(f"Unknown report type: {t}")

    def _single_metric(self, cfg):
        df = apply_filters(
            self.df,
            cfg.get("years"),
            cfg.get("group_col"),
            cfg.get("filter_values")
        )

        agg_dict = AGG_MAP[cfg["agg"]]

        return build_pivot_report(
            df,
            group_cols=[cfg["group_col"]],
            metric=cfg["metric"],
            agg_dict=agg_dict,
            years=cfg.get("years")
        )

    def _multi_metric(self, cfg):
        df = apply_filters(self.df, cfg.get("years"))
        agg_dict = AGG_MAP[cfg["agg"]]

        base = aggregate(df, [cfg["group_col"], "alapadat_ev"], agg_dict)

        pivot = base.pivot_table(
            values=list(agg_dict.keys()),
            index=cfg["group_col"],
            columns="alapadat_ev",
            fill_value=0
        )

        return pivot.sort_index(axis=1, level=0)

    def _entity(self, cfg):
        df = self.df[self.df[cfg["group_col"]] == cfg["entity"]]
        df = apply_filters(df, cfg.get("years"))

        if df.empty:
            raise ValueError("Nincs adat a megadott szűrésre.")

        agg_dict = AGG_MAP[cfg["agg"]]

        report = df.groupby("alapadat_ev").agg(agg_dict).T

        if report.shape[1] >= 2:
            first, last = report.columns[0], report.columns[-1]
            report["diff"] = report[last] - report[first]
            report["pct"] = safe_div(report["diff"], report[first])

        return round_by_agg(report, agg_dict)

    def _totals(self, cfg):
        df = apply_filters(self.df, cfg.get("years"))
        agg_dict = AGG_MAP[cfg["agg"]]

        totals = df.groupby('alapadat_ev').agg(agg_dict)
        counts = df.groupby('alapadat_ev')['park_ID'].nunique()

        result = {}

        for year in totals.index:
            total_vals = totals.loc[year]
            avg_vals = total_vals / counts[year]

            result[(year, "total")] = total_vals
            result[(year, "per_park")] = avg_vals

        return pd.DataFrame(result)

# =========================
# RUNNER
# =========================

def run_reports(df, reports, output_prefix="report"):
    engine = ReportEngine(df)

    for cfg in reports:
        result = engine.run(cfg)

        file_name = f"{output_prefix}_{cfg['name']}.ods"

        export_to_ods(file_name, {
            cfg["name"]: result
        })

# =========================
# USAGE EXAMPLE
# =========================

if __name__ == "__main__":

    db = "ip.db"
    df = load_data(db)

    REPORTS = [
        {
            "name": "arbevetel_regio",
            "type": "single_metric",
            "group_col": "park_regio",
            "metric": "arbevetel",
            "years": [2022, 2023, 2024],
            "agg": "full"
        },
        {
            "name": "exportarany_varmegye",
            "type": "single_metric",
            "group_col": "park_varmegye",
            "metric": "exportarany",
            "years": [2023, 2024],
            "agg": "small"
        },
        {
            "name": "multi_regio",
            "type": "multi_metric",
            "group_col": "park_regio",
            "years": [2024],
            "agg": "small"
        },
        {
            "name": "total_summary",
            "type": "totals",
            "years": [2022, 2023, 2024],
            "agg": "full"
        }
    ]

    run_reports(df, REPORTS)