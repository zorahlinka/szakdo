
import pandas as pd
import sqlite3
import os
import argparse
import traceback
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

def round(df):
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
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

#Fájlnév generálás a konfig alapján (type, metric, year/years, timestamp) 
def generate_report_name(cfg):
    # Riport típusa
    parts = [cfg.get("type", "report")]

    # Hozzáadja a mutatót a névhez, ha csak egy van
    if "metric" in cfg:
        parts.append(str(cfg["metric"]))

    # Egyetlen év esetén azt adjuk hozzá, több év esetén első, utolsó
    if "year" in cfg:
        parts.append(str(cfg["year"]))
    elif "years" in cfg:
        parts.append(f"{min(cfg['years'])}-{max(cfg['years'])}")

    # Összefűzés és timestamp
    base_name = "_".join(parts).replace(" ", "_").lower()
   
    return f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M')}"

#Jelentés exportálása ODS fájlba (egyetlen DataFrame vagy több lap - dict)
def export_to_ods(result, name):
    file_name = f"{name}.ods"

    with pd.ExcelWriter(file_name, engine="odf") as writer:

        # Ha az eredmény dict, minden kulcs-érték pár külön munkalapra kerül;
        # egyébként az egész DataFrame egyetlen lapra kerül a fájlnév alapján
        if isinstance(result, dict):
            sheets = result
        else:
            sheets = {name[:31]: result}

        for sheet_name, df in sheets.items():
            # A munkalapnév legfeljebb 31 karakter lehet
            safe_name = str(sheet_name)[:31]
            df.to_excel(writer, sheet_name=safe_name, index=True)

    print(f"Mentve: {file_name}")

#Riport futtatása: név generálása, riportfüggvény meghívása, eredmény mentése ODS fájlba
def run_report(report, df, agg_config, mgmt_df=None):
    """
    Args:
        report (dict): A riport meghatározása, két kötelező kulccsal:
            - "func": a riportfüggvény (pl. report_totals)
            - "cfg":  a riport konfig szótára (pl. type, years, agg)
        df (pd.DataFrame): A teljes, szűretlen adatkészlet.
        agg_config (dict): Az aggregációs szabályokat tartalmazó szótár
            (small / full kulcsokkal).
        mgmt_df (pd.DataFrame | None): Kezelői e-mail adatok; opcionális,
            csak az emails és missing riportoknál szükséges.
    """
    # Egyedi fájlnév a riport típusa, mutatói és év(ei) alapján
    name = generate_report_name(report["cfg"])

    log(f"A riport futtatása: {report['cfg']['type']}")

    # A riportfüggvény meghívása; visszaad egy DataFrame-et vagy szótárt
    result = report["func"](df, report["cfg"], agg_config, mgmt_df)

    # Az eredmény mentése ODS fájlba
    export_to_ods(result, name)

    log(f"Riport kész: {name}")


# 3. ADATBETÖLTÉS

# Adatbetöltés: a nézetekből egy átfogó adatkeret összeállítása park_ID és alapadat_ev mentén
def load_data(conn):
    # A betöltendő nézetek listája; az első az alaptábla
    views = ["park_base", "vallalkozasok_latest", "terulet_latest", "infrastrukturafejlesztes_latest", "EU_tamogatas_latest", "kapcsolatok_latest"]

    # Az alaptábla betöltése – ez lesz az összekapcsolás bal oldala
    df = pd.read_sql("SELECT * FROM park_base", conn)

    for v in views[1:]:
        # A kiegészítő nézet beolvasása
        tmp = pd.read_sql(f"SELECT * FROM {v}", conn)

        # Egy park egy adott évből csak egy sort kaphat; ha mégis több lenne,
        # az első sort tartjuk meg (groupby + first)
        tmp = tmp.groupby(["park_ID", "alapadat_ev"], as_index=False).first()

        # Bal oldali összekapcsolás park_ID és alapadat_ev mentén; duplikált oszlopok törtlése
        df = df.merge(tmp, on=["park_ID", "alapadat_ev"], how="left", validate="one_to_one", suffixes=('', '_drop'))
        df = df.loc[:, ~df.columns.str.contains('_drop')]

    # Numerikusnak szánt oszlopok kiválasztésa és átalakítása (későbbi numerikus műveletekhez)
    numeric_cols = list(get_agg_config()["full"].keys())
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

# 4. REPORT FÜGGVÉNYEK

# E-mail cím riport: egyedi e-mail címek listája és parkonkénti elérhetőségek 
def report_emails(df, cfg, agg_map=None, mgmt_df=None):
    # Csak az aktív (aktiv == 1) parkokat vesszük figyelembe
    df = df[df['aktiv'] == 1].copy()

    # Opcionális területi vagy egyéb szűrés alkalmazása
    f_df = apply_filters(df, filter_col=cfg.get("filter_col"), filter_values=cfg.get("filter_values"))

    # Ha rendelkezésre állnak kezelői adatok, bal oldali összekapcsolással
    # hozzáfűzzük a management_email oszlopot a szűrt táblához
    if mgmt_df is not None:
        f_df = f_df.merge(mgmt_df, on="park_ID", how="left")

    # A park e-mail és a kezelői e-mail oszlopokat egyetlen listába fűzzük össze,
    # majd eltávolítjuk a hiányzó értékeket, a duplikátumokat, és rendezzük
    emails = pd.concat([f_df["park_email"], f_df["management_email"]], ignore_index=True)
    emails = (emails.dropna().drop_duplicates().sort_values().reset_index(drop=True))

    # Parkonkénti részletes nézet: azonosító, név és mindkét e-mail cím;
    details = (
        f_df[["park_ID", "park_nev", "park_email", "management_email"]].drop_duplicates().reset_index(drop=True))

    return {
        "email_lista": pd.DataFrame({"Email": emails}),
        "reszletes": details
    }

# Hiányzó adatok riport: aktív parkok listája, amelyeknek nincs adata a megadott évben, e-mail címekkel kiegészítve
def report_missing(df, cfg, agg_map=None, mgmt_df=None):
    # Csak az aktív (aktiv == 1) parkokat vesszük figyelembe
    df = df[df['aktiv'] == 1].copy()

    # A vizsgált célév a konfigurációból
    year = cfg["year"]

    # Opcionális területi vagy egyéb szűrés alkalmazása
    filtered_df = apply_filters(df, years=None, filter_col=cfg.get("filter_col"), filter_values=cfg.get("filter_values"))

    # Az összes park–év kombináció megőrzése 
    all_parks = filtered_df[["park_ID", "park_nev", "park_email", "alapadat_ev"]]

    # Azok a park_ID-k, amelyeknek van adata a célévben
    parks_with_year = (apply_filters(all_parks, years=[year])[["park_ID"]].drop_duplicates())

    # Hiányzó parkok: amelyek szerepelnek az adatokban, de NEM a célévben
    missing = all_parks[~all_parks["park_ID"].isin(parks_with_year["park_ID"])].copy()

    # Kezelői e-mail hozzáfűzése bal oldali összekapcsolással
    if mgmt_df is not None:
        missing = missing.merge(mgmt_df, on="park_ID", how="left")
    else:
        missing["management_email"] = np.nan

    missing = missing.reset_index(drop=True)

    # Parkonkénti összesítés
    consolidated = (missing.groupby(["park_ID", "park_nev"], as_index=False).agg({
            "park_email": "first",
            "management_email": lambda x: "; ".join(sorted(x.dropna().unique()))
        })
    )

    return {"hianyzo": consolidated}

# Területi bontású pivot-tábla (mutatók vagy évek szerint) éves összehasonlítással és park-szintű átlagokkal
def report_pivot(df, cfg, agg_map_all, mgmt_df=None):
    # A konfigurációban megadott aggregációs készlet kiválasztása
    agg_dict = agg_map_all[cfg["agg"]]

    # Év és csoport szerinti szűrés alkalmazása
    df = apply_filters(df, cfg.get("years"), cfg.get("group_col"), cfg.get("filter_values"))

    # Az év oszlop egész számmá alakítása (pivot oszlopnevekhez szükséges)
    df['alapadat_ev'] = df['alapadat_ev'].astype(int)

    # Metrikák meghatározása: egyetlen, több (listában), vagy az összes elérhető metrika
    if cfg.get("metric"):
        metrics_to_use = [cfg.get("metric")]
    elif cfg.get("metrics"):
        metrics_to_use = cfg.get("metrics")
        if not isinstance(metrics_to_use, list):
            metrics_to_use = [metrics_to_use]
    else:
        metrics_to_use = list(agg_dict.keys())

    # Csak az aggregációs konfigban szereplő metrikák maradnak
    agg_dict_filtered = {m: agg_dict[m] for m in metrics_to_use if m in agg_dict}

    # Pivot-tábla: sorok = csoportosítási oszlop, oszlopok = évek, értékek = metrikák
    pivot = df.pivot_table(
        values=metrics_to_use,
        index=cfg["group_col"],
        columns="alapadat_ev",
        aggfunc={m: agg_dict_filtered[m] for m in metrics_to_use},
        fill_value=0
    )

    # A többszintű oszlopneveket egyszintű '<metric>_<year>' alakra hozzuk
    pivot.columns = [f"{metric}_{year}" for metric, year in pivot.columns]

    # Numerikus típusra kényszerítés
    for col in pivot.columns:
        pivot[col] = pd.to_numeric(pivot[col], errors='coerce')

    # Az évek listájának kinyerése az oszlopnevekből
    years = sorted({int(str(col).split("_")[-1]) for col in pivot.columns})

    # Egyedi parkok száma csoportonként – az egy parkra jutó átlag számításához
    park_counts = df.groupby(cfg["group_col"])['park_ID'].nunique()

    # A park_counts indexét igazítjuk a pivot indexéhez, hogy az osztás működjön (hiányzó csoportokhoz 0 kerül)
    park_counts = park_counts.reindex(pivot.index).fillna(0)

    for metric in agg_dict_filtered:
        agg_type = agg_dict_filtered[metric]

        for year in years:
            col_name = f"{metric}_{year}"
            per_park_col = f"{metric}_{year}_Per_Park"
            if col_name not in pivot.columns:
                continue

            if agg_type == "sum":
                # Összeg típusnál az értéket elosztjuk a parkok számával
                pivot[per_park_col] = (pivot[col_name] / park_counts) if park_counts.sum() != 0 else 0 
            elif agg_type == "mean":
                # Átlag típusnál az aggregált érték már önmagában átlag
                pivot[per_park_col] = pivot[col_name]
            else:
                pivot[per_park_col] = np.nan

    # Változás számítása csak akkor, ha legalább 2 év van
    if len(years) >= 2:
        y1, y2 = years[0], years[-1]
        for metric in agg_dict_filtered:

            per_park_y1 = f"{metric}_{y1}_Per_Park"
            per_park_y2 = f"{metric}_{y2}_Per_Park"

            # Abszolút változás: utolsó év − első év (park-szintű átlagokon)
            pivot[f"{metric}_Változás_per_park"] = pivot[per_park_y2] - pivot[per_park_y1]

            # Relatív változás százalékban
            pivot[f"{metric}_Változás_%_per_park"] = pivot[f"{metric}_Változás_per_park"] / pivot[per_park_y1].replace(0, np.nan) * 100
            pivot[f"{metric}_Változás_%_per_park"] = pivot[f"{metric}_Változás_%_per_park"].fillna(0).round(2)

    return round(pivot)

# Éves összesítő tábla az összes park adataibol, mutató alapján, parkszintu atlagokkal
def report_totals(df, cfg, agg_map_all, mgmt_df=None):
    # A konfigurációban megadott aggregációs készlet kiválasztása
    agg_dict = agg_map_all[cfg["agg"]]

    # Szűrés alkalmazása (évek, terület stb.)
    work_df = apply_filters(df, years=cfg.get("years"), filter_col=cfg.get("filter_col"), filter_values=cfg.get("filter_values"))

    # Az év oszlop egész számmá alakitása(pivot oszlopnevekhez szükséges)
    work_df['alapadat_ev'] = work_df['alapadat_ev'].astype(int)

    # Az évek listája: konfigurációban, vagy ha nincs megadva, az adatból kinyerve
    if cfg.get("years"):
        years = sorted(cfg.get("years"))
    else:
        years = sorted(work_df['alapadat_ev'].unique())

    years = [int(y) for y in years]

    # Egyedi parkok szama évenként - az átlagszámításhoz szükséges
    counts = work_df.groupby('alapadat_ev')['park_ID'].nunique()

    # Éves aggregáció az összes mutatóra, majd numerikus típusra kényszerítés és transzponálás (sorok = mutatók, oszlopok = évek)
    all_parks_agg = work_df.groupby('alapadat_ev').agg(agg_dict)
    all_parks_agg = all_parks_agg.apply(pd.to_numeric, errors='coerce').transpose()

    out = all_parks_agg

    # Parkszintű átlag hozzáadása minden évhez
    for y in all_parks_agg.columns:
        cnt = counts.get(y, 0)  # az adott evi parkszam

        for metric in out.index:
            agg_type = agg_dict.get(metric)
            col_name = y
            avg_col_name = f"{y} Átlag"

            if agg_type == "sum":
                # Összeg típusnál az összeget elosztjuk a parkok számával
                out.loc[metric, avg_col_name] = out.loc[metric, col_name] / cnt if cnt != 0 else 0
            elif agg_type == "mean":
                # Átlag típusnál az aggregált érték marad önmagában átlag
                out.loc[metric, avg_col_name] = out.loc[metric, col_name]
            else:
                out.loc[metric, avg_col_name] = np.nan

    # Növekedési mutatók számítása, ha legalább két év van
    if len(years) >= 2:
        first_year_col = f"{int(years[0])} Átlag"
        last_year_col = f"{int(years[-1])} Átlag"

        # Abszolút növekedés: utolsó év átlaga - első év átlaga
        out["Növekedés"] = out[last_year_col] - out[first_year_col]

        # Relativ növekedés abszolút értékben és százalekban
        out["Növekedés (%)"] = (out["Növekedés"] / out[first_year_col].replace(0, np.nan) * 100)
        out["Növekedés (%)"] = out["Növekedés (%)"].fillna(0)
    
    return round(out)


# 5. MAIN

def main():

    # Parancssori argumentumok feldolgozása - adatbázis fájl, futtatandó riportok megadása
    parser = argparse.ArgumentParser(
        description="Ipari park riport generátor. Adatokat olvas egy SQLite adatbázisból és ODS fájlokat állít elő."
    )

    # Kötelező: az adatbázis fájl elérési útja
    parser.add_argument(
        "--db",
        required=True,
        metavar="ADATBAZIS",
        help="Az SQLite adatbázis fájl elérési útja (kötelező). Pl.: --db IP"
    )

    # Opcionális: futtatandó riportok magyar nevei, vesszővel elválasztva
    parser.add_argument(
        "--riport",
        default=None,
        metavar="RIPORT_NEVEK",
        help=(
            "Futtatandó riportok neve, vesszővel elválasztva. "
            "Lehetséges értékek: osszesites, novekedes, emailek, hianyzo. "
            "Ha nincs megadva, az összes riport lefut. "
            "Pl.: --riport osszesites,emailek"
        )
    )

    # Opcionális: az összes riport futtatása (--riport alternatívája)
    parser.add_argument(
        "--all",
        action="store_true",
        help="Az összes riport futtatása (alapértelmezett, ha sem --riport, sem --all nincs megadva)."
    )

    args = parser.parse_args()

    # Magyar riportnév → belső típusnév megfeleltetés (ezt a riportkonfiguráció és a fájlnévgenerátor használja.
    RIPORT_NEV_MAP = {
        "osszesites": "totals",
        "novekedes":  "novekedes",
        "emailek":    "emails",
        "hianyzo":    "missing",
    }

    AGG_CONFIG = get_agg_config()

    # Adatbázis fájl ellenőrzése
    DB_PATH = args.db
    print(f"Adatbázis fájl: '{DB_PATH}'")
    if not os.path.isfile(DB_PATH):
        print(f"HIBA: Az adatbázis fájl nem található: '{DB_PATH}'")
        print("Ellenőrizze az elérési utat, és adja meg a --db paraméterrel.")
        raise SystemExit(1)

    # Adatbázis kapcsolat és adatbetöltés
    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Fő adattábla összeállítása a nézetekből
            df = load_data(conn)

            # Management e-mail betöltése
            mgmt_df = pd.read_sql("SELECT park_ID, management_email FROM management_latest", conn)
            mgmt_df = (
                mgmt_df
                .groupby("park_ID")["management_email"]
                .apply(lambda x: "; ".join(sorted(x.dropna().unique())))
                .reset_index()
            )
    except sqlite3.OperationalError as e:
        print(f"HIBA: Az adatbázis olvasása sikertelen (SQLite): {e}")
        print("Ellenőrizze, hogy az adatbázis fájl érvényes SQLite adatbázis-e, és tartalmazza a szükséges táblákat/nézeteket.")
        raise SystemExit(1)
    except pd.errors.DatabaseError as e:
        print(f"HIBA: Az adatbázis lekérdezése sikertelen (pandas): {e}")
        print("Ellenőrizze az adatbázis sémáját és a szükséges nézeteket.")
        raise SystemExit(1)
    except Exception as e:
        print(f"HIBA: Váratlan hiba az adatbetöltés során: {e}")
        traceback.print_exc()
        raise SystemExit(1)

    # Elérhető riportok definíciója (függvény + konfiguráció)
    reporting = [
        {"func": report_totals,  "cfg": {"type": "totals",    "agg": "small", "years": [2021, 2022, 2023], "filter_col": "park_regio", "filter_values": ["Közép-Dunántúl"]}},
        {"func": report_pivot,   "cfg": {"type": "novekedes", "group_col": "park_regio", "agg": "small", "metrics": ["EU_osszkoltseg", "arbevetel", "exportarany"], "years": [2021, 2022]}},
        {"func": report_emails,  "cfg": {"type": "emails",    "filter_col": "park_regio", "filter_values": ["Közép-Dunántúl"]}},
        {"func": report_missing, "cfg": {"type": "missing",   "year": 2024,  "filter_col": "park_regio", "filter_values": ["Közép-Dunántúl"]}},
    ]

    # Futtatandó riportok kiválasztása
    if args.all or args.riport is None:
        # --all jelző vagy hiányzó --riport esetén minden riport lefut
        selected_reports = reporting
    else:
        # A megadott magyar neveket belső típusnevekre képezzük le
        keresett_nevek = [nev.strip() for nev in args.riport.split(",")]
        belso_tipusok = set()
        for nev in keresett_nevek:
            if nev in RIPORT_NEV_MAP:
                belso_tipusok.add(RIPORT_NEV_MAP[nev])
            else:
                # Ismeretlen riportnév esetén figyelmeztetés, de a többi fut tovább
                ervenyes = ", ".join(RIPORT_NEV_MAP.keys())
                log(f"FIGYELMEZTETÉS: Ismeretlen riportnév: '{nev}'. Érvényes nevek: {ervenyes}")
        selected_reports = [r for r in reporting if r["cfg"]["type"] in belso_tipusok]

    # Riportok futtatása
    for report in selected_reports:
        try:
            run_report(report, df, AGG_CONFIG, mgmt_df)
        except Exception as e:
            log(f"HIBA a(z) '{report['cfg']['type']}' riportban: {e}")
            traceback.print_exc()


if __name__ == "__main__":
    main()

