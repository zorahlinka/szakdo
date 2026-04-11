import pandas as pd
import sqlite3
import numpy as np
import os

db = '/home/peti/Dokumentumok/gdf/adatbazis/IP'

agg_full = {
    "EU_osszkoltseg": "sum",

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
    "osszes_forras": "sum",

    "fejlesztesi_ugynokseg": "sum",
    "export_ugynokseg": "sum",
    "kulfoldi_ip": "sum",
    "nemzetkozi_projekt": "sum",
    "kf_tevekenyseg": "sum",
    "uj_technologia": "sum",
    "oktatas_felso": "sum",
    "kutatointezet": "sum",

    "osszterulet": "sum",
    "beepitett_ter": "sum",
    "berbeadott_ter_arany": "mean",
    "eladott_ter_arany": "mean",

    "vallalkozasok_szama": "sum",
    "vallalkozasok_foglalkoztatott": "sum",
    "beruhazasi_ertek": "sum",
    "arbevetel": "sum",
    "exportarany": "mean",
    "kkv_szam": "sum",
    "nagyvall_szam": "sum",
    "egyeb_vall_szam": "sum"
}

agg_small = {
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

# Segédfüggvények (osztás 0-val, nincs adatbázis, adatbetöltés - összes view összefésülése)

def division(numerator, denominator):
    with np.errstate(divide='ignore', invalid='ignore'):
        res = (numerator / denominator) * 100
        return res.replace([np.inf, -np.inf], 0).fillna(0)

def get_conn(db):
    if not os.path.exists(db):
        raise FileNotFoundError(f"Adatbázis nem található: {db}")
    return sqlite3.connect(db)

def load_data(db):
    views = ["park_base", "vallalkozasok_latest", "terulet_latest", "infrastrukturafejlesztes_latest", "EU_tamogatas_latest", "kapcsolatok_latest"]
    
    with get_conn(db) as conn:
        df = pd.read_sql(f"SELECT * FROM park_base", conn)
        for v in views[1:]:
            tmp = pd.read_sql(f"SELECT * FROM {v}", conn)
            df = df.merge(tmp, on=["park_ID", "alapadat_ev"], how="left")
    return df

# -------------------------------------------------

# Adatkezelő függvények

# 1) Parkok és managerek elérhetősége (a) összes email, (b) részletes kapcsolati adatok

def export_all_contacts(db, output_file):
    with get_conn(db) as conn:
        base = pd.read_sql("SELECT park_ID, park_nev, park_email FROM park_base", conn)
        management = pd.read_sql("SELECT park_ID, management_nev, management_email, management_tel FROM management_latest", conn)

    df = base.merge(management, on="park_ID", how="left")
    emails = pd.concat([df_contacts["park_email"], df_contacts["management_email"]]).dropna().drop_duplicates()
    df_emails = pd.DataFrame({"email": emails})
    df_contacts = df.drop(columns="park_ID").drop_duplicates() 
        
    with pd.ExcelWriter(output_file, engine="odf") as writer:
        df_emails.to_excel(writer, sheet_name="emailek", index=False)
        df_contacts.to_excel(writer, sheet_name="park_kapcsolatok", index=False)


# use:
db = "ip.db"
output_file = "park_kapcsolatok.ods"
export_all_contacts(db, output_file)


# 2)Parkok és managerek emailcíme, hiányzó éves jelentés esetén (a)park, (b)email, (c)park-email mapping

def export_missing_reports(db, year, output_file):
    with get_conn(db) as conn:
        base = pd.read_sql("SELECT park_ID, park_nev, park_email, management_email FROM park_base", conn)
        submitted = pd.read_sql(f"SELECT DISTINCT park_ID FROM vallalkozasok_latest WHERE alapadat_ev = {year}", conn)
        
    df_missing = base[~base["park_ID"].isin(submitted["park_ID"])]
    
    df_parks = df_missing[["park_nev"]].drop_duplicates()

    emails = pd.concat([df_missing["park_email"], df_missing["management_email"]]).dropna().drop_duplicates()
    df_emails = pd.DataFrame({"email": emails})
    
    df_mapping = df_missing[["park_nev", "park_email", "management_email"]].drop_duplicates()

    with pd.ExcelWriter(output_file, engine="odf") as writer:
        df_parks.to_excel(writer, sheet_name="parkok", index=False)
        df_emails.to_excel(writer, sheet_name="emailek", index=False)
        df_mapping.to_excel(writer, sheet_name="park-email", index=False)

 
# use:
db = "ip.db"
year = 2024
output_file = "hianyzo_jelentes.ods"
export_missing_reports(db, year, output_file)


#-----------------------------------------------------

# Statisztikai elemző függvények (fő adatok összehasonlítása régiónként vagy vármegyékként, valamint éves összehasonlítás)

# 1) Alap szűrés, aggregálás   
def filter_regional(df, group_col, agg_dict, years=None, filter_values=None):
    """
    df: merged dataframe
    group_col: 'park_regio' or 'park_varmegye'
    years: list of years (optional)
    filter_values: list of regions/varmegye (optional)
    """

    df_filtered = df.copy()

    # Szűrések
    if years is not None:
        df_filtered = df_filtered[df_filtered['alapadat_ev'].isin(years)]

    if filter_values is not None:
        df_filtered = df_filtered[df_filtered[group_col].isin(filter_values)]

    # Aggregálás régiónként, évenként
    df_filtered = df_filtered.groupby([group_col, 'alapadat_ev']).agg(agg_dict).reset_index()

    return df_filtered

# 2) EGY mutató összehasonlítása több régio/vármegye, több év viszonylatában, különbséggel
def compare_single_metric(df, group_col, metric, agg_dict, years=None, filter_values=None):
    """
    df: merged dataframe
    group_col: 'park_regio' or 'park_varmegye'
    metric: column to analyze (e.g. 'arbevetel')
    agg_dict
    years: list of years (optional)
    filter_values: list of regions/varmegye (optional)
    """
      
    # Ellenőrzés és adatlekérés
    df_filtered = df.copy()
    
    if years is not None:
        df_filtered = df_filtered[df_filtered['alapadat_ev'].isin(years)]
    if filter_values is not None:
        df_filtered = df_filtered[df_filtered[group_col].isin(filter_values)]
        
    if metric not in df_filtered.columns:
        raise ValueError(f"Hiba: A kért oszlop ('{metric}') nem található!")

    base_agg = df_filtered.groupby([group_col, 'alapadat_ev']).agg(agg_dict).reset_index()
    pivot = base_agg.pivot_table(values=metric, index=group_col, columns='alapadat_ev', fill_value=0).sort_index(axis=1)
    
    years_present = pivot.columns.tolist()
    
    if len(years_present) < 2:
        print(f"Figyelem: Csak egy évnyi adat ({years_present}) áll rendelkezésre. Nincs összehasonlítás.")
        filter_label = ""
        if filter_values is not None:
            filter_label = f"_filtered_{len(filter_values)}items"
        else ""
        file_name = f"jelentes_{metric}_{group_col}{filter_label}_{years_present}.ods"
        pivot.to_excel(file_name, engine="odf", index=True)
        
        return pivot, None, None
    
    first_year = years_present[0]
    last_year = years_present[-1]

    # Változás évről évre (abszolút, százalék), oszlopok elnevezése
    yearly_diff = pivot.diff(axis=1).drop(columns=first_year).round(0)
    yearly_pct = division(yearly_diff, pivot.drop(columns=last_year).values).round(2)
    
    yearly_diff.columns = [f"{col} vs elozo ev (ertek)" for col in yearly_diff.columns]
    yearly_pct.columns = [f"{col} vs elozo ev (%)" for col in yearly_pct.columns]
    
    # Változás a teljes időszakban (első és utolső év között)
    total_diff = pivot[last_year] - pivot[first_year].round(0)
    total_pct = division(total_diff, pivot[first_year]).round(2)

    # Összeillesztés egy táblázatba
    final_report = pivot.copy()
    final_report = pd.concat([final_report, yearly_diff, yearly_pct], axis=1)
    final_report[f"Teljes kulonbseg ({first_year}-{last_year})"] = total_diff
    final_report[f"Teljes % ({first_year}-{last_year})"] = total_pct

    # Mentés
    file_label = "_".join([str(years_present[0]), str(years_present[-1])])
    filter_label = ""
    if filter_values is not None:
        filter_label = f"_filtered_{len(filter_values)}items"
    else ""
    file_name = f"jelentes_{metric}_{group_col}{filter_label}_{file_label}.ods"
    final_report.to_excel(file_name, engine="odf")

    return final_report

# 3) EGY régió/vármegye/park összes mutatójának idősoros elemzése, különbséggel
def compare_single_entity_all_metrics(df, group_col, entity_name, agg_dict, years=None):
    """
    Y tengely: Mutatók az agg_dict-ből
    X tengely: Évek + Változás oszlopok
    group_col: park_regio, park_varmegye, park_nev
    """
    
    # 1. Ellenőrzés: Létezik-e a keresett entitás?
    if entity_name not in df[group_col].unique():
        available_names = sorted(df[group_col].dropna().unique())
        print(f"HIBA: '{entity_name}' nem található a(z) '{group_col}' oszlopban.")
        print(f"Választható lehetőségek: {available_names}")
        return None

    # 2. Szűrés az adott egységre és évekre
    df_filtered = df[df[group_col] == entity_name].copy()
    if years:
        df_filtered = df_filtered[df_filtered['alapadat_ev'].isin(years)]

    if df_filtered.empty:
        print(f"FIGYELEM: Nincs elérhető adat a megadott évekre: {years}")
        return None

    # 3. Aggregálás évenként és Transzponálás (.T)
    # Ezzel érjük el, hogy a mutatók legyenek a sorok, az évek pedig az oszlopok
    report = df_filtered.groupby('alapadat_ev').agg(agg_dict).T
    report = report.sort_index(axis=1) # Évek sorrendbe tétele

    # 4. Idősoros változások számítása (ha van legalább 2 év)
    years_present = report.columns.tolist()
    if len(years_present) >= 2:
        first_year = years_present[0]
        last_year = years_present[-1]

        # Abszolút különbség az első és utolsó év között
        report['Teljes változás'] = report[last_year] - report[first_year]
        
        # Százalékos változás hibakezeléssel (nullával osztás -> 0)
        report['Teljes változás (%)'] = (
            (report['Teljes változás'] / report[first_year] * 100)
            .replace([np.inf, -np.inf], 0)
            .fillna(0)
        )

    # 5. Kerekítés a mutató típusa szerint (sum -> egész, mean -> 2 tizedes)
    for metric, method in agg_dict.items():
        if method == "sum":
            report.loc[metric] = report.loc[metric].round(0).astype(int)
        else:
            report.loc[metric] = report.loc[metric].round(2)

    # 6. Mentés .ods formátumba
    # Speciális karakterek tisztítása a fájlnévből
    safe_entity_name = str(entity_name).replace(' ', '_').replace('/', '-')
    file_name = f"reszletes_elemzes_{safe_entity_name}.ods"
    
    try:
        report.to_excel(file_name, engine="odf", index=True)
        print(f"Sikeres mentés: {file_name}")
    except Exception as e:
        print(f"Hiba a mentés során: {e}")

    return report

# 4) A compare_single_entity_all_metrics végrejatása minden régióra/vármegyére, mentés egy fájl külön munkalapjaira
def export_all_entities_to_one_file(df, group_col, agg_dict, output_name, years=None):
    
    entities = sorted(df[group_col].dropna().unique())
        
    with pd.ExcelWriter(output_name, engine="odf") as writer:
        for entity in entities:
            report = compare_single_entity_metrics(df, group_col, entity, agg_dict, years)
            if report is not None:
                sheet_label = str(entity)[:31] # sheet neve max 31 karakter
                report.to_excel(writer, sheet_name=sheet_label)
                                
    print(f"Fájl mentve: {output_name}")


# 5) EGY vagy KÉT év adatai több adatoszlop, több régió/vármegye vonatkozásában
def compare_metrics_by_year(df, group_col, agg_dict, years):
    """
    y tengely: régiók
    x tengely: agg_dict mutatói, mindegyik alatt az évek (1 vagy 2 oszlop)
    """
    # Adatok előkészítése (évek, oszlopok listába rendezése)
    if isinstance(years, int): 
        years = [years]
    
    metrics = list(agg_dict.keys())
    
    base = filter_regional(df, group_col, agg_dict, years)
    
    # Pivot tábla ('values' az agg_small kulcsai)
    pivot = base.pivot_table(
        values=metrics, 
        index=group_col, 
        columns='alapadat_ev',
        fill_value=0
    )
    
    # Oszlopok rendezése: Mutató szerint (level 0), majd Év szerint (level 1)
    pivot = pivot.sort_index(axis=1, level=0)
    
    # Kerekítés
    for metric in metrics:
        if agg_dict[metric] == "sum":
            pivot[metric] = pivot[metric].round(0).astype(int)
        else:
            pivot[metric] = pivot[metric].round(2)
    
    # Mentés
    file_label = "_".join(map(str, years))
    filter_label = ""
    if filter_values is not None:
        filter_label = f"_filtered_{len(filter_values)}items"
    file_name = f"jelentes_{group_col}{filter_label}_{file_label}.ods"
    pivot.to_excel(file_name, engine="odf")
    
    return pivot

# 6) Éves összesített jelentés
def compare_all_parks_total_and_avg(df, agg_dict, years=None):
    """
    Y tengely: Mutatók
    X tengely: Évek (minden év alatt: a) Összesen, b) Átlag per park)
    """
    df_f = df.copy()
    if years:
        df_f = df_f[df_f['alapadat_ev'].isin(years)]

    # Évenkénti aggregálás (Összeg és Átlag egyszerre)
    report = df_f.groupby('alapadat_ev').agg(agg_dict).T
    
    park_counts = df_f.groupby('alapadat_ev')['park_ID'].nunique()
    
    # Létrehozunk egy új MultiIndex struktúrát
    years_present = sorted(df_f['alapadat_ev'].unique())
    metrics = list(agg_dict.keys())
    final_cols = []
    for y in years_present:
        final_cols.append((y, 'Összesen'))
        final_cols.append((y, 'Átlag per park'))
    
    final_df = pd.DataFrame(index=metrics, columns=pd.MultiIndex.from_tuples(final_cols))

    for y in years_present:
        count = park_counts[y]
        for m in metrics:
            # Összesen érték
            total_val = df_f[df_f['alapadat_ev'] == y][m].sum() if agg_dict[m] == "sum" else df_f[df_f['alapadat_ev'] == y][m].mean()
            
            final_df.loc[m, (y, 'Összesen')] = total_val.round(0)
            # Átlagos érték (egy parkra jutó)
            final_df.loc[m, (y, 'Átlag per park')] = (total_val / count if agg_dict[m] == "sum" else total_val).round(2)

    final_df.to_excel("osszesitett_park_elemzes.ods", engine="odf")
    return final_df


"""
use

df = load_data(db)

pivot, diff, pct = compare_one_metric_regionally(
    df,
    group_col="park_regio",
    agg_dict=agg_small,
    years=[2021, 2022, 2023]
)

jelentes_park_regio_2022_2023 = compare_metrics_by_year(df, 'park_regio', agg_small, [2022, 2023])

keresett_varmegyek = ['Veszprém', 'Fejér']
jelentes_2_varmegye_2022_2024 = ompare_metrics_by_year(df, 'park_varmegye', agg_small, [2022, 2023], filter_values=keresett_varmegyek)
"""


generalt futas
"""
if __name__ == "__main__":
    # 1. Adatbetöltés
    # Cseréld ki az útvonalat a saját adatbázisodra!
    DATABASE_PATH = "ipari_parkok.db" 
    df = load_data(DATABASE_PATH)
    
    print("Adatok betöltve. Elemzések indítása...")

    # --- PÉLDA 1: Egy konkrét mutató (Árbevétel) YoY elemzése vármegyékre ---
    # Minden vármegyét nézünk, minden elérhető évre
    print("Vármegyei árbevétel elemzés generálása...")
    compare_one_metric(
        df, 
        group_col='park_varmegye', 
        metric='arbevetel', 
        agg_dict=agg_small
    )

    # --- PÉLDA 2: Két konkrét régió összehasonlítása az összes kis mutatóval ---
    # Csak 2022 és 2023 évekre szűrve
    print("Kiemelt régiók összehasonlítása...")
    kiemelt_regiok = ['Közép-Magyarország', 'Észak-Alföld']
    compare_one_metric(
        df, 
        group_col='park_regio', 
        metric='vallalkozasok_szama', 
        agg_dict=agg_small, 
        years=[2022, 2023],
        filter_values=kiemelt_regiok
    )

    # --- PÉLDA 3: Egyetlen régió (vagy park) mélyelemzése (minden mutató) ---
    print("Pest vármegye részletes idősoros elemzése...")
    compare_single_entity_metrics(
        df, 
        group_col='park_varmegye', 
        entity_name='Pest', 
        agg_dict=agg_small
    )

    print("\nGratulálok! Minden riport elkészült és mentve lett .ods formátumban.")

"""




# Összehasonlító riport két év adatai alapján az összes park vonatkozásában

def compare_years(db, year1, year2):
    
    conn = sqlite3.connect(db)
        
    query = """
    SELECT * FROM eves_osszefoglalo 
    WHERE adatszolgaltatas_ev IN (?, ?)
    ORDER BY adatszolgaltatas_ev ASC
    """

       
    df = pd.read_sql_query(query, conn, params=(year1, year2))
    
    if len(df) < 2:
        print("Az egyik év adatai nem találhatók az adatbázisban.")
        return None

    # Esetleg különbség számítása az év1 és év2 között, ha szükséges
    df = df.set_index('adatszolgaltatas_ev')
   
    comparison = df.diff().iloc[1:].rename(index={year2: 'Különbség'})
    
    df_final_report = pd.concat([df, comparison])

    df_final_report.to_excel('osszehasonlitas.ods', engine='odf', index=False)

    conn.close()

    return df_final_report

database = 'ip.db'
year_a = 2023
year_b = 2024

results = compare_years(database, year_a, year_b)
print(results)



