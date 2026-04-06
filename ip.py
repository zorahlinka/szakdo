from email import errors

import pandas as pd
import sqlite3
from thefuzz import process, fuzz


excel_file = '/home/peti/Dokumentumok/gdf/kerdoiv/kerdoiv_valasz_1.ods'
db = '/home/peti/Dokumentumok/gdf/adatbazis/IP'
sheets_to_read = ['Adatok', 'Management', 'Elnyert EU-s támogatás', 'Infrastruktúra', 'Szolgáltatások']


column_mapping = {
    'Ipari Park neve': 'park_nev',
    'Ipari park cím elnyerésének éve': 'ip_cimszerzes',
    'Technológiai park cím elnyerésének éve': 'tp_cimszerzes',
    'Email': 'park_email',
    'Honlap': 'park_honlap',
    'Címviselő szervezet neve': 'cimviselo_nev',
    'Címviselő szervezet foglalkoztatottak száma': 'cimviselo_foglalkoztatott',
    'Címviselő szervezet címe': 'cimviselo_cim',
    'Állami': 'osszetetel_allam',
    'Önkormányzati': 'osszetetel_onkormanyzat',
    'Belföldi magán': 'osszetetel_belfoldi_magan',
    'Külföldi': 'osszetetel_kulfoldi',
    'Egyéb': 'osszetetel_egyeb',
    'Település': 'park_telepules',
    'Út/utca': 'park_utca',
    'Irányítószám': 'park_iranyitoszam',
    'Vármegye': 'park_varmegye',
    'Helyrajzi szám': 'park_hrsz',
    'Régió': 'park_regio',
    'Összterület (ha)': 'osszterulet',
    'Hasznosítható terület (ha)': 'hasznosithato_ter',
    'Betelepített terület (ha)': 'beepitett_ter',
    'Hasznosítható szabad terület (ha)': 'hasznosithato_szabad_ter',
    'Hasznosítható szabad terület aránya (%)': 'hasznosithato_szabad_arany',
    'Parkolóhely területe (m2)': 'parkolo',
    'Zöldterületek, parkok (m2)': 'zoldterulet',
    'Betelpített területeit bérbe adja (%)': 'berbeadott_ter_arany',
    'Betelepített területeit eladta (%)': 'eladott_ter_arany',
    'Kamarák': 'kamara',
    'Klaszterek': 'klaszter',
    'Középfokú oktatási intézmények': 'oktatas_kozep',
    'Munkaügyi központ': 'munkaugy',
    'Szakmai civil szervezetek': 'civil',
    'Más ipari parkok': 'ip',
    'Önkormányzat': 'onkormanyzat',
    'Állami fejlesztési ügynökségek': 'fejlesztesi_ugynokseg',
    'Magyar Exportfejlesztési Ügynökség': 'export_ugynokseg',
    'Külföldi ipari park': 'kulfoldi_ip',
    'Részvétel nemzetközi projektekben': 'nemzetkozi_projekt',
    'Együttműködés felsőoktatási intézménnyel': 'oktatas_felso',
    'Együttműködés kutatóintézettel': 'kutatointezet',
    'Önálló kutatási tevékenység': 'kf_tevekenyseg',
    'Piacközeli stádiumban lévő technológiák alkalmazása': 'uj_technologia',
    'Maga nyújtja (%)': 'sajat_szolg_arany',
    'Kiszervezi (%)': 'kiszervezett_szolg_arany', 
    'Vállalkozások területe (ha)': 'vallalkozasok_terulet', 
    'Vállalkozások száma': 'vallalkozasok_szama', 
    'Foglalkoztatottak létszáma (fő)': 'vallalkozasok_foglalkoztatott',
    'Beruházási érték (millió Ft)': 'beruhazasi_ertek', 
    'Árbevétel (millió Ft)': 'arbevetel', 
    'Exportarány (%)': 'exportarany', 
    'KKV-k száma': 'kkv_szam', 
    'Nagyvállalatok száma': 'nagyvall_szam', 
    'Egyéb vállalkozások száma': 'egyeb_vall_szam',
    'Mely évre vonatkoznak az adatok?': 'vallalkozasok_ev',
    'Saját forrás (millió Ft)': 'sajat_forras',
    'Állami támogatás (millió Ft)': 'allami_forras',
    'Önkormányzati támogatás (millió Ft)': 'onkormanyzati_forras',
    'EU támogatás (millió Ft)': 'EU_forras',
    'Bankhitel (millió Ft)': 'bankhitel',
    'Tagi kölcsön (millió Ft)': 'tagi_kolcson',
    'Tőkeemelés (millió Ft)': 'tokeemeles',
    'Egyéb (millió Ft)': 'egyeb_forras',
    'ÖSSZES forrás (millió Ft)': 'osszes_forras',
    'Felhasználás éve': 'felhasznalas_ev',
    'Felelős neve': 'management_nev',
    'Jognyilatkozatra jogosult': 'jognyilatkozat',
    'Operatív felelős': 'operativ',
    'Beosztása': 'management_beosztas',
    'Telefon': 'management_tel',
    'Felelős e-mail': 'management_email',
    'Megbízatás kezdete': 'management_kezdet',
    'Megbízatás vége': 'management_vege',
    'Operatív program': 'op',
    'Odaítélés éve': 'tamogatas_ev',
    'Projekt tartalom': 'tamogatas_tartalom',
    'Támogatási intenzitás': 'intenzitas',
    'Összköltség (millió Ft)': 'EU_osszkoltseg',
    'Infrastruktúra típusa': 'infra_tipus',
    'Infrastruktúra neve': 'infra_nev',
    'Kapacitás': 'kapacitas',
    'Ellátott terület nagysága (ha)': 'ellatott_ter',
    'Állapota (Megfelelő/ Bővítendő/ Felújítandó)': 'allapot',
    'Tervezett fejlesztés éve': 'terv_fejlesztes_ev',
    'Tervezett forrás': 'terv_forras',
    'Szolgáltatás típusa': 'szolg_tipus',
    'Szolgáltatás neve': 'szolg_nev',
    'Szolgáltatás tartalma': 'szolg_tartalom',
    'Szolgáltató szervezet típusa': 'szolgaltato_fajta',
    'Szolgáltató szervezet neve': 'szolgaltato_nev',
    'Szolgáltatás kezdete': 'szolg_kezdet',
}

allapot_map = {
    'Megfelelő': '1',
    'Bővítendő': '2',
    'Felújítandó': '3'
}

# listák az adatellenőrzéshez
park_varmegye = ['Bács-Kiskun', 'Baranya', 'Békés', 'Borsod-Abaúj-Zemplén', 'Csongrád-Csanád', 'Fejér', 'Győr-Moson-Sopron', 'Hajdú-Bihar', 'Heves', 'Jász-Nagykun-Szolnok', 'Komárom-Esztergom', 'Nógrád', 'Pest', 'Somogy', 'Szabolcs-Szatmár-Bereg', 'Tolna', 'Vas', 'Veszprém', 'Zala', 'Budapest']
park_regio = ['Közép-Magyarország', 'Közép-Dunántúl', 'Nyugat-Dunántúl', 'Dél-Dunántúl', 'Észak-Magyarország', 'Észak-Alföld', 'Dél-Alföld']
szolgaltato_fajta = ['A park üzemeltetője', 'Betelepült vállalkozás', 'Egyéb']
allapot = ['Megfelelő', 'Bővítendő', 'Felújítandó']

# adatellenőrzéshez szükséges szabályok oszlopok szerint:
val_rules = {
    "text_255": ["Ipari Park neve", "Címviselő szervezet neve", "Címviselő szervezet címe", "Település", "Út/utca", "Felelős neve", "Beosztása", "Operatív program", "Infrastruktúra típusa", "Infrastruktúra neve", "Szolgáltatás típusa", "Szolgáltatás neve", "Szolgáltató szervezet neve"],
    "text_500": ["Szolgáltatás tartalma", "Projekt tartalom" ],
    "date": ["Management kezdete", "Management vége"],
    "year_max": ["Ipari park cím elnyerésének éve", "Technológiai park cím elnyerésének éve", "Mely évre vonatkoznak az adatok?", "Felhasználás éve", "Szolgáltatás kezdete", "Odaítélés éve",],
    "year_min": ["Tervezett fejlesztés éve",],
    "email": ["Email", "Felelős e-mail",],
    "percentage": ["Állami", "Önkormányzati", "Belföldi magán", "Külföldi", "Egyéb", "Hasznosítható szabad terület aránya (%)", "Betelepített területeit bérbe adja (%)", "Betelepített területeit eladta (%)", "Maga nyújtja (%)", "Kiszervezi (%)", "Exportarány (%)", "Támogatási intenzitás"],
    "positive_integer": ["Címviselő szervezet foglalkoztatottak száma", "Irányítószám", "Vállalkozások száma", "Foglalkoztatottak létszáma (fő)", "KKV-k száma", "Nagyvállalatok száma", "Egyéb vállalkozások száma"],
    "positive_real": ["Összterület (ha)", "Hasznosítható terület (ha)", "Betelepített terület (ha)", "Hasznosítható szabad terület (ha)", "Parkolóhely területe (m2)", "Zöldterületek, parkok (m2)", "Vállalkozások területe (ha)", "Beruházási érték (millió Ft)", "Árbevétel (millió Ft)", "Saját forrás (millió Ft)", "Állami támogatás (millió Ft)", "Önkormányzati támogatás (millió Ft)", "EU támogatás (millió Ft)", "Bankhitel (millió Ft)", "Tagi kölcsön (millió Ft)", "Tőkeemelés (millió Ft)", "Egyéb (millió Ft)", "ÖSSZES forrás (millió Ft)", "Kapacitás", "Ellátott terület nagysága (ha)", "Tervezett forrás", "Összköltség (millió Ft)"],
    "boolean": ["Kamarák", "Klaszterek", "Középfokú oktatási intézmények", "Munkaügyi központ", "Szakmai civil szervezetek", "Más ipari parkok", "Önkormányzat", "Állami fejlesztési ügynökségek", "Magyar Exportfejlesztési Ügynökség", "Külföldi ipari park", "Részvétel nemzetközi projektekben", "Együttműködés felsőoktatási intézménnyel", "Együttműködés kutatóintézettel", "Önálló kutatási tevékenység", "Piacközeli stádiumban lévő technológiák alkalmazása", "Jognyilatkozatra jogosult", "Operatív felelős"],
    "varmegye": ["Vármegye"],
    "regio": ["Régió"],
    "szolgaltato_fajta": ["Szolgáltató szervezet típusa"],
    "telephone": ["Telefon"],
}

# beolvasás
def read_excel(excel_file, sheets_to_read):
    try:   
        return pd.read_excel(excel_file, sheet_name=sheets_to_read, engine='odf')
    except Exception as e:
        print(f"Hiba történt az Excel fájl beolvasása során: {e}")
        return None 

# adatellenőrzés
def data_validation(all_data):
    errors = []

    for idx, row in all_data.iterrows():
        line = idx + 2

        for col in val_rules["text_255"]:
            if col in row and isinstance(row[col], str) and len(row[col]) > 255:
                errors.append(f"{line}. sor: '{col}' Túl hosszú (max 255).")
        for col in val_rules["text_500"]:
            if col in row and isinstance(row[col], str) and len(row[col]) > 500:
                errors.append(f"{line}. sor: '{col}' Túl hosszú (max 500).")
        for col in val_rules["date"]:
            if col in row and pd.notnull(row[col]) and not isinstance(row[col], pd.Timestamp):
                errors.append(f"{line}. sor: '{col}' Érvénytelen dátum formátum.")
        for col in val_rules["year_max"]:
            if col in row and (not isinstance(row[col], (int, float)) or not (1980 <= row[col] <= pd.Timestamp.now().year)):
                errors.append(f"{line}. sor: '{col}' Érvénytelen év (1980 - jelen).")
        for col in val_rules["year_min"]:
            if col in row and (not isinstance(row[col], (int, float)) or row[col] < pd.Timestamp.now().year):
                errors.append(f"{line}. sor: '{col}' Érvénytelen év (min jelen).")
        for col in val_rules["email"]:
            if col in row and isinstance(row[col], str) and not pd.Series(row[col]).str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$').any():
                errors.append(f"{line}. sor: '{col}' Érvénytelen email cím.")
        for col in val_rules["percentage"]:
            if col in row and (not isinstance(row[col], (int, float)) or not (0 <= row[col] <= 100)):
                errors.append(f"{line}. sor: '{col}' Érvénytelen százalék érték (0-100).")
        for col in val_rules["positive_real"]:
            if col in row and (not isinstance(row[col], (int, float)) or row[col] < 0):
                errors.append(f"{line}. sor: '{col}' Érvénytelen pozitív valós érték (min 0).")
        for col in val_rules["positive_integer"]:
            if col in row and (not isinstance(row[col], (int, float)) or row[col] < 0 or not float(row[col]).is_integer()):
                errors.append(f"{line}. sor: '{col}' Érvénytelen pozitív egész érték (min 0).")
        for col in val_rules["boolean"]:
            if col in row and str(row[col]).lower() not in ['igen', 'nem']:
                errors.append(f"{line}. sor: '{col}' Érvénytelen érték (csak 'igen' vagy 'nem').")
        for col in val_rules["varmegye"]:   
            if col in row and isinstance(row[col], str) and row[col] not in park_varmegye:
                errors.append(f"{line}. sor: '{col}' Érvénytelen vármegye.")
        for col in val_rules["regio"]:
            if col in row and isinstance(row[col], str) and row[col] not in park_regio:
                errors.append(f"{line}. sor: '{col}' Érvénytelen régió.")
        for col in val_rules["szolgaltato_fajta"]:
            if col in row and isinstance(row[col], str) and row[col] not in szolgaltato_fajta:
                errors.append(f"{line}. sor: '{col}' Érvénytelen szolgáltató szervezet típusa.")
        for col in val_rules["allapot"]:
            if col in row and isinstance(row[col], str) and row[col] not in allapot:
                errors.append(f"{line}. sor: '{col}' Érvénytelen állapot (Megfelelő, Bővítendő, Felújítandó).")
        for col in val_rules["telephone"]:
            if col in row and isinstance(row[col], str) and not pd.Series(row[col]).str.contains(r'^\+?[\d\s\-\(\)]+$').any():
                errors.append(f"{line}. sor: '{col}' Érvénytelen telefonszám.")
        
        if row.get('Állami') is not None and row.get('Önkormányzati') is not None and row.get('Belföldi magán') is not None and row.get('Külföldi') is not None and row.get('Egyéb') is not None:
            total_percentage = row['Állami'] + row['Önkormányzati'] + row['Belföldi magán'] + row['Külföldi'] + row['Egyéb']
            if total_percentage != 100:
                errors.append(f"{line}. sor: 'Állami', 'Önkormányzati', 'Belföldi magán', 'Külföldi' és 'Egyéb' összege nem lehet nagyobb, mint 100%.")
        if row.get('Hasznosítható terület (ha)') is not None and row.get('Összterület (ha)') is not None and row['Hasznosítható terület (ha)'] > row['Összterület (ha)']:
            errors.append(f"{line}. sor: 'Hasznosítható terület (ha)' nem lehet nagyobb, mint 'Összterület (ha)'.")
        if row.get('Betelepített terület (ha)') is not None and row.get('Hasznosítható szabad terület (ha)') is not None and row.get('Hasznosítható terület (ha)') is not None:
            if row['Betelepített terület (ha)'] + row['Hasznosítható szabad terület (ha)'] > row['Hasznosítható terület (ha)']:
                errors.append(f"{line}. sor: 'Betelepített terület (ha)' és 'Hasznosítható szabad terület (ha)' összege nem lehet nagyobb, mint 'Hasznosítható terület (ha)'.")
        if row.get('Betelepített területeit bérbe adja (%)') is not None and row.get('Betelepített területeit eladta (%)') is not None:
            if row['Betelepített területeit bérbe adja (%)'] + row['Betelepített területeit eladta (%)'] > 100:
                errors.append(f"{line}. sor: 'Betelepített területeit bérbe adja (%)' és 'Betelepített területeit eladta (%)' összege nem lehet nagyobb, mint 100%.")
        if row.get('Összes forrás (millió Ft)') is not None and row.get('Saját forrás (millió Ft)') is not None and row.get('Állami támogatás (millió Ft)') is not None and row.get('Önkormányzati támogatás (millió Ft)') is not None and row.get('EU támogatás (millió Ft)') is not None and row.get('Bankhitel (millió Ft)') is not None and row.get('Tagi kölcsön (millió Ft)') is not None and row.get('Tőkeemelés (millió Ft)') is not None and row.get('Egyéb (millió Ft)') is not None:
            total_sources = row['Saját forrás (millió Ft)'] + row['Állami támogatás (millió Ft)'] + row['Önkormányzati támogatás (millió Ft)'] + row['EU támogatás (millió Ft)'] + row['Bankhitel (millió Ft)'] + row['Tagi kölcsön (millió Ft)'] + row['Tőkeemelés (millió Ft)'] + row['Egyéb (millió Ft)']
            if total_sources != row['Összes forrás (millió Ft)']:
                errors.append(f"{line}. sor: 'Összes forrás (millió Ft)' értéke nem egyezik meg a források összegével.")
        if row.get('Maga nyújtja (%)') is not None and row.get('Kiszervezi (%)') is not None:
            if row['Maga nyújtja (%)'] + row['Kiszervezi (%)'] > 100:
                errors.append(f"{line}. sor: 'Maga nyújtja (%)' és 'Kiszervezi (%)' összege nem lehet nagyobb, mint 100%.")
        
    return errors

# egyezés keresés a park név és címviselő név oszlopokra, hogy kinyerjük az adatbázisban már szereplő parkok és címviselők azonosítóit    
def match_park_ID(park_tocheck, db_park_azonosito, threshold=0.85):
    #if not park_tocheck or not isinstance(park_tocheck, str):
        #return None
    match = process.extractOne(park_tocheck, db_park_azonosito['park_nev'], scorer=fuzz.token_sort_ratio)
        # visszaad: szöveg, pontszám, index
    if match and match[1] >= threshold:
        park_id = db_park_azonosito.loc[match[2], 'park_ID']
        return park_id
    return None

def match_cimviselo_ID(cimviselo_tocheck, db_cimviselo, threshold=0.85):
    #if not cimviselo_tocheck or not isinstance(cimviselo_tocheck, str):
        #return None
    match = process.extractOne(cimviselo_tocheck, db_cimviselo['cimviselo_nev'], scorer=fuzz.token_sort_ratio)
    if match and match[1] >= threshold:
        cimviselo_id = db_cimviselo.loc[match[2], 'cimviselo_ID']
        return cimviselo_id
    return None

def transform_write_to_db(db, all_data, column_mapping, allapot_map):
    
    df_adatok = all_data['Adatok'].rename(columns=column_mapping)
    df_management = all_data['Management'].rename(columns=column_mapping)
    df_EU_tamogatas = all_data['Elnyert EU-s támogatás'].rename(columns=column_mapping)
    df_infrastruktura = all_data['Infrastruktúra'].rename(columns=column_mapping)
    df_szolgaltatasok = all_data['Szolgáltatások'].rename(columns=column_mapping)
    
    df_infrastruktura['allapot'] = df_infrastruktura['allapot'].map(allapot_map)

    table_park_azonosito_data = df_adatok[['park_nev']].copy(),
    table_cimviselo_azonosito_data = df_adatok[['cimviselo_nev']].copy(),    
    table_cimviselo_data = df_adatok[['cimviselo_foglalkoztatott', 'cimviselo_cim', 'osszetetel_allam', 'osszetetel_onkormanyzat', 'osszetetel_belfoldi_magan', 'osszetetel_kulfoldi', 'osszetetel_egyeb',]].copy(),
    table_alapadat_data = df_adatok[['ip_cimszerzes', 'tp_cimszerzes', 'park_email', 'park_telepules', 'park_utca', 'park_iranyitoszam', 'park_varmegye', 'park_hrsz', 'park_regio', 'sajat_szolg_arany' , 'kiszervezett_szolg_arany', 'park_honlap']].copy(),
    table_management_data = df_management[['management_nev', 'jognyilatkozat', 'operativ', 'management_beosztas', 'management_tel', 'management_email', 'management_kezdet', 'management_vege']].copy(),
    table_EU_tamogatas_data = df_EU_tamogatas[['op', 'tamogatas_ev', 'tamogatas_tartalom', 'intenzitas', 'EU_osszkoltseg']].copy(),
    table_vallalkozasok_data = df_adatok[['vallalkozasok_terulet', 'vallalkozasok_szama', 'vallalkozasok_foglalkoztatott', 'beruhazasi_ertek', 'arbevetel', 'exportarany', 'kkv_szam', 'nagyvall_szam', 'egyeb_vall_szam', 'vallalkozasok_ev']].copy(),
    table_infra_fajta_data = df_infrastruktura[['infra_tipus','infra_nev']].copy(),
    table_infrastruktura_data = df_infrastruktura[['infra_tipus','infra_nev','kapacitas','ellatott_ter','allapot','terv_fejlesztes_ev','terv_forras']].copy(),
    table_infrastrukturafejlesztes_data = df_adatok[['felhasznalas_ev', 'sajat_forras', 'allami_forras', 'onkormanyzati_forras', 'EU_forras', 'bankhitel', 'tagi_kolcson', 'tokeemeles', 'egyeb_forras', 'osszes_forras']].copy(),
    table_szolg_fajta_data = df_szolgaltatasok[['szolg_tipus','szolg_nev']].copy(),
    table_szolgaltatasok_data = df_szolgaltatasok[['szolg_tipus','szolg_nev','szolg_tartalom','szolgaltato_fajta','szolgaltato_nev','szolg_kezdet']].copy(),
    table_helyrajzi_szam_data = df_adatok['park_hrsz'].astype(str).str.split(',').explode().str.strip().to_frame(name='park_hrsz').copy(),
    table_kapcsolatok_data = df_adatok[['kamara', 'klaszter', 'oktatas_kozep', 'munkaugy', 'civil', 'ip', 'onkormanyzat', 'fejlesztesi_ugynokseg', 'export_ugynokseg', 'kulfoldi_ip', 'nemzetkozi_projekt', 'oktatas_felso', 'kutatointezet', 'kf_tevekenyseg', 'uj_technologia']].copy()
    table_terulet_data = df_adatok[['osszterulet', 'hasznosithato_ter', 'beepitett_ter', 'hasznosithato_szabad_ter', 'hasznosithato_szabad_arany', 'parkolo', 'zoldterulet', 'berbeadott_ter_arany', 'eladott_ter_arany']].copy()


    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    db_park_azonosito = pd.read_sql_query("SELECT park_ID, park_nev FROM park_azonosito;", conn)
    db_cimviselo_azonosito = pd.read_sql_query("SELECT cimviselo_ID, cimviselo_nev FROM cimviselo_azonosito;", conn)

    
    try:
        with conn:
            park_tocheck = table_park_azonosito_data['park_nev'].iloc[0]
            park_id = match_park_ID(park_tocheck, db_park_azonosito, threshold=0.85)
            if park_id is not None:
                print(f"'{park_tocheck}' hasonló park név találat: '{db_park_azonosito.loc[db_park_azonosito['park_ID'] == park_id, 'park_nev'].values[0]}' (park_ID: {park_id})")
            else:
                #park_id = cursor.execute("SELECT IFNULL(MAX(park_ID), 0) + 1 FROM park_azonosito").fetchone()[0]
                #table_park_azonosito_data['park_ID'] = park_id
                table_park_azonosito_data.to_sql('park_azonosito', conn, if_exists='append', index=False)
                park_id = cursor.execute("SELECT park_ID FROM park_azonosito WHERE park_nev = ?", (park_tocheck,)).fetchone()[0]
                print(f"Új park név hozzáadva: '{park_tocheck}') (park_ID: {park_id})")
       
            
            cimviselo_tocheck = table_cimviselo_data['cimviselo_nev'].iloc[0]
            cimviselo_id = match_cimviselo_ID(cimviselo_tocheck, db_cimviselo_azonosito, threshold=0.85)
            if cimviselo_id is not None:
                print(f"'{cimviselo_tocheck}' hasonló címviselő név találat: '{db_cimviselo_azonosito.loc[db_cimviselo_azonosito['cimviselo_ID'] == cimviselo_id, 'cimviselo_nev'].values[0]}' (cimviselo_ID: {cimviselo_id})")
            else:                     
                table_cimviselo_azonosito_data.to_sql('cimviselo_azonosito', conn, if_exists='append', index=False)
                cimviselo_id = cursor.execute("SELECT cimviselo_ID FROM cimviselo_azonosito WHERE cimviselo_nev = ?", (cimviselo_tocheck,)).fetchone()[0]
                print(f"Új címviselő név hozzáadva: '{cimviselo_tocheck}') (cimviselo_ID: {cimviselo_id})")
                

            table_alapadat_data['park_ID'] = park_id
            table_alapadat_data['cimviselo_ID'] = cimviselo_id
            table_alapadat_data.to_sql('alapadat', conn, if_exists='append', index=False)   

            table_cimviselo_data['cimviselo_ID'] = cimviselo_id
            table_cimviselo_data.to_sql('cimviselo', conn, if_exists='append', index=False)


            table_management_data['park_ID'] = park_id
            table_management_data.to_sql('management', conn, if_exists='append', index=False)

            table_vallalkozasok_data['park_ID'] = park_id
            table_vallalkozasok_data.to_sql('vallalkozasok', conn, if_exists='append', index=False)

            table_infrastrukturafejlesztes_data['park_ID'] = park_id
            table_infrastrukturafejlesztes_data.to_sql('infrastrukturafejlesztes', conn, if_exists='append', index=False)

            table_EU_tamogatas_data['park_ID'] = park_id
            table_EU_tamogatas_data.to_sql('EU_tamogatas', conn, if_exists='append', index=False)   
        
            table_helyrajzi_szam_data['park_ID'] = park_id
            table_helyrajzi_szam_data.to_sql('helyrajzi_szam', conn, if_exists='append', index=False)

            table_kapcsolatok_data['park_ID'] = park_id
            table_kapcsolatok_data.to_sql('kapcsolatok', conn, if_exists='append', index=False)

            table_terulet_data['park_ID'] = park_id
            table_terulet_data.to_sql('terulet', conn, if_exists='append', index=False)


#az infra_fajta adatokat csak első alkalommal kell bevinni
            table_infra_fajta_data.to_sql('infra_fajta', conn, if_exists='append', index=False)

            infra_fajta_df = pd.read_sql_query("SELECT * FROM infra_fajta;", conn)  

            merged_infra_df = pd.merge(infra_fajta_df, table_infrastruktura_data, on=['infra_tipus', 'infra_nev'],how='left')
            final_merged_infra_df = merged_infra_df[['infra_ID','kapacitas','ellatott_ter','allapot','terv_fejlesztes_ev','terv_forras',]]
            clean_infra_data_df = final_merged_infra_df.dropna(subset=['ellatott_ter', 'allapot']).copy()
            clean_infra_data_df['park_ID'] = park_id
            clean_infra_data_df.to_sql('infrastruktura', conn, if_exists='append', index=False)

#a szolg_fajta adatokat csak első alkalommal kell bevinni
            table_szolg_fajta_data.to_sql('szolg_fajta', conn, if_exists='append', index=False)

            szolg_fajta_df = pd.read_sql_query("SELECT * FROM szolg_fajta;", conn)

            merged_szolg_df = pd.merge(szolg_fajta_df, table_szolgaltatasok_data, on=['szolg_tipus', 'szolg_nev'],how='left')
            final_merged_szolg_df = merged_szolg_df[['szolg_ID','szolg_tartalom','szolgaltato_fajta','szolgaltato_nev','szolg_kezdet',]]
            clean_szolg_data_df = final_merged_szolg_df.dropna(subset=['szolgaltato_fajta']).copy()
            clean_szolg_data_df['park_ID'] = park_id
            clean_szolg_data_df.to_sql('szolgaltatas', conn, if_exists='append', index=False)   

        print("Adatok sikeresen feltöltve az adatbázisba.")

    except Exception as e:
        print(f"Hiba történt az adatbázis művelet során: {e}")

    finally:
        conn.close()    


all_data = read_excel(excel_file, sheets_to_read)
validation_errors = data_validation(all_data)
if not validation_errors:
    transform_write_to_db(db, all_data, column_mapping, allapot_map)
else:
    print("Adatellenőrzési hibák:")
    for error in validation_errors:
        print(error)


#----------------------------------------------------------------
# LEKERDEZÉSEK

# Összehasonlító riport két év adatai alapján

def compare_years(db, year1, year2):
    
    conn = sqlite3.connect(db)
        
    query = """
    SELECT * FROM eves_osszefoglalo 
    WHERE adatszolgaltatas_ev IN (?, ?)
    ORDER BY adatszolgaltatas_ev ASC
    """
        
    df = pd.read_sql_query(query, conn, params=(str(year1), str(year2)))
    
    if len(df) < 2:
        print("Az egyik év adatai nem találhatók az adatbázisban.")
        return None

    # Esetleg különbség számítása az év1 és év2 között, ha szükséges
    df = df.set_index('adatszolgaltatas_ev')
    comparison = df.diff().iloc[1:].rename(index={str(year2): 'Különbség'})
    
    df_final_report = pd.concat([df, comparison])

    df_final_report.to_excel('osszehasonlitas.ods', engine='odf', index=False)

    conn.close()

    return df_final_report

database = 'ip.db'
year_a = 2023
year_b = 2024

results = compare_years(database, year_a, year_b)
print(results)

