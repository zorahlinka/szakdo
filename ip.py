from email import errors

import pandas as pd
import sqlite3


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
    "text_255": ['text', 'length:255'],
    "text_500": ['text', 'length:500'],
    "year_max": ['numeric', 'whole_number', 'range:1980-current_year'],
    "year_min": ['numeric', 'whole_number', 'min:current_year'],
    "email": ['text', 'email_pattern'],
    "percentage": ['numeric', 'range:0-100'],
    "percentage_sum": ['numeric', 'range:0-100', 'sum:100'],
    "positive_real": ['numeric', 'real', 'min:0'],
    "positive_integer": ['numeric', 'whole_number', 'min:0'],
    "boolean": ['boolean', 'values:igen-nem'],
    "varmegye": ['text', 'values:' + ','.join(park_varmegye)],
    "regio": ['text', 'values:' + ','.join(park_regio)],
    "szolgaltato_fajta": ['text', 'values:' + ','.join(szolgaltato_fajta)],
    "telephone": ['text', 'telephone_pattern'],
       
}

# beolvasás
def read_excel(excel_file, sheets_to_read):
    try:   
        all_data = pd.read_excel(excel_file, sheet_name=sheets_to_read, engine='odf')
        return all_data
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
      


def transform_data(all_data, column_mapping, allapot_map):  
    try:   
        df_adatok = all_data['Adatok'].rename(columns=column_mapping)
        df_management = all_data['Management'].rename(columns=column_mapping)
        df_EU_tamogatas = all_data['Elnyert EU-s támogatás'].rename(columns=column_mapping)
        df_infrastruktura = all_data['Infrastruktúra'].rename(columns=column_mapping)
        df_szolgaltatasok = all_data['Szolgáltatások'].rename(columns=column_mapping)

        df_infrastruktura['allapot'] = df_infrastruktura['allapot'].map(allapot_map)
        
        transformed_data = {
            'table_cimviselo_data': df_adatok[['cimviselo_nev', 'cimviselo_foglalkoztatott', 'cimviselo_cim', 'osszetetel_allam', 'osszetetel_onkormanyzat', 'osszetetel_belfoldi_magan', 'osszetetel_kulfoldi', 'osszetetel_egyeb',]].copy(),
            'table_alapadat_data': df_adatok[['park_nev', 'ip_cimszerzes', 'tp_cimszerzes', 'park_email', 'park_telepules', 'park_utca', 'park_iranyitoszam', 'park_varmegye', 'park_hrsz', 'park_regio', 'osszterulet', 'hasznosithato_ter', 'beepitett_ter', 'hasznosithato_szabad_ter', 'hasznosithato_szabad_arany', 'parkolo', 'zoldterulet', 'berbeadott_ter_arany', 'eladott_ter_arany', 'kamara', 'klaszter', 'oktatas_kozep', 'munkaugy', 'civil', 'ip', 'onkormanyzat', 'fejlesztesi_ugynokseg', 'export_ugynokseg', 'kulfoldi_ip', 'nemzetkozi_projekt', 'oktatas_felso', 'kutatointezet', 'kf_tevekenyseg', 'uj_technologia' ,    'sajat_szolg_arany' , 'kiszervezett_szolg_arany', 'park_honlap']].copy(),
            'table_vallalkozasok_data': df_adatok[['vallalkozasok_terulet', 'vallalkozasok_szama', 'vallalkozasok_foglalkoztatott', 'beruhazasi_ertek', 'arbevetel', 'exportarany', 'kkv_szam', 'nagyvall_szam', 'egyeb_vall_szam','vallalkozasok_ev']].copy(),
            'table_infrastrukturafejlesztes_data': df_adatok[['sajat_forras', 'allami_forras', 'onkormanyzati_forras', 'EU_forras', 'bankhitel', 'tagi_kolcson', 'tokeemeles', 'egyeb_forras', 'osszes_forras', 'felhasznalas_ev']].copy(),
            'table_management_data': df_management[['management_nev','jognyilatkozat','operativ','management_beosztas','management_tel','management_email']].copy(),
            'table_EU_tamogatas_data': df_EU_tamogatas[['op','tamogatas_ev','tamogatas_tartalom','intenzitas','EU_osszkoltseg']].copy(),
            'table_infra_fajta_data': df_infrastruktura[['infra_tipus','infra_nev']].copy(),
            'table_infrastruktura_data': df_infrastruktura[['infra_tipus','infra_nev','kapacitas','ellatott_ter','allapot','terv_fejlesztes_ev','terv_forras']].copy(),
            'table_szolg_fajta_data': df_szolgaltatasok[['szolg_tipus','szolg_nev']].copy(),
            'table_szolgaltatasok_data': df_szolgaltatasok[['szolg_tipus','szolg_nev','szolg_tartalom','szolgaltato_fajta','szolgaltato_nev','szolg_kezdet']].copy(),
            'table_helyrajzi_szam_data': (df_adatok['park_hrsz'].astype(str).str.split(',').explode().str.strip().to_frame(name='park_hrsz').copy())
        }   
        return transformed_data
    except Exception as e:
        print(f"Hiba történt az átalakítás során: {e}") 

def write_to_db(db, transformed_data):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    try:
        with conn:
            cimviselo_tocheck = table_cimviselo_data['cimviselo_nev'].iloc[0]
            cursor.execute("SELECT DISTINCT cimviselo_ID FROM cimviselo WHERE cimviselo_nev = ?", (cimviselo_tocheck,))
            result = cursor.fetchone()
            if result:
                cimviselo_id = result[0]
                table_cimviselo_data['cimviselo_ID'] = cimviselo_id
                table_cimviselo_data.to_sql('cimviselo', conn, if_exists='append', index=False)
            else:
                new_cimviselo_id = cursor.execute("SELECT IFNULL(MAX(cimviselo_ID), 0) + 1 FROM cimviselo").fetchone()[0]
                table_cimviselo_data['cimviselo_ID'] = new_cimviselo_id
                table_cimviselo_data.to_sql('cimviselo', conn, if_exists='append', index=False)
                cimviselo_id = new_cimviselo_id
    

            park_tocheck = table_alapadat_data['park_nev'].iloc[0]
            cursor.execute("SELECT DISTINCT park_ID FROM alapadat WHERE park_nev = ?", (park_tocheck,))
            result = cursor.fetchone()
            if result:
                park_id = result[0]
                table_alapadat_data['cimviselo_ID'] = cimviselo_id
                table_alapadat_data['park_ID'] = park_id
                table_alapadat_data.to_sql('alapadat', conn, if_exists='append', index=False)
            else:
                table_alapadat_data['cimviselo_ID'] = cimviselo_id
                new_park_id = cursor.execute("SELECT IFNULL(MAX(park_ID), 0) + 1 FROM alapadat").fetchone()[0]
                table_alapadat_data['park_ID'] = new_park_id
                table_alapadat_data.to_sql('alapadat', conn, if_exists='append', index=False)
                park_id = new_park_id


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


if all_data is not None:
    if not errors:
        transformed_data = transform_data(all_data, column_mapping, allapot_map)
        if transformed_data is not None:
            write_to_db(db, transformed_data)
