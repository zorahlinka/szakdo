import pandas as pd
import sqlite3


excel_file = '/home/peti/Dokumentumok/gdf/kerdoiv/kerdoiv_valasz_1.ods'
db = '/home/peti/Dokumentumok/gdf/IP'
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
    'Önálló kutatási tevékenység': 'k+f_tevekenyseg',
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
    'Saját forrás (millió Ft)': 'sajat_forras',
    'Állami támogatás (millió Ft)': 'allami_forras',
    'Önkormányzati támogatás (millió Ft)': 'onkormanyzati_forras',
    'EU támogatás (millió Ft)': 'EU_forras',
    'Bankhitel (millió Ft)': 'bankhitel',
    'Tagi kölcsön (millió Ft)': 'tagi_kolcson',
    'Tőkeemelés (millió Ft)': 'tokeemeles',
    'Egyéb (millió Ft)': 'egyeb_forras',
    'ÖSSZES forrás (millió Ft)': 'osszes_forras',
    'Felelős neve': 'management_nev',
    'Jognyilatkozatra jogosult': 'jognyilatkozat',
    'Operatív felelős': 'operativ',
    'Beosztása': 'management_beosztas',
    'Telefon': 'management_tel',
    'Felelős e-mail': 'management_email',
    'Operatív program': 'op',
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

all_data= pd.read_excel(excel_file, engine='odf', sheet_name=sheets_to_read, skiprows=1)

df_adatok = all_data['Adatok'].rename(columns=column_mapping)
df_management = all_data['Management'].rename(columns=column_mapping)
df_EU_tamogatas = all_data['Elnyert EU-s támogatás'].rename(columns=column_mapping)
df_infrastruktura = all_data['Infrastruktúra'].rename(columns=column_mapping)
df_szolgaltatasok = all_data['Szolgáltatások'].rename(columns=column_mapping)

table_cimviselo_data = df_adatok[['cimviselo_nev', 'cimviselo_foglalkoztatott', 'cimviselo_cim', 'osszetetel_allam', 'osszetetel_onkormanyzat', 'osszetetel_belfoldi_magan', 'osszetetel_kulfoldi', 'osszetetel_egyeb',]]
table_alapadat_data = df_adatok[['park_nev', 'ip_cimszerzes', 'tp_cimszerzes', 'park_email', 'park_telepules', 'park_utca', 'park_iranyitoszam', 'park_varmegye', 'park_hrsz', 'park_regio', 'osszterulet', 'hasznosithato_ter', 'beepitett_ter', 'hasznosithato_szabad_ter', 'hasznosithato_szabad_arany', 'parkolo', 'zoldterulet', 'berbeadott_ter_arany', 'eladott_ter_arany', 'kamara', 'klaszter', 'oktatas_kozep', 'munkaugy', 'civil', 'ip', 'onkormanyzat', 'fejlesztesi_ugynokseg', 'export_ugynokseg', 'kulfoldi_ip', 'nemzetkozi_projekt', 'oktatas_felso', 'kutatointezet', 'k+f_tevekenyseg', 'uj_technologia' , 	'sajat_szolg_arany' , 	'kiszervezett_szolg_arany']].copy()
table_vallalkozasok_data = df_adatok[['vallalkozasok_terulet', 'vallalkozasok_szama', 'vallalkozasok_foglalkoztatott', 'beruhazasi_ertek', 'arbevetel', 'exportarany', 'kkv_szam', 'nagyvall_szam', 'egyeb_vall_szam']].copy()
table_infrastrukturafejlesztes_data = df_adatok[['sajat_forras', 'allami_forras', 'onkormanyzati_forras', 'EU_forras', 'bankhitel', 'tagi_kolcson', 'tokeemeles', 'egyeb_forras', 'osszes_forras']].copy()
table_management_data = df_management[['management_nev','jognyilatkozat','operativ','management_beosztas','management_tel','management_email']]
table_EU_tamogatas_data = df_EU_tamogatas[['op','tamogatas_tartalom','intenzitas','EU_osszkoltseg']]
table_infra_fajta_data = df_infrastruktura[['infra_tipus','infra_nev']]
table_infrastruktura_data = df_infrastruktura[['infra_tipus','infra_nev','kapacitas','ellatott_ter','allapot','terv_fejlesztes_ev','terv_forras']]
table_szolg_fajta_data = df_szolgaltatasok[['szolg_tipus','szolg_nev']]
table_szolgaltatasok_data = df_szolgaltatasok[['szolg_tipus','szolg_nev','szolg_tartalom','szolgaltato_fajta','szolgaltato_nev','szolg_kezdet']]

conn = sqlite3.connect(db)
cursor = conn.cursor()

#cursor.execute("PRAGMA foreign_keys = ON;")

cimviselo_tocheck = table_cimviselo_data['cimviselo_nev'].iloc[0]
cursor.execute("SELECT cimviselo_ID FROM cimviselo WHERE cimviselo_nev = ?", (cimviselo_tocheck,))
result = cursor.fetchone()
if result:
    cimviselo_id = result[0]
    cols = ", ".join([f"{col} = ?" for col in table_cimviselo_data.columns])
    vals = table_cimviselo_data.iloc[0].tolist() + [cimviselo_id]
    cursor.execute(f"UPDATE cimviselo SET {cols} WHERE cimviselo_ID = ?", vals)
else:
    table_cimviselo_data.to_sql('cimviselo', conn, if_exists='append', index=False)
    cimviselo_id = cursor.execute("SELECT last_insert_rowid()").fetchone()[0]

table_management_data['cimviselo_ID'] = cimviselo_id
table_alapadat_data['cimviselo_ID'] = cimviselo_id

table_management_data.to_sql('management', conn, if_exists='append', index=False)
table_alapadat_data.to_sql('alapadat', conn, if_exists='append', index=False)
                                                                        
park_id = cursor.execute("SELECT last_insert_rowid()").fetchone()[0]

table_vallalkozasok_data['park_ID'] = park_id
table_infrastrukturafejlesztes_data['park_ID'] = park_id
table_EU_tamogatas_data['park_ID'] = park_id

table_vallalkozasok_data.to_sql('vallalkozasok', conn, if_exists='append', index=False)
table_infrastrukturafejlesztes_data.to_sql('infrastrukturafejlesztes', conn, if_exists='append', index=False)
table_EU_tamogatas_data.to_sql('EU_tamogatas', conn, if_exists='append', index=False)

#table_infra_fajta_data.to_sql('infra_fajta', conn, if_exists='append', index=False)

infra_fajta_df = pd.read_sql_query("SELECT * FROM infra_fajta;", conn)  

merged_infra_df = pd.merge(infra_fajta_df, table_infrastruktura_data, on=['infra_tipus', 'infra_nev'],how='left')
final_merged_infra_df = merged_infra_df[['infra_ID','kapacitas','ellatott_ter','allapot','terv_fejlesztes_ev','terv_forras',]]
clean_infra_data_df = final_merged_infra_df.dropna(subset=['ellatott_ter', 'allapot']).copy()
clean_infra_data_df['park_ID'] = park_id
clean_infra_data_df.to_sql('infrastruktura', conn, if_exists='append', index=False)

#table_szolg_fajta_data.to_sql('szolg_fajta', conn, if_exists='append', index=False)

szolg_fajta_df = pd.read_sql_query("SELECT * FROM szolg_fajta;", conn)

merged_szolg_df = pd.merge(szolg_fajta_df, table_szolgaltatasok_data, on=['szolg_tipus', 'szolg_nev'],how='left')
final_merged_szolg_df = merged_szolg_df[['szolg_ID','szolg_tartalom','szolgaltato_fajta','szolgaltato_nev','szolg_kezdet',]]
clean_szolg_data_df = final_merged_szolg_df.dropna(subset=['szolgaltato_fajta']).copy()
clean_szolg_data_df['park_ID'] = park_id
clean_szolg_data_df.to_sql('szolgaltatas', conn, if_exists='append', index=False)

conn.commit()
conn.close()