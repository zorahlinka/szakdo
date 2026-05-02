import argparse
import os
import pandas as pd
import sqlite3
import datetime
from thefuzz import process, fuzz

sheets_to_read = ['Adatok', 'Management', 'Elnyert EU-s támogatás', 'Infrastruktúra', 'Szolgáltatások']

column_mapping = {
    'Ipari Park neve': 'park_nev',
    'Ipari park cím elnyerésének éve': 'ip_cimszerzes',
    'Technológiai park cím elnyerésének éve': 'tp_cimszerzes',
    'Email': 'park_email',
    'Honlap': 'park_honlap',
    'Mely évre vonatkoznak az általános adatok?': 'alapadat_ev',
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
    'Mely évre vonatkoznak a területi adatok?': 'terulet_ev',
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
    'Mely évre vonatkoznak a kapcsolati adatok?': 'kapcsolatok_ev',
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
    'Megbízatás kezdete (ha ismert)': 'management_kezdet',
    'Megbízatás vége (ha ismert)': 'management_vege',
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

eldontendo_map = {
    'igen': '1',
    'nem': '0',
}

eldontendo = ['kamara', 'klaszter', 'oktatas_kozep', 'munkaugy', 'civil', 'ip', 'onkormanyzat', 'fejlesztesi_ugynokseg', 'export_ugynokseg', 'kulfoldi_ip', 'nemzetkozi_projekt', 'oktatas_felso', 'kutatointezet', 'kf_tevekenyseg', 'uj_technologia']


allowed_columns = {
            'park_azonosito': ['park_nev'],
            'cimviselo_azonosito': ['cimviselo_nev'],
            'alapadat': ['ip_cimszerzes', 'tp_cimszerzes', 'park_email', 'park_telepules', 'park_utca', 'park_iranyitoszam', 'park_varmegye', 'park_regio', 'sajat_szolg_arany', 'kiszervezett_szolg_arany', 'park_honlap', 'alapadat_ev', 'park_ID', 'cimviselo_ID'],
            'cimviselo': ['cimviselo_foglalkoztatott', 'cimviselo_cim', 'osszetetel_allam', 'osszetetel_onkormanyzat', 'osszetetel_belfoldi_magan', 'osszetetel_kulfoldi', 'osszetetel_egyeb', 'cimviselo_ID'],
            'management': ['management_nev', 'jognyilatkozat', 'operativ', 'management_beosztas', 'management_tel', 'management_email', 'management_kezdet', 'management_vege', 'park_ID'],
            'vallalkozasok': ['vallalkozasok_terulet', 'vallalkozasok_szama', 'vallalkozasok_foglalkoztatott', 'beruhazasi_ertek', 'arbevetel', 'exportarany', 'kkv_szam', 'nagyvall_szam', 'egyeb_vall_szam', 'vallalkozasok_ev', 'park_ID'],
            'infrastrukturafejlesztes': ['felhasznalas_ev', 'sajat_forras', 'allami_forras', 'onkormanyzati_forras', 'EU_forras', 'bankhitel', 'tagi_kolcson', 'tokeemeles', 'egyeb_forras', 'osszes_forras', 'park_ID'],
            'EU_tamogatas': ['op', 'tamogatas_ev', 'tamogatas_tartalom', 'intenzitas', 'EU_osszkoltseg', 'park_ID'],
            'helyrajzi_szam': ['park_hrsz', 'park_ID'],
            'kapcsolatok': ['kamara', 'klaszter', 'oktatas_kozep', 'munkaugy', 'civil', 'ip', 'onkormanyzat', 'fejlesztesi_ugynokseg', 'export_ugynokseg', 'kulfoldi_ip', 'nemzetkozi_projekt', 'oktatas_felso', 'kutatointezet', 'kf_tevekenyseg', 'uj_technologia', 'kapcsolatok_ev', 'park_ID'],
            'terulet': ['osszterulet', 'hasznosithato_ter', 'beepitett_ter', 'hasznosithato_szabad_ter', 'hasznosithato_szabad_arany', 'parkolo', 'zoldterulet', 'berbeadott_ter_arany', 'eladott_ter_arany', 'terulet_ev', 'park_ID'],
            'infrastruktura': ['infra_ID', 'kapacitas', 'ellatott_ter', 'allapot', 'terv_fejlesztes_ev', 'terv_forras', 'park_ID'],
            'szolgaltatas': ['szolg_ID', 'szolg_tartalom', 'szolgaltato_fajta', 'szolgaltato_nev', 'szolg_kezdet', 'park_ID'],
        }

# Listák az adatellenőrzéshez
park_varmegye = ['Bács-Kiskun', 'Baranya', 'Békés', 'Borsod-Abaúj-Zemplén', 'Csongrád-Csanád', 'Fejér', 'Győr-Moson-Sopron', 'Hajdú-Bihar', 'Heves', 'Jász-Nagykun-Szolnok', 'Komárom-Esztergom', 'Nógrád', 'Pest', 'Somogy', 'Szabolcs-Szatmár-Bereg', 'Tolna', 'Vas', 'Veszprém', 'Zala', 'Budapest']
park_regio = ['Közép-Magyarország', 'Közép-Dunántúl', 'Nyugat-Dunántúl', 'Dél-Dunántúl', 'Észak-Magyarország', 'Észak-Alföld', 'Dél-Alföld']
szolgaltato_fajta = ['A park üzemeltetője', 'Betelepült vállalkozás', 'Egyéb']
allapot = ['Megfelelő', 'Bővítendő', 'Felújítandó']

# Adatellenőrzéshez szükséges szabályok oszlopok szerint:
val_rules = {
    "text_255": ["Ipari Park neve", "Címviselő szervezet neve", "Címviselő szervezet címe", "Település", "Út/utca", "Felelős neve", "Beosztása", "Operatív program", "Infrastruktúra típusa", "Infrastruktúra neve", "Szolgáltatás típusa", "Szolgáltatás neve", "Szolgáltató szervezet neve"],
    "text_500": ["Szolgáltatás tartalma", "Projekt tartalom" ],
    "date": ["Megbízatás kezdete (ha ismert)", "Megbízatás vége (ha ismert)"],
    "year_max": ["Ipari park cím elnyerésének éve", "Technológiai park cím elnyerésének éve", "Mely évre vonatkoznak az adatok?", "Felhasználás éve", "Szolgáltatás kezdete", "Odaítélés éve", "Mely évre vonatkoznak a kapcsolati adatok?", "Mely évre vonatkoznak a területi adatok?", "Mely évre vonatkoznak az általános adatok?"],
    "year_min": ["Tervezett fejlesztés éve"],
    "email": ["Email", "Felelős e-mail"],
    "percentage": ["Állami", "Önkormányzati", "Belföldi magán", "Külföldi", "Egyéb", "Hasznosítható szabad terület aránya (%)", "Betelepített területeit bérbe adja (%)", "Betelepített területeit eladta (%)", "Maga nyújtja (%)", "Kiszervezi (%)", "Exportarány (%)", "Támogatási intenzitás"],
    "positive_integer": ["Címviselő szervezet foglalkoztatottak száma", "Irányítószám", "Vállalkozások száma", "Foglalkoztatottak létszáma (fő)", "KKV-k száma", "Nagyvállalatok száma", "Egyéb vállalkozások száma"],
    "positive_real": ["Összterület (ha)", "Hasznosítható terület (ha)", "Betelepített terület (ha)", "Hasznosítható szabad terület (ha)", "Parkolóhely területe (m2)", "Zöldterületek, parkok (m2)", "Vállalkozások területe (ha)", "Beruházási érték (millió Ft)", "Árbevétel (millió Ft)", "Saját forrás (millió Ft)", "Állami támogatás (millió Ft)", "Önkormányzati támogatás (millió Ft)", "EU támogatás (millió Ft)", "Bankhitel (millió Ft)", "Tagi kölcsön (millió Ft)", "Tőkeemelés (millió Ft)", "Egyéb (millió Ft)", "ÖSSZES forrás (millió Ft)", "Kapacitás", "Ellátott terület nagysága (ha)", "Tervezett forrás", "Összköltség (millió Ft)"],
    "boolean": ["Kamarák", "Klaszterek", "Középfokú oktatási intézmények", "Munkaügyi központ", "Szakmai civil szervezetek", "Más ipari parkok", "Önkormányzat", "Állami fejlesztési ügynökségek", "Magyar Exportfejlesztési Ügynökség", "Külföldi ipari park", "Részvétel nemzetközi projektekben", "Együttműködés felsőoktatási intézménnyel", "Együttműködés kutatóintézettel", "Önálló kutatási tevékenység", "Piacközeli stádiumban lévő technológiák alkalmazása", "Jognyilatkozatra jogosult", "Operatív felelős"],
    "varmegye": ["Vármegye"],
    "regio": ["Régió"],
    "szolgaltato_fajta": ["Szolgáltató szervezet típusa"],
    "allapot": ["Állapota (Megfelelő/ Bővítendő/ Felújítandó)"],
    "telephone": ["Telefon"],
}
# Segédfüggvények az adatellenőrzéshez

def normalize_text(value):
    # Szöveges értéket normalizál: eltávolítja a vezető és záró szóközöket.
    # Ha az érték hiányzó (NaN/None), None-t ad vissza, hogy az ellenőrzések
    # egységesen kezelhessék a kitöltetlen cellákat.
    # Visszatérési érték: str vagy None
    if pd.isna(value):
        return None
    return str(value).strip()


def is_valid_date_value(value):
    # Megvizsgálja, hogy az adott érték érvényes dátumnak tekinthető-e.
    # Az adatellenőrzés során azokat a mezőket ellenőrzi, amelyekbe dátumot
    # várunk (pl. megbízatás kezdete/vége).
    #
    # Elfogadott típusok:
    #   - pd.Timestamp, datetime.date, datetime.datetime – ezek natív dátumok,
    #     az Excel/ODS fájlból automatikusan ilyen típusként olvasódnak be.
    #   - Szöveges érték, amely pd.to_datetime() által értelmezhető dátummá
    #     alakítható (pl. "2023-05-01").
    #
    # Elutasított típusok:
    #   - int/float – a számokat szándékosan elutasítjuk, mert egy puszta szám
    #     (pl. 2023) nem dátum, még ha egész is; az évellenőrzés a year_value()
    #     függvény feladata.
    #   - Bármilyen szöveg, amelyet a pandas nem tud dátummá alakítani.
    #
    # Visszatérési érték: True (érvényes) vagy False (érvénytelen)
    if pd.isna(value):
        # Hiányzó érték: nem jelent hibát, a kötelezőség ellenőrzése máshol történik.
        return True
    if isinstance(value, (pd.Timestamp, datetime.date, datetime.datetime)):
        # Natív dátumtípus – biztosan érvényes.
        return True
    if isinstance(value, (int, float)):
        # Nyers szám nem fogadható el dátumként.
        return False
    # Szöveges érték esetén megpróbáljuk dátummá alakítani; ha nem sikerül,
    # a to_datetime NaT-ot ad vissza, amelyet isna() True-ként értékel.
    return not pd.isna(pd.to_datetime(value, errors='coerce'))


def year_value(value):
    # Tetszőleges típusú értékből megpróbál egész évet kinyerni.
    # Az adatellenőrzés évmező-szabályaiban (year_max, year_min) használjuk,
    # ahol az ODS-ből beolvasott évek lehetnek Timestamp, egész szám vagy
    # lebegőpontos szám (pl. 2023.0) formátumban egyaránt.
    #
    # Visszatérési érték: int (a kinyert év) vagy None (ha az értékből nem
    # nyerhető ki értelmes egész év).
    if pd.isna(value):
        # Hiányzó érték – nincs mit visszaadni.
        return None
    if isinstance(value, (pd.Timestamp, datetime.date, datetime.datetime)):
        # Natív dátumtípusoknál a .year attribútum közvetlenül elérhető.
        return value.year
    # Nem dátumtípus esetén numerikus értékké próbáljuk alakítani.
    # Ha az átalakítás sikertelen (pl. szöveges érték), NaN-t kapunk.
    numeric = pd.to_numeric(value, errors='coerce')
    if pd.isna(numeric):
        # Nem értelmezhető számként – nincs visszaadható év.
        return None
    # Csak akkor fogadjuk el évként, ha az érték egész szám (pl. 2023.0 → 2023).
    # Törtszámot (pl. 2023.5) nem tekintünk érvényes évnek.
    if float(numeric).is_integer():
        return int(numeric)
    return None


# Beolvasás
def read_excel(excel_file, SHEETS):
    # Beolvassa a megadott ODS/Excel fájl megadott munkalapjait egy szótárba,
    # ahol a kulcsok a munkalapneveknek, az értékek az azokhoz tartozó
    # pandas DataFrame-eknek felelnek meg.
    #
    # Technikai megjegyzések:
    #   - engine='odf': az .ods (OpenDocument Spreadsheet) formátum olvasásához
    #     szükséges; az odfpy csomag telepítését igényli.
    #   - header=1: a fejlécsor a fájlban a 2. sorban van (0-alapú indexeléssel 1),
    #     az 1. sor általában egy cím- vagy megjegyzéssor, amelyet kihagyunk.
    #
    # Visszatérési érték:
    #   - dict[str, pd.DataFrame]: sikeres olvasás esetén
    #   - None: ha az olvasás bármilyen okból meghiúsul (fájl nem található,
    #     sérült fájl, hiányzó munkalap stb.)
    try:
        return pd.read_excel(excel_file, sheet_name=SHEETS, engine='odf', header=1)
    except Exception as e:
        print(f"Hiba történt az Excel fájl beolvasása során: {e}")
        return None

# Adatellenőrzés
def data_validation(all_data):
    # Az errors listába gyűjtjük az összes talált hibát; ha a függvény végén
    # üres marad, az adatok érvényesnek tekinthetők.
    errors = []
    # EPS: lebegőpontos összehasonlításhoz használt kis toleranciaérték,
    # hogy az összeadási kerekítési hibák ne okozzanak hamis riasztást.
    EPS = 1e-6

    # Minden munkalapot (sheet) egymás után ellenőrzünk.
    # A sheet_name az esetleges hibaüzenetben azonosítja, melyik lapról van szó.
    for sheet_name, df in all_data.items():

        # ---------------------------------------------------------------------
        # 1. OSZLOPONKÉNTI (VEKTORIZÁLT) ELLENŐRZÉSEK
        # Minden szabálytípushoz végigmegyünk a val_rules szótárban felsorolt
        # oszlopokon. A pandas maszkos megközelítés egyszerre értékeli ki az
        # összes sort, így gyorsabb, mint soronkénti iteráció.
        # ---------------------------------------------------------------------

        # Szöveges mezők hosszellenőrzése (max 255 karakter)
        for col in val_rules["text_255"]:
            if col in df.columns:
                mask = df[col].astype(str).str.len() > 255
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Túl hosszú (max 255).")

        # Hosszabb szöveges mezők ellenőrzése (max 500 karakter)
        for col in val_rules["text_500"]:
            if col in df.columns:
                mask = df[col].astype(str).str.len() > 500
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Túl hosszú (max 500).")

        # Dátum formátum ellenőrzése – az is_valid_date_value() segédfüggvény
        # elfogadja a Timestamp, date és datetime típusokat, szöveges dátumokat viszont nem.
        for col in val_rules["date"]:
            if col in df.columns:
                mask = df[col].notna() & ~df[col].apply(is_valid_date_value)
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Érvénytelen dátum formátum.")

        # Múltbeli vagy jelenbeli év ellenőrzése (1980 – aktuális év között kell lennie).
        # A year_value() segédfüggvény kezeli a Timestamp és numerikus formátumokat egyaránt.
        for col in val_rules["year_max"]:
            if col in df.columns:
                current_year = pd.Timestamp.now().year
                mask = df[col].notna() & df[col].apply(lambda x: year_value(x) is None or year_value(x) < 1980 or year_value(x) > current_year)
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Érvénytelen év (1980 - jelen).")

        # Jövőbeli év ellenőrzése – a tervezett fejlesztés éve nem lehet a jelennél korábbi.
        for col in val_rules["year_min"]:
            if col in df.columns:
                current_year = pd.Timestamp.now().year
                mask = df[col].notna() & df[col].apply(lambda x: year_value(x) is None or year_value(x) < current_year)
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Érvénytelen év (min jelen).")

        # E-mail cím formátum ellenőrzése egyszerű regex mintával.
        for col in val_rules["email"]:
            if col in df.columns:
                mask = df[col].notna() & (df[col].astype(str).str.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', na=False) == False)
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Érvénytelen email cím.")

        # Százalékos értékek tartomány-ellenőrzése (0–100 közé kell esnie).
        for col in val_rules["percentage"]:
            if col in df.columns:
                mask = df[col].notna() & ~df[col].apply(lambda x: isinstance(x, (int, float)) and 0 <= x <= 100)
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Érvénytelen százalék érték (0-100).")

        # Pozitív valós szám ellenőrzése (pl. területek, pénzügyi értékek; 0 megengedett).
        for col in val_rules["positive_real"]:
            if col in df.columns:
                mask = df[col].notna() & ~df[col].apply(lambda x: isinstance(x, (int, float)) and x >= 0)
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Érvénytelen pozitív valós érték (min 0).")

        # Pozitív egész szám ellenőrzése (pl. darabszámok, irányítószám; 0 megengedett).
        for col in val_rules["positive_integer"]:
            if col in df.columns:
                mask = df[col].notna() & ~df[col].apply(lambda x: isinstance(x, (int, float)) and x >= 0 and float(x).is_integer())
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Érvénytelen pozitív egész érték (min 0).")

        # Logikai (igen/nem) mezők ellenőrzése – csak ez a két érték megengedett.
        for col in val_rules["boolean"]:
            if col in df.columns:
                mask = df[col].notna() & ~df[col].astype(str).str.lower().isin(['igen', 'nem'])
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Érvénytelen érték (csak 'igen' vagy 'nem').")

        # Vármegye ellenőrzése az engedélyezett park_varmegye listával szemben.
        for col in val_rules["varmegye"]:
            if col in df.columns:
                mask = df[col].notna() & df[col].astype(str).apply(lambda x: x not in park_varmegye)
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Érvénytelen vármegye.")

        # Régió ellenőrzése az engedélyezett park_regio listával szemben.
        for col in val_rules["regio"]:
            if col in df.columns:
                mask = df[col].notna() & df[col].astype(str).apply(lambda x: x not in park_regio)
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Érvénytelen régió.")

        # Szolgáltató szervezet típusának ellenőrzése – kis-nagybetű érzéketlen összehasonlítás.
        for col in val_rules["szolgaltato_fajta"]:
            if col in df.columns:
                mask = df[col].notna() & ~df[col].astype(str).str.strip().str.lower().isin([s.lower() for s in szolgaltato_fajta])
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Érvénytelen szolgáltató szervezet típusa.")

        # Infrastruktúra állapot ellenőrzése – csak a három megengedett érték fogadható el.
        for col in val_rules["allapot"]:
            if col in df.columns:
                mask = df[col].notna() & ~df[col].astype(str).str.strip().str.lower().isin([s.lower() for s in allapot])
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Érvénytelen állapot (Megfelelő, Bővítendő, Felújítandó).")

        # Telefonszám formátum ellenőrzése – opcionális +, számok, szóköz, kötőjel és zárójelek.
        for col in val_rules["telephone"]:
            if col in df.columns:
                mask = df[col].notna() & (df[col].astype(str).str.match(r'^\+?[\d\s\-\(\)/]+$', na=False) == False)
                for idx in mask[mask].index:
                    line = idx + 2
                    errors.append(f"{sheet_name} - {line}. sor: '{col}' Érvénytelen telefonszám.")

        # ---------------------------------------------------------------------
        # 2. KERESZTOSZLOPOS (KOMPLEX) ELLENŐRZÉSEK
        # Ezek az ellenőrzések több oszlop értékét egyszerre vizsgálják, ezért
        # nem vectorizálhatók egyszerűen – soronként iterálunk. Minden szabály
        # csak akkor fut le, ha az érintett oszlopok mindegyike kitöltött (notna),
        # így hiányos sorok esetén nem keletkeznek hamis hibák.
        # ---------------------------------------------------------------------
        for idx, row in df.iterrows():
            line = idx + 2

            # Tulajdonosi összetétel összege pontosan 100% kell legyen.
            # (Állami + Önkormányzati + Belföldi magán + Külföldi + Egyéb = 100%)
            if pd.notna(row.get('Állami')) and pd.notna(row.get('Önkormányzati')) and pd.notna(row.get('Belföldi magán')) and pd.notna(row.get('Külföldi')) and pd.notna(row.get('Egyéb')):
                total_percentage = row['Állami'] + row['Önkormányzati'] + row['Belföldi magán'] + row['Külföldi'] + row['Egyéb']
                if abs(total_percentage - 100) > EPS:
                    errors.append(f"{sheet_name} - {line}. sor: 'Állami', 'Önkormányzati', 'Belföldi magán', 'Külföldi' és 'Egyéb' összege nem lehet nagyobb, mint 100%.")

            # A hasznosítható terület nem haladhatja meg a teljes összterületet.
            if pd.notna(row.get('Hasznosítható terület (ha)')) and pd.notna(row.get('Összterület (ha)')) and row['Hasznosítható terület (ha)'] > row['Összterület (ha)']:
                errors.append(f"{sheet_name} - {line}. sor: 'Hasznosítható terület (ha)' nem lehet nagyobb, mint 'Összterület (ha)'.")

            # A betelepített és a hasznosítható szabad terület együttesen nem haladhatja
            # meg a teljes hasznosítható területet.
            if pd.notna(row.get('Betelepített terület (ha)')) and pd.notna(row.get('Hasznosítható szabad terület (ha)')) and pd.notna(row.get('Hasznosítható terület (ha)')):
                if row['Betelepített terület (ha)'] + row['Hasznosítható szabad terület (ha)'] > row['Hasznosítható terület (ha)'] + EPS:
                    errors.append(f"{sheet_name} - {line}. sor: 'Betelepített terület (ha)' és 'Hasznosítható szabad terület (ha)' összege nem lehet nagyobb, mint 'Hasznosítható terület (ha)'.")

            # A bérbe adott és az eladott betelepített terület aránya együttesen legfeljebb 100% lehet.
            if pd.notna(row.get('Betelepített területeit bérbe adja (%)')) and pd.notna(row.get('Betelepített területeit eladta (%)')):
                if row['Betelepített területeit bérbe adja (%)'] + row['Betelepített területeit eladta (%)'] > 100 + EPS:
                    errors.append(f"{sheet_name} - {line}. sor: 'Betelepített területeit bérbe adja (%)' és 'Betelepített területeit eladta (%)' összege nem lehet nagyobb, mint 100%.")

            # Az összes forrás értékének egyeznie kell a részforrások összegével.
            # Ellenőrzés csak akkor fut, ha minden részforrás-oszlop kitöltött.
            if pd.notna(row.get('Összes forrás (millió Ft)')) and pd.notna(row.get('Saját forrás (millió Ft)')) and pd.notna(row.get('Állami támogatás (millió Ft)')) and pd.notna(row.get('Önkormányzati támogatás (millió Ft)')) and pd.notna(row.get('EU támogatás (millió Ft)')) and pd.notna(row.get('Bankhitel (millió Ft)')) and pd.notna(row.get('Tagi kölcsön (millió Ft)')) and pd.notna(row.get('Tőkeemelés (millió Ft)')) and pd.notna(row.get('Egyéb (millió Ft)')):
                total_sources = row['Saját forrás (millió Ft)'] + row['Állami támogatás (millió Ft)'] + row['Önkormányzati támogatás (millió Ft)'] + row['EU támogatás (millió Ft)'] + row['Bankhitel (millió Ft)'] + row['Tagi kölcsön (millió Ft)'] + row['Tőkeemelés (millió Ft)'] + row['Egyéb (millió Ft)']
                if abs(total_sources - row['Összes forrás (millió Ft)']) > EPS:
                    errors.append(f"{sheet_name} - {line}. sor: 'Összes forrás (millió Ft)' értéke nem egyezik meg a források összegével.")

            # A saját és kiszervezett szolgáltatás aránya együttesen legfeljebb 100% lehet.
            if pd.notna(row.get('Maga nyújtja (%)')) and pd.notna(row.get('Kiszervezi (%)')):
                if row['Maga nyújtja (%)'] + row['Kiszervezi (%)'] > 100 + EPS:
                    errors.append(f"{sheet_name} - {line}. sor: 'Maga nyújtja (%)' és 'Kiszervezi (%)' összege nem lehet nagyobb, mint 100%.")

    return errors

# Fuzzy matching segédfüggvények (park_ID és cimviselo_ID hozzárendeléséhez)

def match_park_ID(park_tocheck, db_park_azonosito, threshold=0.85):
    # Megkeresi, hogy a beérkező park neve egyezik-e (közelítőleg) valamelyik
    # már az adatbázisban szereplő aktív park nevével.
    #
    # Az egyezés mérésére a token_sort_ratio algoritmust használjuk: ez a módszer
    # a szavakat sorba rendezi összehasonlítás előtt, így a szórendbeli különbségek
    # (pl. "Győri Ipari Park" vs. "Ipari Park Győr") nem okoznak hamis eltérést.
    #
    # Visszatérési érték:
    #   - park_ID (int): ha a legjobb egyezés pontszáma eléri a küszöböt
    #   - None: ha nincs elég közeli egyezés, vagy az adatbázis üres
    #
    # Paraméterek:
    #   park_tocheck       – az ellenőrizendő park neve (str)
    #   db_park_azonosito  – az adatbázisból beolvasott aktív parkok DataFrame-je
    #                        (szükséges oszlopok: park_ID, park_nev)
    #   threshold          – minimális egyezési pontszám 0–100 skálán (alapértelmezett: 0.85)
    #                        Megjegyzés: a thefuzz 0–100 egész skálán ad pontszámot,
    #                        így a 0.85-ös alapértelmezett csak 0-nak felel meg;
    #                        a hívó helyek explicit küszöböt adnak meg (pl. 90).

    # Az extractOne visszaad egy (szöveg, pontszám, index) hármasát,
    # vagy None-t, ha a choices lista üres.
    match = process.extractOne(park_tocheck, db_park_azonosito['park_nev'], scorer=fuzz.token_sort_ratio)

    # Ha van találat és a pontszám eléri a küszöböt, visszaadjuk a megfelelő park_ID-t.
    # A match[2] a DataFrame sorának indexe, amellyel közvetlenül kikereshetjük az ID-t.
    if match and match[1] >= threshold:
        park_id = db_park_azonosito.loc[match[2], 'park_ID']
        return park_id

    # Nincs elég közeli egyezés – a hívó függvény új rekord beszúrásáról dönt.
    return None


def match_cimviselo_ID(cimviselo_tocheck, db_cimviselo, threshold=0.85):
    # A match_park_ID-hoz teljesen analóg logika, de a cimviselo_azonosito táblán
    # működik. A szervezetnevek változatosabbak és hosszabbak lehetnek, ezért
    # a hívó helyen alacsonyabb küszöb (85) kerül beállításra.
    #
    # Visszatérési érték:
    #   - cimviselo_ID (int): ha a legjobb egyezés eléri a küszöböt
    #   - None: ha nincs elég közeli egyezés
    #
    # Paraméterek:
    #   cimviselo_tocheck  – az ellenőrizendő szervezet neve (str)
    #   db_cimviselo       – az adatbázisból beolvasott címviselők DataFrame-je
    #                        (szükséges oszlopok: cimviselo_ID, cimviselo_nev)
    #   threshold          – minimális egyezési pontszám (alapértelmezett: 0.85)

    match = process.extractOne(cimviselo_tocheck, db_cimviselo['cimviselo_nev'], scorer=fuzz.token_sort_ratio)

    if match and match[1] >= threshold:
        cimviselo_id = db_cimviselo.loc[match[2], 'cimviselo_ID']
        return cimviselo_id

    return None


# Adatbázisba írás
def insert_df(transaction_conn, table_name, dataframe):
    # Egy DataFrame összes sorát beszúrja a megadott SQLite táblába,
    # egy már megnyitott tranzakción belül (ezért nem nyit új kapcsolatot).
    #
    # Biztonsági megfontolások:
    #   - Az allowed_columns szótárral ellenőrizzük, hogy a táblanév és minden
    #     oszlopnév szerepel-e az engedélyezőlistán. Ez megakadályozza, hogy
    #     nem várt oszlopok vagy táblanevek SQL-injekciós vektorrá váljanak.
    #   - Az isidentifier() ellenőrzés kizárja a speciális karaktereket tartalmazó
    #     neveket (pl. szóköz, kötőjel), amelyek az f-string alapú SQL-összerakásban
    #     biztonsági kockázatot jelenthetnek.
    #   - A tényleges értékeket paraméteres lekérdezéssel (? placeholder) adjuk át,
    #     így azok soha nem kerülnek bele közvetlenül az SQL-szövegbe.
    #
    # Paraméterek:
    #   transaction_conn  – aktív sqlite3 kapcsolat/tranzakció
    #   table_name        – a céltábla neve (str)
    #   dataframe         – a beszúrandó adatokat tartalmazó pandas DataFrame

    # Üres DataFrame esetén nincs teendő.
    if dataframe.empty:
        return

    # Táblanév ellenőrzése az engedélyezőlistával szemben.
    if table_name not in allowed_columns:
        raise ValueError(f"Invalid table name for insert: {table_name}")

    columns = list(dataframe.columns)
    for column in columns:
        # Minden oszlopnevet ellenőrzünk: szerepel-e az adott táblához tartozó
        # engedélyezett oszlopok listájában, és érvényes Python/SQL azonosító-e.
        if column not in allowed_columns[table_name]:
            raise ValueError(f"Invalid column name for table {table_name}: {column}")
        if not column.isidentifier():
            raise ValueError(f"Invalid SQL identifier: {column}")

    # A táblanevet is ellenőrizzük azonosító-érvényességre.
    if not table_name.isidentifier():
        raise ValueError(f"Invalid SQL identifier: {table_name}")

    # Az SQL utasítást dinamikusan állítjuk össze az oszlopnevekből.
    # Az oszlopneveket idézőjelbe tesszük, a táblanevet szintén,
    # az értékeket pedig ? placeholderekkel adjuk át – ez véd az SQL-injekció ellen.
    placeholders = ','.join(['?'] * len(columns))
    quoted_columns = ','.join(f'"{column}"' for column in columns)
    sql = f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES ({placeholders})'

    # Soronként iterálunk a DataFrame-en (itertuples gyorsabb, mint iterrows),
    # és minden pandas NA/NaN értéket None-ra alakítunk, hogy SQLite NULL-ként
    # kerüljön az adatbázisba.
    for row in dataframe.itertuples(index=False, name=None):
        values = tuple(None if pd.isna(value) else value for value in row)
        transaction_conn.execute(sql, values)

# Fő függvény az adatok transzformálásához és adatbázisba írásához
def transform_write_to_db(db, all_data, column_mapping):
    # -------------------------------------------------------------------------
    # 1. OSZLOPNEVEK ÁTNEVEZÉSE
    # Az összes munkalap DataFrame-jének oszlopneveit a column_mapping szótár
    # alapján magyarra (belső névkonvencióra) cseréljük.
    # -------------------------------------------------------------------------
    df_adatok = all_data['Adatok'].rename(columns=column_mapping)
    df_management = all_data['Management'].rename(columns=column_mapping)
    df_EU_tamogatas = all_data['Elnyert EU-s támogatás'].rename(columns=column_mapping)
    df_infrastruktura = all_data['Infrastruktúra'].rename(columns=column_mapping)
    df_szolgaltatasok = all_data['Szolgáltatások'].rename(columns=column_mapping)

    # -------------------------------------------------------------------------
    # 2. IGEN/NEM ÉRTÉKEK NUMERIKUS KÓDOLÁSA
    # Az eldontendo listában szereplő logikai (igen/nem) oszlopokat 1/0 értékre
    # alakítjuk az adatbázisba írás előtt.
    # -------------------------------------------------------------------------
    df_adatok[eldontendo] = df_adatok[eldontendo].replace(eldontendo_map)

    # -------------------------------------------------------------------------
    # 3. TÁBLÁNKÉNTI ADATKIVÁGÁS
    # Minden adatbázis-táblához külön DataFrame-t hozunk létre, csak a szükséges
    # oszlopokat megtartva. Ez biztosítja, hogy az insert_df() csak az engedélyezett
    # oszlopokat kapja meg.
    # -------------------------------------------------------------------------
    table_park_azonosito_data = df_adatok[['park_nev']].copy()
    table_cimviselo_azonosito_data = df_adatok[['cimviselo_nev']].copy()
    table_cimviselo_data = df_adatok[['cimviselo_foglalkoztatott', 'cimviselo_cim', 'osszetetel_allam', 'osszetetel_onkormanyzat', 'osszetetel_belfoldi_magan', 'osszetetel_kulfoldi', 'osszetetel_egyeb',]].copy()
    table_alapadat_data = df_adatok[['ip_cimszerzes', 'tp_cimszerzes', 'park_email', 'park_telepules', 'park_utca', 'park_iranyitoszam', 'park_varmegye', 'park_regio', 'sajat_szolg_arany' , 'kiszervezett_szolg_arany', 'park_honlap', 'alapadat_ev']].copy()
    table_alapadat_data = table_alapadat_data.rename(columns={'alapadatok_ev': 'alapadat_ev'})
    table_management_data = df_management[['management_nev', 'jognyilatkozat', 'operativ', 'management_beosztas', 'management_tel', 'management_email', 'management_kezdet', 'management_vege']].copy()
    table_EU_tamogatas_data = df_EU_tamogatas[['op', 'tamogatas_ev', 'tamogatas_tartalom', 'intenzitas', 'EU_osszkoltseg']].copy()
    table_vallalkozasok_data = df_adatok[['vallalkozasok_terulet', 'vallalkozasok_szama', 'vallalkozasok_foglalkoztatott', 'beruhazasi_ertek', 'arbevetel', 'exportarany', 'kkv_szam', 'nagyvall_szam', 'egyeb_vall_szam', 'vallalkozasok_ev']].copy()
    table_infra_fajta_data = df_infrastruktura[['infra_tipus','infra_nev']].copy()
    table_infrastruktura_data = df_infrastruktura[['infra_tipus','infra_nev','kapacitas','ellatott_ter','allapot','terv_fejlesztes_ev','terv_forras']].copy()
    table_infrastrukturafejlesztes_data = df_adatok[['felhasznalas_ev', 'sajat_forras', 'allami_forras', 'onkormanyzati_forras', 'EU_forras', 'bankhitel', 'tagi_kolcson', 'tokeemeles', 'egyeb_forras', 'osszes_forras']].copy()
    table_szolg_fajta_data = df_szolgaltatasok[['szolg_tipus','szolg_nev']].copy()
    table_szolgaltatasok_data = df_szolgaltatasok[['szolg_tipus','szolg_nev','szolg_tartalom','szolgaltato_fajta','szolgaltato_nev','szolg_kezdet']].copy()
    # A helyrajzi számok vesszővel elválasztott listát alkothatnak egy cellában;
    # ezeket külön sorokra bontjuk (explode), majd eltávolítjuk a szóközöket.
    table_helyrajzi_szam_data = df_adatok['park_hrsz'].astype(str).str.split(',').explode().str.strip().to_frame(name='park_hrsz').copy()
    table_kapcsolatok_data = df_adatok[['kamara', 'klaszter', 'oktatas_kozep', 'munkaugy', 'civil', 'ip', 'onkormanyzat', 'fejlesztesi_ugynokseg', 'export_ugynokseg', 'kulfoldi_ip', 'nemzetkozi_projekt', 'oktatas_felso', 'kutatointezet', 'kf_tevekenyseg', 'uj_technologia', 'kapcsolatok_ev']].copy()
    table_terulet_data = df_adatok[['osszterulet', 'hasznosithato_ter', 'beepitett_ter', 'hasznosithato_szabad_ter', 'hasznosithato_szabad_arany', 'parkolo', 'zoldterulet', 'berbeadott_ter_arany', 'eladott_ter_arany', 'terulet_ev']].copy()

    # -------------------------------------------------------------------------
    # 4. ADATBÁZISBA ÍRÁS
    # -------------------------------------------------------------------------
    try:
        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()

            # -----------------------------------------------------------------
            # 4a. PARK AZONOSÍTÁSA VAGY FELVÉTELE (park_azonosito tábla)
            # Fuzzy egyezéssel (90%-os küszöb) megkeressük, hogy a beérkező park
            # neve szerepel-e már az adatbázisban. Ha igen, az egyező park_ID-t
            # használjuk; ha nem, új rekordot szúrunk be és lekérdezzük az
            # automatikusan generált park_ID-t.
            # -----------------------------------------------------------------
            db_park_azonosito = pd.read_sql_query("SELECT park_ID, park_nev, aktiv FROM park_azonosito WHERE aktiv = 1;", conn)
            db_cimviselo_azonosito = pd.read_sql_query("SELECT cimviselo_ID, cimviselo_nev FROM cimviselo_azonosito;", conn)

            park_tocheck = table_park_azonosito_data['park_nev'].iloc[0]
            park_id = match_park_ID(park_tocheck, db_park_azonosito, threshold=90)
            if park_id is not None:
                print(f"'{park_tocheck}' hasonló park név találat: '{db_park_azonosito.loc[db_park_azonosito['park_ID'] == park_id, 'park_nev'].values[0]}' (park_ID: {park_id})")
            else:
                insert_df(conn, 'park_azonosito', table_park_azonosito_data)
                park_id = cursor.execute("SELECT park_ID FROM park_azonosito WHERE park_nev = ?", (park_tocheck,)).fetchone()[0]
                print(f"Új park név hozzáadva: '{park_tocheck}') (park_ID: {park_id})")

            # -----------------------------------------------------------------
            # 4b. CÍMVISELŐ AZONOSÍTÁSA VAGY FELVÉTELE (cimviselo_azonosito tábla)
            # A park azonosításához hasonló logika, de 85%-os egyezési küszöbbel,
            # mivel a szervezetnevek változatosabbak lehetnek.
            # -----------------------------------------------------------------
            cimviselo_tocheck = table_cimviselo_azonosito_data['cimviselo_nev'].iloc[0]
            cimviselo_id = match_cimviselo_ID(cimviselo_tocheck, db_cimviselo_azonosito, threshold=85)
            if cimviselo_id is not None:
                print(f"'{cimviselo_tocheck}' hasonló címviselő név találat: '{db_cimviselo_azonosito.loc[db_cimviselo_azonosito['cimviselo_ID'] == cimviselo_id, 'cimviselo_nev'].values[0]}' (cimviselo_ID: {cimviselo_id})")
            else:
                insert_df(conn, 'cimviselo_azonosito', table_cimviselo_azonosito_data)
                cimviselo_id = cursor.execute("SELECT cimviselo_ID FROM cimviselo_azonosito WHERE cimviselo_nev = ?", (cimviselo_tocheck,)).fetchone()[0]
                print(f"Új címviselő név hozzáadva: '{cimviselo_tocheck}') (cimviselo_ID: {cimviselo_id})")

            # -----------------------------------------------------------------
            # 4c. ALAPADAT ÉS CÍMVISELŐ RÉSZLETEK ÍRÁSA
            # Az alapadat táblába kerül a park általános éves összefoglalója;
            # a cimviselo táblába a szervezet összetételére és foglalkoztatottjaira
            # vonatkozó adatok. Mindkettőhöz hozzárendeljük a megfelelő azonosítókat.
            # -----------------------------------------------------------------
            table_alapadat_data['park_ID'] = park_id
            table_alapadat_data['cimviselo_ID'] = cimviselo_id
            insert_df(conn, 'alapadat', table_alapadat_data)

            table_cimviselo_data['cimviselo_ID'] = cimviselo_id
            insert_df(conn, 'cimviselo', table_cimviselo_data)

            # -----------------------------------------------------------------
            # 4d. EGYSZERŰ PARK_ID-ALAPÚ TÁBLÁK ÍRÁSA
            # Ezek a táblák közvetlenül a park_ID-hoz kötődnek, külön
            # segédtáblás (lookup) logika nélkül. Minden táblához hozzáfűzzük
            # a park_ID oszlopot, majd egyszerre írjuk be az adatokat.
            # -----------------------------------------------------------------
            tables = {
                'management': table_management_data,
                'vallalkozasok': table_vallalkozasok_data,
                'infrastrukturafejlesztes': table_infrastrukturafejlesztes_data,
                'EU_tamogatas': table_EU_tamogatas_data,
                'helyrajzi_szam': table_helyrajzi_szam_data,
                'kapcsolatok': table_kapcsolatok_data,
                'terulet': table_terulet_data
            }

            for name, df in tables.items():
                df['park_ID'] = park_id
                insert_df(conn, name, df)

            # -----------------------------------------------------------------
            # 4e. INFRASTRUKTÚRA FAJTAJEGYZÉK FRISSÍTÉSE ÉS RÉSZLETEK ÍRÁSA
            # Az infra_fajta tábla egy normalizált fajtajegyzék (típus + név párok).
            # Először meghatározzuk, mely (infra_tipus, infra_nev) kombinációk
            # újak a beérkező adatban, és csak ezeket szúrjuk be. Ezután az
            # aktuális fajtajegyzékkel összekapcsoljuk a részletes adatokat, és
            # csak a kitöltött sorokat (ellátott_ter és állapot nem üres) mentjük.
            # -----------------------------------------------------------------
            infra_fajta_df = pd.read_sql_query("SELECT * FROM infra_fajta;", conn)

            # Bal oldali összekapcsolással megkeressük, mely (infra_tipus, infra_nev) párok
            # szerepelnek a beérkező adatokban, de még nem léteznek az adatbázisban.
            # Az indicator=True hozzáad egy '_merge' oszlopot, amelynek értéke:
            #   'left_only'  – csak a beérkező adatban szerepel (új rekord)
            #   'both'       – mindkét helyen megvan (már létezik, kihagyandó)
            uj_infra_merge = table_infra_fajta_data.merge(
                infra_fajta_df[['infra_tipus', 'infra_nev']],
                on=['infra_tipus', 'infra_nev'],
                how='left',
                indicator=True
            )
            # Csak az újonnan beérkező, még nem szereplő sorokat tartjuk meg
            uj_infra_fajta = uj_infra_merge[uj_infra_merge['_merge'] == 'left_only'][['infra_tipus', 'infra_nev']]

            # Csak akkor írunk az adatbázisba, ha valóban van új rekord
            if not uj_infra_fajta.empty:
                uj_infra_fajta.to_sql('infra_fajta', conn, if_exists='append', index=False)
                # A teljes infra_fajta táblát újra beolvassuk, hogy az újonnan
                # kapott infra_ID értékek is elérhetők legyenek a következő lépésben
                infra_fajta_df = pd.read_sql_query("SELECT * FROM infra_fajta;", conn)

            merged_infra_df = pd.merge(infra_fajta_df, table_infrastruktura_data, on=['infra_tipus', 'infra_nev'],how='left')
            final_merged_infra_df = merged_infra_df[['infra_ID','kapacitas','ellatott_ter','allapot','terv_fejlesztes_ev','terv_forras',]]
            clean_infra_data_df = final_merged_infra_df.dropna(subset=['ellatott_ter', 'allapot']).copy()
            clean_infra_data_df['park_ID'] = park_id
            insert_df(conn, 'infrastruktura', clean_infra_data_df)

            # -----------------------------------------------------------------
            # 4f. SZOLGÁLTATÁS FAJTAJEGYZÉK FRISSÍTÉSE ÉS RÉSZLETEK ÍRÁSA
            # A szolg_fajta tábla az infra_fajta-hoz teljesen analóg logikával
            # működik: (szolg_tipus, szolg_nev) párok alkotják a fajtajegyzéket.
            # Csak a kitöltött sorokat (szolgaltato_fajta nem üres) mentjük.
            # -----------------------------------------------------------------
            szolg_fajta_df = pd.read_sql_query("SELECT * FROM szolg_fajta;", conn)

            # Bal oldali összekapcsolással megkeressük, mely (szolg_tipus, szolg_nev) párok
            # szerepelnek a beérkező adatokban, de még nem léteznek az adatbázisban.
            # Az indicator=True hozzáad egy '_merge' oszlopot, amelynek értéke:
            #   'left_only'  – csak a beérkező adatban szerepel (új rekord)
            #   'both'       – mindkét helyen megvan (már létezik, kihagyandó)
            uj_szolg_merge = table_szolg_fajta_data.merge(
                szolg_fajta_df[['szolg_tipus', 'szolg_nev']],
                on=['szolg_tipus', 'szolg_nev'],
                how='left',
                indicator=True
            )
            # Csak az újonnan beérkező, még nem szereplő sorokat tartjuk meg
            uj_szolg_fajta = uj_szolg_merge[uj_szolg_merge['_merge'] == 'left_only'][['szolg_tipus', 'szolg_nev']]

            # Csak akkor írunk az adatbázisba, ha valóban van új rekord
            if not uj_szolg_fajta.empty:
                uj_szolg_fajta.to_sql('szolg_fajta', conn, if_exists='append', index=False)
                # A teljes szolg_fajta táblát újra beolvassuk, hogy az újonnan
                # kapott szolg_ID értékek is elérhetők legyenek a következő lépésben
                szolg_fajta_df = pd.read_sql_query("SELECT * FROM szolg_fajta;", conn)

            merged_szolg_df = pd.merge(szolg_fajta_df, table_szolgaltatasok_data, on=['szolg_tipus', 'szolg_nev'],how='left')
            final_merged_szolg_df = merged_szolg_df[['szolg_ID','szolg_tartalom','szolgaltato_fajta','szolgaltato_nev','szolg_kezdet',]]
            clean_szolg_data_df = final_merged_szolg_df.dropna(subset=['szolgaltato_fajta']).copy()
            clean_szolg_data_df['park_ID'] = park_id
            insert_df(conn, 'szolgaltatas', clean_szolg_data_df)
    except Exception as e:
        print(f"Hiba történt az adatbázis művelet során: {e}")
        raise
    finally:
        conn.close()


def init_db(db_path):
    """Létrehozza az adatbázist a séma, nézetek és indexek alapján.

    Az SQL utasítások három fájlból kerülnek beolvasásra, amelyek a
    szkripttel azonos könyvtárban találhatók:
      - schema.sql          : táblák definíciói
      - CREATE VIEW latest.txt : legfrissebb adat nézetek
      - CREATE INDEX.txt    : teljesítményt javító indexek
    """
    # A szkript könyvtára – innen keressük a séma fájlokat
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Beolvasandó SQL fájlok sorrendben (a nézetek a tábláktól függenek)
    sql_fajlok = [
        os.path.join(base_dir, 'schema.sql'),
        os.path.join(base_dir, 'CREATE VIEW latest.txt'),
        os.path.join(base_dir, 'CREATE INDEX.txt'),
    ]

    sql_reszek = []
    for fajl in sql_fajlok:
        with open(fajl, 'r', encoding='utf-8') as f:
            sql_reszek.append(f.read())

    # Az összes SQL utasítást egyetlen szöveggé fűzzük össze
    combined_sql = '\n'.join(sql_reszek)

    # Adatbázis létrehozása és az összes utasítás végrehajtása
    with sqlite3.connect(db_path) as conn:
        conn.executescript(combined_sql)

    print(f"Az adatbázis sikeresen létrehozva: '{db_path}'")


def main():
    parser = argparse.ArgumentParser(
        description='Ipari Park adatok importálása .ods fájlból SQLite adatbázisba.'
    )
    parser.add_argument(
        '--input',
        required=True,
        help='Az importálandó Excel (.ods) fájl elérési útja.'
    )
    parser.add_argument(
        '--db',
        required=True,
        help='A cél SQLite adatbázis fájl elérési útja.'
    )
    args = parser.parse_args()

    excel_file = args.input
    db = args.db

    if not os.path.isfile(excel_file):
        print(f"Hiba: A megadott bemeneti fájl nem található: '{excel_file}'")
        return
    if not os.path.isfile(db):
        # Az adatbázis fájl nem létezik – megkérdezzük a felhasználót
        valasz = input(
            f"Az adatbázis fájl nem található: '{db}'.\n"
            "Szeretne új adatbázist létrehozni? (i/n): "
        )
        if valasz.strip().lower() == 'i':
            init_db(db)
        else:
            print("Adatbázis létrehozása megszakítva. Kilépés.")
            return

    all_data = read_excel(excel_file, sheets_to_read)
    if all_data is None:
        print("A beolvasás sikertelen, kilépés.")
        return
    else:
        print("Adat fájl sikeresen beolvasva.")

    validation_errors = data_validation(all_data)
    if validation_errors:
        print("Adatellenőrzési hibák:")
        for error in validation_errors:
            print(error)
        print(f"Összes hiba: {len(validation_errors)}")
        return
    else:
        print("Adatellenőrzés sikeres.")

    transform_write_to_db(db, all_data, column_mapping)
    print("Adatbetöltés kész.")

if __name__ == "__main__":
    main()
