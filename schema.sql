CREATE TABLE "EU_tamogatas" (
	"park_ID"	INTEGER,
	"tamogatas_ev"	INTEGER,
	"op"	TEXT,
	"tamogatas_tartalom"	TEXT,
	"intenzitas"	REAL,
	"EU_osszkoltseg"	REAL,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("park_ID","tamogatas_ev","tamogatas_tartalom","datum"),
	FOREIGN KEY("park_ID") REFERENCES "park_azonosito"("park_ID")
);
CREATE TABLE "alapadat" (
	"park_ID"	INTEGER,
	"ip_cimszerzes"	INTEGER,
	"tp_cimszerzes"	INTEGER,
	"park_email"	TEXT,
	"park_telepules"	TEXT,
	"park_utca"	TEXT,
	"park_iranyitoszam"	TEXT,
	"park_varmegye"	TEXT,
	"park_regio"	TEXT,
	"sajat_szolg_arany"	REAL,
	"kiszervezett_szolg_arany"	REAL,
	"park_honlap"	TEXT,
	"cimviselo_ID"	INTEGER,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	"alapadat_ev"	INTEGER,
	PRIMARY KEY("park_ID","datum"),
	FOREIGN KEY("cimviselo_ID") REFERENCES "cimviselo_azonosito"("cimviselo_ID"),
	FOREIGN KEY("park_ID") REFERENCES "park_azonosito"("park_ID")
);
CREATE TABLE "cimviselo" (
	"cimviselo_ID"	INTEGER,
	"cimviselo_foglalkoztatott"	INTEGER,
	"osszetetel_allam"	REAL,
	"osszetetel_onkormanyzat"	REAL,
	"osszetetel_belfoldi_magan"	REAL,
	"osszetetel_kulfoldi"	REAL,
	"cimviselo_cim"	TEXT,
	"osszetetel_egyeb"	REAL,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("cimviselo_ID","datum"),
	FOREIGN KEY("cimviselo_ID") REFERENCES "cimviselo_azonosito"("cimviselo_ID")
);
CREATE TABLE "helyrajzi_szam" (
	"park_ID"	INTEGER,
	"park_hrsz"	TEXT,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("park_ID","park_hrsz","datum"),
	FOREIGN KEY("park_ID") REFERENCES "park_azonosito"("park_ID")
);
CREATE TABLE "infra_fajta" (
	"infra_ID"	INTEGER NOT NULL,
	"infra_tipus"	TEXT NOT NULL,
	"infra_nev"	TEXT NOT NULL,
	PRIMARY KEY("infra_ID"),
	UNIQUE("infra_tipus", "infra_nev")
);
CREATE TABLE "infrastruktura" (
	"park_ID"	INTEGER,
	"infra_ID"	INTEGER,
	"kapacitas"	REAL,
	"ellatott_ter"	REAL,
	"allapot"	TEXT,
	"terv_fejlesztes_ev"	INTEGER,
	"terv_forras"	TEXT,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("park_ID","infra_ID","datum"),
	FOREIGN KEY("infra_ID") REFERENCES "infra_fajta"("infra_ID"),
	FOREIGN KEY("park_ID") REFERENCES "park_azonosito"("park_ID")
);
CREATE TABLE "infrastrukturafejlesztes" (
	"park_ID"	INTEGER,
	"felhasznalas_ev"	INTEGER,
	"sajat_forras"	REAL,
	"allami_forras"	REAL,
	"onkormanyzati_forras"	REAL,
	"EU_forras"	REAL,
	"bankhitel"	REAL,
	"tagi_kolcson"	REAL,
	"tokeemeles"	REAL,
	"egyeb_forras"	REAL,
	"osszes_forras"	REAL,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("felhasznalas_ev","park_ID","datum"),
	FOREIGN KEY("park_ID") REFERENCES "park_azonosito"("park_ID")
);
CREATE TABLE "kapcsolatok" (
	"park_ID"	INTEGER NOT NULL,
	"kamara"	TEXT,
	"oktatas_kozep"	TEXT,
	"munkaugy"	TEXT,
	"civil"	TEXT,
	"ip"	TEXT,
	"onkormanyzat"	TEXT,
	"fejlesztesi_ugynokseg"	TEXT,
	"export_ugynokseg"	TEXT,
	"kulfoldi_ip"	TEXT,
	"nemzetkozi_projekt"	TEXT,
	"kf_tevekenyseg"	TEXT,
	"uj_technologia"	TEXT,
	"oktatas_felso"	TEXT,
	"kutatointezet"	TEXT,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	"kapcsolatok_ev"	INTEGER,
	PRIMARY KEY("park_ID","datum"),
	FOREIGN KEY("park_ID") REFERENCES "park_azonosito"("park_ID")
);
CREATE TABLE "management" (
	"management_nev"	TEXT NOT NULL,
	"management_beosztas"	TEXT,
	"management_tel"	TEXT,
	"management_email"	TEXT,
	"jognyilatkozat"	INTEGER,
	"operativ"	INTEGER,
	"park_ID"	INTEGER,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	"management_kezdet"	TEXT,
	"management_vege"	TEXT,
	PRIMARY KEY("park_ID","datum","management_email"),
	FOREIGN KEY("park_ID") REFERENCES "park_azonosito"("park_ID")
);
CREATE TABLE "park_azonosito" (
	"park_ID"	INTEGER,
	"park_nev"	TEXT,
	"aktiv"	INTEGER DEFAULT 1,
	PRIMARY KEY("park_ID" AUTOINCREMENT)
);
CREATE TABLE "szolg_fajta" (
	"szolg_ID"	INTEGER,
	"szolg_tipus"	TEXT,
	"szolg_nev"	TEXT,
	PRIMARY KEY("szolg_ID"),
	UNIQUE("szolg_tipus", "szolg_nev")
);
CREATE TABLE "szolgaltatas" (
	"park_ID"	INTEGER,
	"szolg_ID"	INTEGER,
	"szolg_tartalom"	TEXT,
	"szolgaltato_fajta"	TEXT,
	"szolgaltato_nev"	TEXT,
	"szolg_kezdet"	INTEGER,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("park_ID","szolg_ID","datum"),
	FOREIGN KEY("park_ID") REFERENCES "park_azonosito"("park_ID"),
	FOREIGN KEY("szolg_ID") REFERENCES "szolg_fajta"("szolg_ID")
);
CREATE TABLE "terulet" (
	"park_ID"	INTEGER NOT NULL,
	"osszterulet"	REAL,
	"hasznosithato_ter"	REAL,
	"beepitett_ter"	REAL,
	"hasznosithato_szabad_ter"	REAL,
	"hasznosithato_szabad_arany"	REAL,
	"parkolo"	REAL,
	"zoldterulet"	REAL,
	"berbeadott_ter_arany"	REAL,
	"eladott_ter_arany"	REAL,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	"terulet_ev"	INTEGER,
	PRIMARY KEY("datum","park_ID"),
	FOREIGN KEY("park_ID") REFERENCES "park_azonosito"("park_ID")
);
CREATE TABLE "vallalkozasok" (
	"park_ID"	INTEGER,
	"vallalkozasok_ev"	INTEGER,
	"vallalkozasok_terulet"	REAL,
	"vallalkozasok_szama"	INTEGER,
	"vallalkozasok_foglalkoztatott"	INTEGER,
	"beruhazasi_ertek"	REAL,
	"arbevetel"	REAL,
	"exportarany"	REAL,
	"kkv_szam"	INTEGER,
	"nagyvall_szam"	INTEGER,
	"egyeb_vall_szam"	INTEGER,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("park_ID","vallalkozasok_ev","datum"),
	FOREIGN KEY("park_ID") REFERENCES "park_azonosito"("park_ID")
);
CREATE TABLE "cimviselo_azonosito" (
	"cimviselo_ID"	INTEGER,
	"cimviselo_nev"	TEXT,
	PRIMARY KEY("cimviselo_ID" AUTOINCREMENT)
);
