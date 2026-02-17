CREATE TABLE "EU_tamogatas" (
	"park_ID"	INTEGER,
	"tamogatas_ev"	INTEGER
	"op"	TEXT,
	"tamogatas_tartalom"	TEXT,
	"intenzitas"	INTEGER,
	"EU_osszkoltseg"	INTEGER,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("park_ID","tamogatas_ev","tamogatas_tartalom", "datum"),
	FOREIGN KEY("park_ID") REFERENCES "alapadat"("park_ID")
);

CREATE TABLE "KF_kapcsolat" (
	"park_ID"	INTEGER NOT NULL,
	"kutatoint_ID"	INTEGER NOT NULL,
	"kezdet"	INTEGER NOT NULL,
	"tartalom"	TEXT NOT NULL,
	PRIMARY KEY("park_ID","kutatoint_ID"),
	FOREIGN KEY("kutatoint_ID") REFERENCES "kutatointezet"("kutatoint_ID"),
	FOREIGN KEY("park_ID") REFERENCES "alapadat"("park_ID")
);

CREATE TABLE "alapadat" (
	"park_ID"	INTEGER,
	"park_nev"	TEXT NOT NULL,
	"ip_cimszerzes"	INTEGER,
	"tp_cimszerzes"	INTEGER,
	"park_email"	TEXT,
	"park_telepules"	TEXT,
	"park_utca"	TEXT,
	"park_iranyitoszam"	INTEGER,
	"park_varmegye"	TEXT,
	"park_regio"	TEXT,
	"park_hrsz"	TEXT,
	"osszterulet"	NUMERIC,
	"hasznosithato_ter"	NUMERIC,
	"beepitett_ter"	NUMERIC,
	"hasznosithato_szabad_ter"	NUMERIC,
	"hasznosithato_szabad_arany"	NUMERIC,
	"parkolo"	NUMERIC,
	"zoldterulet"	NUMERIC,
	"berbeadott_ter_arany"	NUMERIC,
	"eladott_ter_arany"	NUMERIC,
	"sajat_szolg_arany"	NUMERIC,
	"kiszervezett_szolg_arany"	NUMERIC,
	"kamara"	INTEGER,
	"klaszter"	INTEGER,
	"oktatas_kozep"	INTEGER,
	"munkaugy"	INTEGER,
	"civil"	INTEGER,
	"ip"	INTEGER,
	"onkormanyzat"	INTEGER,
	"fejlesztesi_ugynokseg"	INTEGER,
	"export_ugynokseg"	INTEGER,
	"kulfoldi_ip"	INTEGER,
	"nemzetkozi_projekt"	INTEGER,
	"kf_tevekenyseg"	INTEGER,
	"uj_technologia"	INTEGER,
	"oktatas_felso"	INTEGER,
	"kutatointezet"	INTEGER,
	"park_honlap"	TEXT,
	"cimviselo_ID"	INTEGER,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("park_ID", "datum"),
	FOREIGN KEY("cimviselo_ID") REFERENCES "cimviselo"("cimviselo_ID")
);

CREATE TABLE "cimviselo" (
	"cimviselo_ID"	INTEGER NOT NULL,
	"cimviselo_nev"	TEXT NOT NULL,
	"cimviselo_foglalkoztatott"	INTEGER,
	"osszetetel_allam"	NUMERIC,
	"osszetetel_onkormanyzat"	NUMERIC,
	"osszetetel_belfoldi_magan"	NUMERIC,
	"osszetetel_kulfoldi"	NUMERIC,
	"cimviselo_cim"	TEXT,
	"osszetetel_egyeb"	NUMERIC,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("cimviselo_ID","datum")
);

CREATE TABLE "igenyelt_tamogatas" (
	"igenyles_ID"	INTEGER NOT NULL UNIQUE,
	"park_ID"	INTEGER NOT NULL,
	"ev"	INTEGER NOT NULL,
	"cel"	TEXT NOT NULL,
	"osszeg"	INTEGER NOT NULL,
	"forras"	TEXT NOT NULL,
	PRIMARY KEY("igenyles_ID"),
	FOREIGN KEY("park_ID") REFERENCES "alapadat"("park_ID")
);

CREATE TABLE "infra_fajta" (
	"infra_ID"	INTEGER NOT NULL,
	"infra_tipus"	TEXT NOT NULL,
	"infra_nev"	TEXT NOT NULL,
	PRIMARY KEY("infra_ID")
);

CREATE TABLE "infrastruktura" (
	"park_ID"	INTEGER,
	"infra_ID"	INTEGER,
	"kapacitas"	INTEGER,
	"ellatott_ter"	INTEGER,
	"allapot"	TEXT,
	"terv_fejlesztes_ev"	INTEGER,
	"terv_forras"	TEXT,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("park_ID","infra_ID", "datum"),
	FOREIGN KEY("infra_ID") REFERENCES "infra_fajta"("infra_ID"),
	FOREIGN KEY("park_ID") REFERENCES "alapadat"("park_ID")
);

CREATE TABLE "infrastrukturafejlesztes" (
	"park_ID"	INTEGER,
	"felhasznalas_ev"	INTEGER,
	"sajat_forras"	INTEGER,
	"allami_forras"	INTEGER,
	"onkormanyzati_forras"	INTEGER,
	"EU_forras"	INTEGER,
	"bankhitel"	INTEGER,
	"tagi_kolcson"	INTEGER,
	"tokeemeles"	INTEGER,
	"egyeb_forras"	INTEGER,
	"osszes_forras"	INTEGER,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("felhasznalas_ev","park_ID","datum"),
	FOREIGN KEY("park_ID") REFERENCES "alapadat"("park_ID")
);

CREATE TABLE "kutatointezet" (
	"kutatoint_ID"	INTEGER NOT NULL UNIQUE,
	"nev"	TEXT NOT NULL,
	PRIMARY KEY("kutatoint_ID" AUTOINCREMENT)
);

CREATE TABLE "management" (
	"management_nev"	TEXT NOT NULL,
	"management_beosztas"	TEXT,
	"management_tel"	NUMERIC,
	"management_email"	TEXT,
	"jognyilatkozat"	INTEGER,
	"operativ"	INTEGER,
	"park_ID"	INTEGER,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("park_ID","datum", "management_email"),
	FOREIGN KEY("park_ID") REFERENCES "alapadat"("park_ID")
);

CREATE TABLE "szolg_fajta" (
	"szolg_ID"	INTEGER,
	"szolg_tipus"	TEXT,
	"szolg_nev"	TEXT,
	PRIMARY KEY("szolg_ID")
);

CREATE TABLE "szolgaltatas" (
	"park_ID"	INTEGER,
	"szolg_ID"	INTEGER,
	"szolg_tartalom"	TEXT,
	"szolgaltato_fajta"	TEXT,
	"szolgaltato_nev"	TEXT,
	"szolg_kezdet"	INTEGER,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("park_ID","szolg_ID", "datum"),
	FOREIGN KEY("park_ID") REFERENCES "alapadat"("park_ID"),
	FOREIGN KEY("szolg_ID") REFERENCES "szolg_fajta"("szolg_ID")
);

CREATE TABLE "vallalkozasok" (
	"park_ID"	INTEGER,
	"vallalkozasok_ev"	INTEGER,
	"vallalkozasok_terulet"	INTEGER,
	"vallalkozasok_szama"	INTEGER,
	"vallalkozasok_foglalkoztatott"	NUMERIC,
	"beruhazasi_ertek"	NUMERIC,
	"arbevetel"	NUMERIC,
	"exportarany"	NUMERIC,
	"kkv_szam"	NUMERIC,
	"nagyvall_szam"	NUMERIC,
	"egyeb_vall_szam"	NUMERIC,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("park_ID","vallalkozasok_ev","datum"),
	FOREIGN KEY("park_ID") REFERENCES "alapadat"("park_ID")
);

CREATE TABLE "helyrajzi_szam" (
	"park_ID"	INTEGER,
	"park_hrsz"	TEXT,
	"datum"	TEXT DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("park_ID","park_hrsz","datum"),
	FOREIGN KEY("park_ID") REFERENCES "alapadat"("park_ID")
);
