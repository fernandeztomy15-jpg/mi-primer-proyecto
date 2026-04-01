"""
fetch_data.py
Descarga indicadores económicos de EE.UU. (FRED), Brasil (BCB),
Argentina (INDEC API + dolarapi.com), Chile, Colombia y México (FRED).
Guarda en archivos CSV/JSON en la carpeta data/
"""

import os
import json
import requests
import pandas as pd
from datetime import datetime, date

# ─── Configuración general ────────────────────────────────────────────────────
START_DATE    = "2020-01-01"
END_DATE      = datetime.today().strftime("%Y-%m-%d")
DATA_DIR      = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# ─── FRED (EE.UU.) ────────────────────────────────────────────────────────────
FRED_API_KEY  = os.environ.get("FRED_API_KEY", "0fd385a768cca11d2a1ff2a1e0765443")
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# ─── INEGI + Banxico (México) ─────────────────────────────────────────────────
INEGI_API_KEY   = os.environ.get("INEGI_API_KEY", "")
BANXICO_API_KEY = os.environ.get("BANXICO_API_KEY", "")

US_INDICATORS = {
    "CPILFESL":        {"name": "Core CPI",                   "mom": True,  "yoy": True},
    "UMCSENT":         {"name": "Consumer Sentiment (UMich)", "mom": False, "yoy": False},
    "RSXFS":           {"name": "Retail Sales",               "mom": True,  "yoy": True},
    "PPIACO":          {"name": "Producer Price Index",       "mom": True,  "yoy": True},
    "ICSA":            {"name": "Initial Jobless Claims",     "mom": False, "yoy": False},
    "A191RL1Q225SBEA": {"name": "GDP Growth QoQ",            "mom": False, "yoy": False},
    "INDPRO":          {"name": "Industrial Production",      "mom": True,  "yoy": True},
    "FEDFUNDS":        {"name": "Federal Funds Rate",         "mom": False, "yoy": False},
    "PCEPILFE":        {"name": "Core PCE Price Index",       "mom": True,  "yoy": True},
    "JTSJOL":          {"name": "JOLTs Job Openings",         "mom": False, "yoy": False},
    "UNRATE":          {"name": "Unemployment Rate",          "mom": False, "yoy": False},
}

# ─── BCB (Brasil) ─────────────────────────────────────────────────────────────
BCB_BASE_URL  = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"

# ─── LATAM — FRED (Chile, Colombia) ──────────────────────────────────────────
LATAM_COUNTRIES = [
    {
        "code": "cl", "name": "Chile", "flag": "🇨🇱",
        "cpi_yoy": "CPALTT01CLM659N",          # CPI YoY % (OECD 659N = same period prev. year)
        "cpi_mom": "CPALTT01CLM657N",          # CPI MoM % (OECD 657N = previous period)
        "rate":    "IRSTCI01CLM156N",           # Overnight interbank (proxy política BCCh)
        "unrate":  "LRHUTTTTCLM156S",           # Desempleo armonizado OECD
        "activity": {"id": "NAEXKP01CLQ657S", "name": "PIB QoQ% (OECD)", "mom": False, "yoy": False},
        "fx_fred": "CCUSMA02CLM618N",           # CLP/USD monthly (OECD) — DEXCHUS era CNY/USD
    },
    {
        "code": "co", "name": "Colombia", "flag": "🇨🇴",
        "cpi_yoy": None,                        # No disponible en FRED → hardcodeado vía CO_HARDCODED
        "cpi_mom": None,
        "rate":    "COLIR3TIB01STM",            # 3M interbank rate Colombia
        "unrate":  "LRUN64TTCOM156S",           # Desempleo armonizado OECD Colombia (FRED)
        "activity": None,
        "fx_fred": "DEXCOUS",                   # COP/USD daily (FRED) → resample mensual
    },
]

# ─── Colombia (hardcoded — fallback cuando FRED no tiene la serie) ────────────
# Investigación de fuentes (2026-03):
#   CPI: BIS API (WS_LONG_CPI) — migrado. Hardcoded como fallback.
#   Unrate: LRUN64TTCOM156S en FRED (OECD armonizado); hardcoded DANE como fallback.
#   COP/USD: DEXCOUS en FRED (daily → mensual); hardcoded BanRep TRM como fallback.
#   Tasa BanRep: disponible en FRED (COLIR3TIB01STM) → ya se usa, auto-actualiza.
#   Nota CL: FRED/OECD (CPALTT01, IRSTCI01, LRHUTTTT, NAEXKP01, CCUSMA02CLM618N) cubren
#            todo Chile sin necesidad de migrar a BCCh API.
CO_HARDCODED = {
    "co_cpi": {
        "name": "CPI YoY/MoM — DANE Colombia",
        "data_yoy": [
            # YoY % (value), MoM % (mom_pct) — fuente DANE
            ("2020-01-01", 3.62, 0.54), ("2020-02-01", 3.72, 0.71), ("2020-03-01", 3.72, 0.57),
            ("2020-04-01", 3.46, 0.12), ("2020-05-01", 2.85, 0.18), ("2020-06-01", 2.19, 0.29),
            ("2020-07-01", 1.97, -0.07), ("2020-08-01", 1.90, 0.07), ("2020-09-01", 1.97, 0.44),
            ("2020-10-01", 1.75, 0.68), ("2020-11-01", 1.49, 0.16), ("2020-12-01", 1.61, 0.33),
            ("2021-01-01", 1.60, 0.64), ("2021-02-01", 1.56, 0.65), ("2021-03-01", 1.51, 0.78),
            ("2021-04-01", 1.95, 0.41), ("2021-05-01", 3.30, 0.31), ("2021-06-01", 3.63, 0.33),
            ("2021-07-01", 3.97, 0.54), ("2021-08-01", 4.44, 0.63), ("2021-09-01", 4.51, 0.55),
            ("2021-10-01", 4.58, 0.69), ("2021-11-01", 5.26, 0.29), ("2021-12-01", 5.62, 0.73),
            ("2022-01-01", 6.94, 1.67), ("2022-02-01", 8.01, 1.63), ("2022-03-01", 8.53, 1.00),
            ("2022-04-01", 9.23, 0.95), ("2022-05-01", 9.07, 0.88), ("2022-06-01", 9.67, 0.93),
            ("2022-07-01", 10.21, 0.97), ("2022-08-01", 10.84, 1.02), ("2022-09-01", 11.44, 0.63),
            ("2022-10-01", 12.22, 0.54), ("2022-11-01", 12.53, 0.76), ("2022-12-01", 13.12, 0.42),
            ("2023-01-01", 13.25, 1.78), ("2023-02-01", 13.28, 1.66), ("2023-03-01", 12.73, 1.12),
            ("2023-04-01", 12.82, 0.64), ("2023-05-01", 12.36, 0.43), ("2023-06-01", 12.13, 0.31),
            ("2023-07-01", 11.78, 0.28), ("2023-08-01", 11.43, 0.24), ("2023-09-01", 10.99, 0.16),
            ("2023-10-01", 10.48, 0.23), ("2023-11-01", 10.15, 0.11), ("2023-12-01", 9.28, 0.33),
            ("2024-01-01", 8.35, 0.82), ("2024-02-01", 7.74, 0.63), ("2024-03-01", 7.36, 0.54),
            ("2024-04-01", 7.16, 0.68), ("2024-05-01", 7.18, 0.44), ("2024-06-01", 6.86, 0.27),
            ("2024-07-01", 6.86, 0.27), ("2024-08-01", 6.11, 0.21), ("2024-09-01", 5.81, 0.25),
            ("2024-10-01", 5.41, 0.23), ("2024-11-01", 5.20, 0.18), ("2024-12-01", 5.20, 0.15),
            ("2025-01-01", 5.22, 0.64), ("2025-02-01", 5.08, 0.62),
            ("2025-03-01", 5.09, 0.52), ("2025-04-01", 5.16, 0.66),
            ("2025-05-01", 5.05, 0.32), ("2025-06-01", 4.82, 0.10),
            ("2025-07-01", 4.90, 0.28), ("2025-08-01", 5.10, 0.19),
            ("2025-09-01", 5.18, 0.32), ("2025-10-01", 5.51, 0.18),
            ("2025-11-01", 5.30, 0.07), ("2025-12-01", 5.10, 0.27),
            ("2026-01-01", 5.35, 1.18), ("2026-02-01", 5.29, 1.08),
        ],
    },
    "co_unrate": {
        "name": "Desempleo — DANE Colombia (%)",
        "data": [
            # Dato mensual GEIH (DANE)
            ("2020-01-01", 12.2), ("2020-02-01", 12.2), ("2020-03-01", 12.6),
            ("2020-04-01", 19.8), ("2020-05-01", 21.4), ("2020-06-01", 19.8),
            ("2020-07-01", 20.2), ("2020-08-01", 16.8), ("2020-09-01", 15.8),
            ("2020-10-01", 15.1), ("2020-11-01", 14.4), ("2020-12-01", 14.2),
            ("2021-01-01", 17.3), ("2021-02-01", 15.9), ("2021-03-01", 14.2),
            ("2021-04-01", 15.0), ("2021-05-01", 15.6), ("2021-06-01", 14.4),
            ("2021-07-01", 14.3), ("2021-08-01", 12.3), ("2021-09-01", 11.6),
            ("2021-10-01", 11.3), ("2021-11-01", 10.8), ("2021-12-01", 11.1),
            ("2022-01-01", 14.6), ("2022-02-01", 13.1), ("2022-03-01", 11.4),
            ("2022-04-01", 11.0), ("2022-05-01", 10.1), ("2022-06-01", 10.6),
            ("2022-07-01", 10.7), ("2022-08-01", 10.4), ("2022-09-01", 10.7),
            ("2022-10-01", 10.3), ("2022-11-01",  9.8), ("2022-12-01",  9.7),
            ("2023-01-01", 13.1), ("2023-02-01", 12.0), ("2023-03-01", 10.9),
            ("2023-04-01", 10.8), ("2023-05-01", 10.5), ("2023-06-01", 10.4),
            ("2023-07-01", 11.0), ("2023-08-01", 10.2), ("2023-09-01", 10.2),
            ("2023-10-01", 10.1), ("2023-11-01",  9.3), ("2023-12-01",  9.3),
            ("2024-01-01", 12.2), ("2024-02-01", 11.1), ("2024-03-01", 10.1),
            ("2024-04-01",  9.9), ("2024-05-01",  9.2), ("2024-06-01",  9.3),
            ("2024-07-01",  9.8), ("2024-08-01",  9.0), ("2024-09-01",  9.5),
            ("2024-10-01",  9.5), ("2024-11-01",  8.9), ("2024-12-01",  9.1),
            ("2025-01-01", 11.0), ("2025-02-01", 10.4),
            ("2025-03-01",  9.6), ("2025-04-01",  8.8),
            ("2025-05-01",  9.0), ("2025-06-01",  8.6),
            ("2025-07-01",  8.8), ("2025-08-01",  8.6),
            ("2025-09-01",  8.2), ("2025-10-01",  8.2),
            ("2025-11-01",  7.0), ("2025-12-01",  8.0),
            ("2026-01-01", 10.9), ("2026-02-01",  9.2),
        ],
    },
}

# ─── Colombia — COP/USD hardcodeado (fallback si DEXCOUS falla en FRED) ──────
CO_USD_HARDCODED = [
    ("2020-01-01", 3288), ("2020-02-01", 3377), ("2020-03-01", 3817),
    ("2020-04-01", 3900), ("2020-05-01", 3700), ("2020-06-01", 3720),
    ("2020-07-01", 3790), ("2020-08-01", 3810), ("2020-09-01", 3820),
    ("2020-10-01", 3855), ("2020-11-01", 3700), ("2020-12-01", 3432),
    ("2021-01-01", 3554), ("2021-02-01", 3610), ("2021-03-01", 3680),
    ("2021-04-01", 3810), ("2021-05-01", 3800), ("2021-06-01", 3740),
    ("2021-07-01", 3860), ("2021-08-01", 3965), ("2021-09-01", 3808),
    ("2021-10-01", 3850), ("2021-11-01", 3955), ("2021-12-01", 3981),
    ("2022-01-01", 3990), ("2022-02-01", 3930), ("2022-03-01", 3817),
    ("2022-04-01", 3770), ("2022-05-01", 3900), ("2022-06-01", 4170),
    ("2022-07-01", 4370), ("2022-08-01", 4450), ("2022-09-01", 4630),
    ("2022-10-01", 4970), ("2022-11-01", 4820), ("2022-12-01", 4810),
    ("2023-01-01", 4620), ("2023-02-01", 4720), ("2023-03-01", 4640),
    ("2023-04-01", 4600), ("2023-05-01", 4530), ("2023-06-01", 4360),
    ("2023-07-01", 4120), ("2023-08-01", 4108), ("2023-09-01", 4310),
    ("2023-10-01", 4280), ("2023-11-01", 4010), ("2023-12-01", 3960),
    ("2024-01-01", 3920), ("2024-02-01", 3930), ("2024-03-01", 3940),
    ("2024-04-01", 3890), ("2024-05-01", 3900), ("2024-06-01", 4120),
    ("2024-07-01", 4200), ("2024-08-01", 4190), ("2024-09-01", 4165),
    ("2024-10-01", 4330), ("2024-11-01", 4410), ("2024-12-01", 4380),
    ("2025-01-01", 4395), ("2025-02-01", 4358),
    ("2025-03-01", 4147), ("2025-04-01", 4298),
    ("2025-05-01", 4272), ("2025-06-01", 4167),
    ("2025-07-01", 4078), ("2025-08-01", 4117),
    ("2025-09-01", 4174), ("2025-10-01", 4119),
    ("2025-11-01", 3947), ("2025-12-01", 3769),
    ("2026-01-01", 3704), ("2026-02-01", 3667),
]

# ─── Argentina (hardcoded fallback — INDEC/BCRA) ──────────────────────────────
AR_HARDCODED = {
    "ar_cpi_mom_fallback": {
        "name": "CPI Mensual — INDEC (%) [fallback]",
        "data": [
            ("2020-01-01", 2.3), ("2020-02-01", 2.0), ("2020-03-01", 3.3),
            ("2020-04-01", 1.5), ("2020-05-01", 1.5), ("2020-06-01", 2.2),
            ("2020-07-01", 1.9), ("2020-08-01", 2.7), ("2020-09-01", 2.8),
            ("2020-10-01", 3.8), ("2020-11-01", 3.2), ("2020-12-01", 4.0),
            ("2021-01-01", 4.0), ("2021-02-01", 3.6), ("2021-03-01", 4.8),
            ("2021-04-01", 4.1), ("2021-05-01", 3.3), ("2021-06-01", 3.2),
            ("2021-07-01", 3.0), ("2021-08-01", 2.5), ("2021-09-01", 3.5),
            ("2021-10-01", 3.5), ("2021-11-01", 2.5), ("2021-12-01", 3.8),
            ("2022-01-01", 3.9), ("2022-02-01", 4.7), ("2022-03-01", 6.7),
            ("2022-04-01", 6.0), ("2022-05-01", 5.1), ("2022-06-01", 5.3),
            ("2022-07-01", 7.4), ("2022-08-01", 7.0), ("2022-09-01", 6.2),
            ("2022-10-01", 6.3), ("2022-11-01", 4.9), ("2022-12-01", 5.1),
            ("2023-01-01", 6.0), ("2023-02-01", 6.6), ("2023-03-01", 7.7),
            ("2023-04-01", 8.4), ("2023-05-01", 7.8), ("2023-06-01", 6.0),
            ("2023-07-01", 6.3), ("2023-08-01", 12.4), ("2023-09-01", 12.7),
            ("2023-10-01", 8.3), ("2023-11-01", 12.8), ("2023-12-01", 25.5),
            ("2024-01-01", 20.6), ("2024-02-01", 13.2), ("2024-03-01", 11.0),
            ("2024-04-01", 8.8), ("2024-05-01", 4.2), ("2024-06-01", 4.6),
            ("2024-07-01", 4.0), ("2024-08-01", 4.2), ("2024-09-01", 3.5),
            ("2024-10-01", 2.4), ("2024-11-01", 2.4), ("2024-12-01", 2.7),
            ("2025-01-01", 2.3), ("2025-02-01", 2.4),
            ("2025-03-01", 3.7), ("2025-04-01", 2.8),
            ("2025-05-01", 1.5), ("2025-06-01", 1.6),
            ("2025-07-01", 1.9), ("2025-08-01", 1.9),
            ("2025-09-01", 2.1), ("2025-10-01", 2.3),
            ("2025-11-01", 2.5), ("2025-12-01", 2.8),
            ("2026-01-01", 2.9), ("2026-02-01", 2.9),
        ],
    },
    "ar_rate": {
        "name": "Tasa BCRA — Política Monetaria (% anual)",
        "data": [
            ("2020-01-01", 52.0), ("2020-02-01", 50.0), ("2020-03-01", 44.0),
            ("2020-04-01", 38.0), ("2020-05-01", 38.0), ("2020-06-01", 38.0),
            ("2020-07-01", 38.0), ("2020-08-01", 38.0), ("2020-09-01", 38.0),
            ("2020-10-01", 38.0), ("2020-11-01", 38.0), ("2020-12-01", 38.0),
            ("2021-01-01", 38.0), ("2021-02-01", 38.0), ("2021-03-01", 38.0),
            ("2021-04-01", 38.0), ("2021-05-01", 38.0), ("2021-06-01", 38.0),
            ("2021-07-01", 38.0), ("2021-08-01", 38.0), ("2021-09-01", 38.0),
            ("2021-10-01", 38.0), ("2021-11-01", 38.0), ("2021-12-01", 40.0),
            ("2022-01-01", 40.0), ("2022-02-01", 40.0), ("2022-03-01", 42.5),
            ("2022-04-01", 47.0), ("2022-05-01", 49.0), ("2022-06-01", 52.0),
            ("2022-07-01", 60.0), ("2022-08-01", 69.5), ("2022-09-01", 75.0),
            ("2022-10-01", 75.0), ("2022-11-01", 75.0), ("2022-12-01", 75.0),
            ("2023-01-01", 75.0), ("2023-02-01", 75.0), ("2023-03-01", 78.0),
            ("2023-04-01", 81.0), ("2023-05-01", 97.0), ("2023-06-01", 97.0),
            ("2023-07-01", 97.0), ("2023-08-01", 118.0), ("2023-09-01", 118.0),
            ("2023-10-01", 133.0), ("2023-11-01", 133.0), ("2023-12-01", 100.0),
            ("2024-01-01", 97.0), ("2024-02-01", 60.0), ("2024-03-01", 60.0),
            ("2024-04-01", 60.0), ("2024-05-01", 40.0), ("2024-06-01", 40.0),
            ("2024-07-01", 40.0), ("2024-08-01", 40.0), ("2024-09-01", 35.0),
            ("2024-10-01", 35.0), ("2024-11-01", 35.0), ("2024-12-01", 32.0),
            ("2025-01-01", 29.0), ("2025-02-01", 29.0),
            ("2025-03-01", 29.0), ("2025-04-01", 29.0),
            ("2025-05-01", 29.0), ("2025-06-01", 29.0),
            ("2025-07-01", 29.0), ("2025-08-01", 29.0),
            ("2025-09-01", 29.0), ("2025-10-01", 29.0),
            ("2025-11-01", 29.0), ("2025-12-01", 29.0),
            ("2026-01-01", 29.0), ("2026-02-01", 29.0),
        ],
    },
    "ar_unrate": {
        "name": "Desocupación INDEC-EPH (%)",
        "data": [
            # Dato trimestral (EPH) — se repite el mismo valor dentro de cada trimestre
            ("2020-01-01", 10.4), ("2020-02-01", 10.4), ("2020-03-01", 10.4),
            ("2020-04-01", 13.1), ("2020-05-01", 13.1), ("2020-06-01", 13.1),
            ("2020-07-01", 11.7), ("2020-08-01", 11.7), ("2020-09-01", 11.7),
            ("2020-10-01", 11.0), ("2020-11-01", 11.0), ("2020-12-01", 11.0),
            ("2021-01-01", 10.2), ("2021-02-01", 10.2), ("2021-03-01", 10.2),
            ("2021-04-01",  9.6), ("2021-05-01",  9.6), ("2021-06-01",  9.6),
            ("2021-07-01",  8.2), ("2021-08-01",  8.2), ("2021-09-01",  8.2),
            ("2021-10-01",  7.0), ("2021-11-01",  7.0), ("2021-12-01",  7.0),
            ("2022-01-01",  7.0), ("2022-02-01",  7.0), ("2022-03-01",  7.0),
            ("2022-04-01",  7.0), ("2022-05-01",  7.0), ("2022-06-01",  7.0),
            ("2022-07-01",  6.3), ("2022-08-01",  6.3), ("2022-09-01",  6.3),
            ("2022-10-01",  6.3), ("2022-11-01",  6.3), ("2022-12-01",  6.3),
            ("2023-01-01",  6.9), ("2023-02-01",  6.9), ("2023-03-01",  6.9),
            ("2023-04-01",  6.2), ("2023-05-01",  6.2), ("2023-06-01",  6.2),
            ("2023-07-01",  5.7), ("2023-08-01",  5.7), ("2023-09-01",  5.7),
            ("2023-10-01",  5.7), ("2023-11-01",  5.7), ("2023-12-01",  5.7),
            ("2024-01-01",  7.7), ("2024-02-01",  7.7), ("2024-03-01",  7.7),
            ("2024-04-01",  7.6), ("2024-05-01",  7.6), ("2024-06-01",  7.6),
            ("2024-07-01",  6.9), ("2024-08-01",  6.9), ("2024-09-01",  6.9),
            ("2024-10-01",  6.4), ("2024-11-01",  6.4), ("2024-12-01",  6.4),
            ("2025-01-01",  7.9), ("2025-02-01",  7.9), ("2025-03-01",  7.9),
            ("2025-04-01",  7.6), ("2025-05-01",  7.6), ("2025-06-01",  7.6),
            ("2025-07-01",  6.6), ("2025-08-01",  6.6), ("2025-09-01",  6.6),
            ("2025-10-01",  7.5), ("2025-11-01",  7.5), ("2025-12-01",  7.5),
        ],
    },
    "ar_emae": {
        "name": "EMAE — Actividad Económica INDEC (2004=100)",
        "data": [
            ("2020-01-01", 142.6), ("2020-02-01", 143.5), ("2020-03-01", 133.2),
            ("2020-04-01", 105.4), ("2020-05-01", 120.5), ("2020-06-01", 126.9),
            ("2020-07-01", 133.8), ("2020-08-01", 139.5), ("2020-09-01", 143.7),
            ("2020-10-01", 146.0), ("2020-11-01", 145.2), ("2020-12-01", 144.3),
            ("2021-01-01", 140.2), ("2021-02-01", 142.1), ("2021-03-01", 147.9),
            ("2021-04-01", 146.7), ("2021-05-01", 150.1), ("2021-06-01", 153.2),
            ("2021-07-01", 155.4), ("2021-08-01", 157.8), ("2021-09-01", 161.0),
            ("2021-10-01", 162.4), ("2021-11-01", 163.9), ("2021-12-01", 161.8),
            ("2022-01-01", 157.6), ("2022-02-01", 161.4), ("2022-03-01", 165.9),
            ("2022-04-01", 165.2), ("2022-05-01", 166.8), ("2022-06-01", 165.4),
            ("2022-07-01", 166.1), ("2022-08-01", 168.4), ("2022-09-01", 168.0),
            ("2022-10-01", 168.9), ("2022-11-01", 169.5), ("2022-12-01", 167.2),
            ("2023-01-01", 162.8), ("2023-02-01", 165.3), ("2023-03-01", 167.1),
            ("2023-04-01", 165.4), ("2023-05-01", 166.7), ("2023-06-01", 164.2),
            ("2023-07-01", 166.3), ("2023-08-01", 163.5), ("2023-09-01", 163.9),
            ("2023-10-01", 162.1), ("2023-11-01", 158.3), ("2023-12-01", 153.9),
            ("2024-01-01", 143.2), ("2024-02-01", 141.5), ("2024-03-01", 143.8),
            ("2024-04-01", 144.1), ("2024-05-01", 145.7), ("2024-06-01", 147.2),
            ("2024-07-01", 148.9), ("2024-08-01", 149.4), ("2024-09-01", 150.1),
            ("2024-10-01", 152.3), ("2024-11-01", 153.5), ("2024-12-01", 152.8),
        ],
    },
}

# serie: número de série BCB
# raw_rate: True si el valor YA es una tasa/variación (no se calcula pct_change)
BR_INDICATORS = {
    "br_ipca":      {"serie": 433,   "name": "IPCA Monthly Change (%)",         "mom": False, "yoy": False, "raw_rate": True},
    "br_icc":       {"serie": 4393,  "name": "Consumer Confidence (ICC)",       "mom": False, "yoy": False, "raw_rate": False},
    "br_retail":    {"serie": 1455,  "name": "Retail Sales",                    "mom": True,  "yoy": True,  "raw_rate": False},
    "br_ipp":       {"serie": 11426, "name": "Producer Price Index (IPP)",      "mom": True,  "yoy": True,  "raw_rate": False},
    "br_gdp":       {"serie": 22109, "name": "GDP Real Index (QoQ via MoM)",    "mom": True,  "yoy": True,  "raw_rate": False},
    "br_indpro":    {"serie": 21859, "name": "Industrial Production",           "mom": True,  "yoy": True,  "raw_rate": False},
    "br_selic":     {"serie": 4189,  "name": "Selic Rate (%)",                  "mom": False, "yoy": False, "raw_rate": True},
    "br_ipca_core": {"serie": 11427, "name": "Core IPCA Monthly Change (%)",   "mom": False, "yoy": False, "raw_rate": True},
    "br_caged":     {"serie": 28763, "name": "Formal Employment Stock (CAGED)", "mom": True,  "yoy": True,  "raw_rate": False},
    "br_unrate":    {"serie": 24369, "name": "Unemployment Rate (PNAD)",        "mom": False, "yoy": False, "raw_rate": True},
    "br_usdbrl":    {"serie": 10813, "name": "BRL/USD Exchange Rate",          "mom": False, "yoy": False, "raw_rate": False},
}


# ─── Helpers comunes ──────────────────────────────────────────────────────────

def add_changes(df: pd.DataFrame, mom: bool, yoy: bool) -> pd.DataFrame:
    """Agrega columnas MoM y/o YoY al DataFrame."""
    if mom:
        df["mom_pct"] = df["value"].pct_change(periods=1) * 100
    if yoy:
        df["yoy_pct"] = df["value"].pct_change(periods=12) * 100
    return df


def make_summary_row(series_id, name, df, source=None):
    """Construye la fila de resumen a partir del último registro del DataFrame."""
    last_row  = df.iloc[-1]
    last_date = last_row["date"].strftime("%Y-%m-%d")
    last_val  = round(last_row["value"], 4)
    mom_val   = round(last_row["mom_pct"], 4) if "mom_pct" in df.columns else None
    yoy_val   = round(last_row["yoy_pct"], 4) if "yoy_pct" in df.columns else None
    row = {
        "series_id":   series_id,
        "name":        name,
        "last_date":   last_date,
        "last_value":  last_val,
        "mom_pct":     mom_val,
        "yoy_pct":     yoy_val,
        "rows":        len(df),
    }
    if source is not None:
        row["source"] = source
    return row


def empty_summary_row(series_id, name, source=None):
    row = {"series_id": series_id, "name": name,
           "last_date": None, "last_value": None,
           "mom_pct": None, "yoy_pct": None, "rows": 0}
    if source is not None:
        row["source"] = source
    return row


# ─── FRED ─────────────────────────────────────────────────────────────────────

def fetch_fred(series_id: str) -> pd.DataFrame:
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": START_DATE,
        "observation_end": END_DATE,
    }
    r = requests.get(FRED_BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    observations = r.json().get("observations", [])
    if not observations:
        raise ValueError(f"Sin datos para {series_id}")
    df = pd.DataFrame(observations)[["date", "value"]]
    df["date"]  = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna(subset=["value"]).sort_values("date").reset_index(drop=True)


def build_uempjolt(jtsjol_df: pd.DataFrame) -> pd.DataFrame:
    unemploy_df = fetch_fred("UNEMPLOY")
    merged = pd.merge(
        unemploy_df.rename(columns={"value": "unemploy"}),
        jtsjol_df[["date", "value"]].rename(columns={"value": "jolts"}),
        on="date", how="inner",
    )
    merged["value"] = merged["unemploy"] / merged["jolts"]
    return merged[["date", "value"]].dropna().reset_index(drop=True)


# ─── BCB ──────────────────────────────────────────────────────────────────────

def fetch_bcb(serie: int) -> pd.DataFrame:
    """Descarga una serie del Banco Central do Brasil."""
    start_bcb = datetime.strptime(START_DATE, "%Y-%m-%d").strftime("%d/%m/%Y")
    end_bcb   = datetime.today().strftime("%d/%m/%Y")
    url = BCB_BASE_URL.format(serie=serie)
    params = {"formato": "json", "dataInicial": start_bcb, "dataFinal": end_bcb}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError(f"Sin datos para serie BCB {serie}")
    df = pd.DataFrame(data)
    # BCB devuelve columnas "data" y "valor"
    df = df.rename(columns={"data": "date", "valor": "value"})
    df["date"]  = pd.to_datetime(df["date"], format="%d/%m/%Y")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna(subset=["value"]).sort_values("date").reset_index(drop=True)


# ─── Argentina — INDEC API ────────────────────────────────────────────────────

def fetch_ar_cpi_indec():
    """Descarga CPI nivel desde INDEC API y calcula MoM% y YoY%."""
    url = ("https://apis.datos.gob.ar/series/api/series/"
           "?ids=148.3_INIVELNAL_DICI_M_26&limit=200&sort=asc"
           "&start_date=2017-01-01&format=json")
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    records = data.get("data", [])
    if not records:
        raise ValueError("Sin datos de INDEC CPI")
    df = pd.DataFrame(records, columns=["date", "value"])
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"]).sort_values("date").reset_index(drop=True)
    df["mom_pct"] = df["value"].pct_change(1) * 100
    df["yoy_pct"] = df["value"].pct_change(12) * 100
    # filter to START_DATE
    df = df[df["date"] >= pd.to_datetime(START_DATE)].reset_index(drop=True)
    return df


def fetch_ar_emae_csv():
    """Descarga EMAE mensual desde CSV directo de infra.datos.gob.ar."""
    url = (
        "https://infra.datos.gob.ar/catalog/sspm/dataset/143/distribution/"
        "143.3/download/emae-valores-anuales-indice-base-2004-mensual.csv"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    from io import StringIO
    df = pd.read_csv(StringIO(r.text))
    # columnas: indice_tiempo, emae_original, emae_desestacionalizada, ...
    if "indice_tiempo" not in df.columns or "emae_original" not in df.columns:
        raise ValueError(f"Columnas inesperadas en CSV EMAE: {list(df.columns)}")
    df = df[["indice_tiempo", "emae_original"]].rename(
        columns={"indice_tiempo": "date", "emae_original": "value"}
    )
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"]).sort_values("date").reset_index(drop=True)
    df = df[df["date"] >= pd.to_datetime("2017-01-01")].reset_index(drop=True)
    df["mom_pct"] = df["value"].pct_change(1) * 100
    df["yoy_pct"] = df["value"].pct_change(12) * 100
    if df.empty:
        raise ValueError("Sin datos de EMAE tras filtrar por fecha")
    return df


def fetch_cl_cpi_bis():
    """Descarga CPI Chile desde BIS API (WS_LONG_CPI).
    UNIT_MEASURE=628: índice nivel base → calcula MoM%
    UNIT_MEASURE=771: YoY% directo
    Retorna columnas: date, value (YoY%), mom_pct, yoy_pct
    """
    from io import StringIO
    url = (
        "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_LONG_CPI/1.0/M.CL."
        "?startPeriod=2016-01&format=csv"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    df_raw = pd.read_csv(StringIO(r.text))
    df_idx = (
        df_raw[df_raw["UNIT_MEASURE"] == 628][["TIME_PERIOD", "OBS_VALUE"]]
        .copy()
        .rename(columns={"TIME_PERIOD": "date", "OBS_VALUE": "idx"})
    )
    df_yoy = (
        df_raw[df_raw["UNIT_MEASURE"] == 771][["TIME_PERIOD", "OBS_VALUE"]]
        .copy()
        .rename(columns={"TIME_PERIOD": "date", "OBS_VALUE": "yoy_pct"})
    )
    if df_idx.empty or df_yoy.empty:
        raise ValueError("BIS CPI Chile: unidades 628/771 no encontradas en la respuesta")
    df_idx["date"] = pd.to_datetime(df_idx["date"])
    df_idx["idx"] = pd.to_numeric(df_idx["idx"], errors="coerce")
    df_yoy["date"] = pd.to_datetime(df_yoy["date"])
    df_yoy["yoy_pct"] = pd.to_numeric(df_yoy["yoy_pct"], errors="coerce")
    df_idx = df_idx.sort_values("date").reset_index(drop=True)
    df_idx["mom_pct"] = df_idx["idx"].pct_change(1) * 100
    df = pd.merge(df_idx[["date", "mom_pct"]], df_yoy, on="date", how="inner")
    df["value"] = df["yoy_pct"]
    df = df[["date", "value", "mom_pct", "yoy_pct"]].dropna(subset=["value"])
    df = df[df["date"] >= pd.to_datetime(START_DATE)].reset_index(drop=True)
    if df.empty:
        raise ValueError("Sin datos BIS CPI Chile tras filtrar por fecha")
    return df


def fetch_co_cpi_bis():
    """Descarga CPI Colombia desde BIS API (WS_LONG_CPI).
    UNIT_MEASURE=628: índice nivel → calcula MoM%
    UNIT_MEASURE=771: YoY% directo
    Retorna columnas: date, value (YoY%), mom_pct, yoy_pct
    """
    from io import StringIO
    url = (
        "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_LONG_CPI/1.0/M.CO."
        "?startPeriod=2016-01&format=csv"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    df_raw = pd.read_csv(StringIO(r.text))
    df_idx = (
        df_raw[df_raw["UNIT_MEASURE"] == 628][["TIME_PERIOD", "OBS_VALUE"]]
        .copy()
        .rename(columns={"TIME_PERIOD": "date", "OBS_VALUE": "idx"})
    )
    df_yoy = (
        df_raw[df_raw["UNIT_MEASURE"] == 771][["TIME_PERIOD", "OBS_VALUE"]]
        .copy()
        .rename(columns={"TIME_PERIOD": "date", "OBS_VALUE": "yoy_pct"})
    )
    if df_idx.empty or df_yoy.empty:
        raise ValueError("BIS CPI Colombia: unidades 628/771 no encontradas en la respuesta")
    df_idx["date"] = pd.to_datetime(df_idx["date"])
    df_idx["idx"] = pd.to_numeric(df_idx["idx"], errors="coerce")
    df_yoy["date"] = pd.to_datetime(df_yoy["date"])
    df_yoy["yoy_pct"] = pd.to_numeric(df_yoy["yoy_pct"], errors="coerce")
    df_idx = df_idx.sort_values("date").reset_index(drop=True)
    df_idx["mom_pct"] = df_idx["idx"].pct_change(1) * 100
    df = pd.merge(df_idx[["date", "mom_pct"]], df_yoy, on="date", how="inner")
    df["value"] = df["yoy_pct"]
    df = df[["date", "value", "mom_pct", "yoy_pct"]].dropna(subset=["value"])
    df = df[df["date"] >= pd.to_datetime(START_DATE)].reset_index(drop=True)
    if df.empty:
        raise ValueError("Sin datos BIS CPI Colombia tras filtrar por fecha")
    return df


def fetch_ar_unrate_indec():
    """Descarga desempleo trimestral desde INDEC API."""
    url = ("https://apis.datos.gob.ar/series/api/series/"
           "?ids=45.2_ECTDT_0_T_33&limit=50&sort=desc&format=json")
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    records = data.get("data", [])
    if not records:
        raise ValueError("Sin datos de INDEC desempleo")
    df = pd.DataFrame(records, columns=["date", "value"])
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce") * 100  # decimal a porcentaje
    df = df.dropna(subset=["value"]).sort_values("date").reset_index(drop=True)
    df = df[df["date"] >= pd.to_datetime(START_DATE)].reset_index(drop=True)
    return df


def fetch_inegi(series_id):
    """Descarga una serie del Banco de Indicadores INEGI (BISE).
    Retorna DataFrame con columnas date (YYYY-MM-01) y value (float).
    """
    url = (
        f"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml"
        f"/INDICATOR/{series_id}/es/00/false/BIE-BISE/2.0/{INEGI_API_KEY}?type=json"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    observations = r.json()["Series"][0]["OBSERVATIONS"]
    records = []
    for obs in observations:
        raw_val = obs.get("OBS_VALUE")
        if raw_val is None:
            continue
        try:
            val = float(raw_val)
        except (ValueError, TypeError):
            continue
        records.append({"date": obs["TIME_PERIOD"] + "-01", "value": val})
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=["value"]).sort_values("date").reset_index(drop=True)
    df = df[df["date"] >= pd.to_datetime(START_DATE)].reset_index(drop=True)
    return df


def fetch_banxico(series_id):
    """Descarga una serie diaria de la SIE API de Banxico.
    Retorna DataFrame con columnas date y value (float), ignorando registros 'N/E'.
    """
    url = (
        f"https://www.banxico.org.mx/SieAPIRest/service/v1"
        f"/series/{series_id}/datos/{START_DATE}/{END_DATE}"
    )
    headers = {"Bmx-Token": BANXICO_API_KEY, "Accept": "application/json"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    datos = r.json()["bmx"]["series"][0]["datos"]
    records = []
    for d in datos:
        if d["dato"] in ("N/E", "", None):
            continue
        try:
            val = float(d["dato"])
        except (ValueError, TypeError):
            continue
        records.append({
            "date": pd.to_datetime(d["fecha"], dayfirst=True),
            "value": val,
        })
    df = pd.DataFrame(records)
    df = df.dropna(subset=["value"]).sort_values("date").reset_index(drop=True)
    return df


def fetch_ar_cpi_argentinadatos():
    """Descarga CPI MoM% desde api.argentinadatos.com y calcula YoY% por producto acumulado."""
    url = "https://api.argentinadatos.com/v1/finanzas/indices/inflacion"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError("Sin datos de argentinadatos inflacion")
    df = pd.DataFrame(data).rename(columns={"fecha": "date", "valor": "value"})
    df["date"] = pd.to_datetime(df["date"]).dt.to_period("M").dt.to_timestamp()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"]).sort_values("date").reset_index(drop=True)
    df = df[df["date"] >= pd.to_datetime("2017-01-01")].reset_index(drop=True)
    df["mom_pct"] = df["value"]
    # YoY compuesto: ratio del cumprod contra 12 meses atrás
    cumprod = (1 + df["value"] / 100).cumprod()
    df["yoy_pct"] = (cumprod / cumprod.shift(12) - 1) * 100
    return df


def fetch_ar_dolar_argentinadatos():
    """Descarga histórico de cotizaciones dólar desde api.argentinadatos.com.
    Retorna serie mensual con value=oficial_venta, blue_rate=blue_venta.
    """
    url = "https://api.argentinadatos.com/v1/cotizaciones/dolares"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError("Sin datos de argentinadatos dolares")
    df = pd.DataFrame(data)
    df["fecha"] = pd.to_datetime(df["fecha"])
    df["venta"] = pd.to_numeric(df["venta"], errors="coerce")
    oficial = df[df["casa"] == "oficial"].set_index("fecha")["venta"].rename("value")
    blue    = df[df["casa"] == "blue"].set_index("fecha")["venta"].rename("blue_rate")
    merged = pd.concat([oficial, blue], axis=1).sort_index()
    merged = merged.resample("ME").last().reset_index().rename(columns={"fecha": "date"})
    merged = merged[merged["date"] >= pd.to_datetime("2017-01-01")].reset_index(drop=True)
    return merged.dropna(subset=["value"]).reset_index(drop=True)


def fetch_ar_dolar():
    """Descarga tipos de cambio spot desde dolarapi.com (solo valor actual)."""
    r = requests.get("https://dolarapi.com/v1/dolares", timeout=10)
    r.raise_for_status()
    return r.json()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"Descargando datos desde {START_DATE} hasta {END_DATE}\n")

    # ── EE.UU. (FRED) ────────────────────────────────────────────────────────
    print("=" * 55)
    print("  🇺🇸  UNITED STATES — FRED")
    print("=" * 55)
    us_summary = []
    downloaded = {}

    for sid, meta in US_INDICATORS.items():
        try:
            print(f"  [{sid}] {meta['name']}...", end=" ", flush=True)
            df = fetch_fred(sid)
            downloaded[sid] = df
            df = add_changes(df, meta["mom"], meta["yoy"])
            df.to_csv(os.path.join(DATA_DIR, f"{sid}.csv"), index=False)
            row = make_summary_row(sid, meta["name"], df, source="FRED")
            us_summary.append(row)
            print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
        except Exception as e:
            print(f"ERROR: {e}")
            us_summary.append(empty_summary_row(sid, meta["name"], source="FRED"))

    # UEMPJOLT derivado
    print(f"  [UEMPJOLT] Unemployed per Job Opening (derivado)...", end=" ", flush=True)
    try:
        jtsjol_df = downloaded["JTSJOL"] if "JTSJOL" in downloaded else fetch_fred("JTSJOL")
        udf = build_uempjolt(jtsjol_df)
        udf.to_csv(os.path.join(DATA_DIR, "UEMPJOLT.csv"), index=False)
        row = make_summary_row("UEMPJOLT", "Unemployed per Job Opening", udf, source="FRED")
        us_summary.append(row)
        print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
    except Exception as e:
        print(f"ERROR: {e}")
        us_summary.append(empty_summary_row("UEMPJOLT", "Unemployed per Job Opening", source="FRED"))

    pd.DataFrame(us_summary).to_csv(os.path.join(DATA_DIR, "summary.csv"), index=False)
    ok_us = sum(1 for r in us_summary if r["rows"] > 0)
    print(f"\n  ✓ EE.UU.: {ok_us}/{len(us_summary)} series OK — guardado en data/summary.csv\n")

    # ── Brasil (BCB) ─────────────────────────────────────────────────────────
    print("=" * 55)
    print("  🇧🇷  BRASIL — Banco Central do Brasil (BCB)")
    print("=" * 55)
    br_summary = []

    for file_key, meta in BR_INDICATORS.items():
        try:
            print(f"  [{file_key} / serie {meta['serie']}] {meta['name']}...", end=" ", flush=True)
            df = fetch_bcb(meta["serie"])
            df = add_changes(df, meta["mom"], meta["yoy"])
            df.to_csv(os.path.join(DATA_DIR, f"{file_key}.csv"), index=False)
            row = make_summary_row(file_key, meta["name"], df, source="BCB")
            br_summary.append(row)
            print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
        except Exception as e:
            print(f"ERROR: {e}")
            br_summary.append(empty_summary_row(file_key, meta["name"], source="BCB"))

    pd.DataFrame(br_summary).to_csv(os.path.join(DATA_DIR, "br_summary.csv"), index=False)
    ok_br = sum(1 for r in br_summary if r["rows"] > 0)
    print(f"\n  ✓ Brasil: {ok_br}/{len(br_summary)} series OK — guardado en data/br_summary.csv\n")

    # ── LATAM: Chile, Colombia, México (FRED) ────────────────────────────────
    for country in LATAM_COUNTRIES:
        code = country["code"]
        print("=" * 55)
        print(f"  {country['flag']}  {country['name'].upper()} — FRED")
        print("=" * 55)
        latam_summary = []

        # CPI: combina YoY (valor) + MoM (mom_pct) en un solo CSV
        file_key = f"{code}_cpi"
        cpi_source = "FRED/OECD" if code in ("cl", "mx") else "DANE (estimado)"
        if code == "cl":
            # Chile: BIS API primero (último dato ~1 mes), FRED/OECD como fallback (850d rezago)
            try:
                print(f"  [{file_key}] CPI Chile (BIS API)...", end=" ", flush=True)
                df_cl_cpi = fetch_cl_cpi_bis()
                df_cl_cpi.to_csv(os.path.join(DATA_DIR, f"{file_key}.csv"), index=False)
                row = make_summary_row(file_key, "CPI YoY/MoM — Chile (BIS)", df_cl_cpi, source="BIS")
                latam_summary.append(row)
                print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
            except Exception as e:
                print(f"ERROR BIS: {e} — fallback FRED/OECD...", end=" ", flush=True)
                try:
                    yoy_df = fetch_fred(country["cpi_yoy"])
                    mom_df = fetch_fred(country["cpi_mom"])
                    merged = pd.merge(
                        yoy_df[["date", "value"]],
                        mom_df[["date", "value"]].rename(columns={"value": "mom_pct"}),
                        on="date", how="inner"
                    ).sort_values("date").reset_index(drop=True)
                    merged.to_csv(os.path.join(DATA_DIR, f"{file_key}.csv"), index=False)
                    row = make_summary_row(file_key, "CPI YoY/MoM — Chile (FRED fallback)", merged, source="FRED/OECD")
                    latam_summary.append(row)
                    print(f"OK fallback ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
                except Exception as e2:
                    print(f"ERROR: {e2}")
                    latam_summary.append(empty_summary_row(file_key, "CPI YoY/MoM — Chile", source="BIS/FRED"))
        elif country["cpi_yoy"] is not None:
            try:
                print(f"  [{file_key}] CPI YoY/MoM (FRED)...", end=" ", flush=True)
                yoy_df = fetch_fred(country["cpi_yoy"])
                mom_df = fetch_fred(country["cpi_mom"])
                merged = pd.merge(
                    yoy_df[["date", "value"]],
                    mom_df[["date", "value"]].rename(columns={"value": "mom_pct"}),
                    on="date", how="inner"
                ).sort_values("date").reset_index(drop=True)
                merged.to_csv(os.path.join(DATA_DIR, f"{file_key}.csv"), index=False)
                row = make_summary_row(file_key, f"CPI YoY/MoM — {country['name']}", merged, source=cpi_source)
                latam_summary.append(row)
                print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
            except Exception as e:
                print(f"ERROR: {e}")
                latam_summary.append(empty_summary_row(file_key, f"CPI YoY/MoM — {country['name']}", source=cpi_source))
        elif code == "co":
            # Colombia: BIS API primero, fallback al hardcoded DANE
            try:
                print(f"  [{file_key}] CPI Colombia (BIS API)...", end=" ", flush=True)
                df_co_cpi = fetch_co_cpi_bis()
                df_co_cpi.to_csv(os.path.join(DATA_DIR, f"{file_key}.csv"), index=False)
                row = make_summary_row(file_key, "CPI YoY/MoM — Colombia (BIS)", df_co_cpi, source="BIS")
                latam_summary.append(row)
                print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
            except Exception as e:
                print(f"ERROR BIS: {e} — fallback hardcoded DANE...", end=" ", flush=True)
                rows = CO_HARDCODED["co_cpi"]["data_yoy"]
                df = pd.DataFrame(rows, columns=["date", "value", "mom_pct"])
                df["date"] = pd.to_datetime(df["date"])
                df.sort_values("date", inplace=True)
                df.reset_index(drop=True, inplace=True)
                df.to_csv(os.path.join(DATA_DIR, f"{file_key}.csv"), index=False)
                row = make_summary_row(file_key, CO_HARDCODED["co_cpi"]["name"], df, source="DANE (estimado)")
                latam_summary.append(row)
                print(f"OK fallback ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")

        # Tasa de política monetaria
        file_key = f"{code}_rate"
        rate_source = "FRED"
        if country["rate"] is not None:
            try:
                print(f"  [{file_key}] Policy Rate (FRED)...", end=" ", flush=True)
                df = fetch_fred(country["rate"])
                df.to_csv(os.path.join(DATA_DIR, f"{file_key}.csv"), index=False)
                row = make_summary_row(file_key, f"Tasa política — {country['name']}", df, source=rate_source)
                latam_summary.append(row)
                print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
            except Exception as e:
                print(f"ERROR: {e}")
                latam_summary.append(empty_summary_row(file_key, f"Tasa política — {country['name']}", source=rate_source))

        # Desempleo
        file_key = f"{code}_unrate"
        unrate_source = "FRED" if code in ("cl", "mx") else "DANE (estimado)"
        if country["unrate"] is not None:
            try:
                print(f"  [{file_key}] Unemployment (FRED)...", end=" ", flush=True)
                df = fetch_fred(country["unrate"])
                df.to_csv(os.path.join(DATA_DIR, f"{file_key}.csv"), index=False)
                row = make_summary_row(file_key, f"Desempleo — {country['name']}", df, source=unrate_source)
                latam_summary.append(row)
                print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
            except Exception as e:
                print(f"ERROR FRED: {e}", end=" ", flush=True)
                if code == "co" and "co_unrate" in CO_HARDCODED:
                    print("— fallback hardcodeado...", end=" ", flush=True)
                    df = pd.DataFrame(CO_HARDCODED["co_unrate"]["data"], columns=["date", "value"])
                    df["date"] = pd.to_datetime(df["date"])
                    df.to_csv(os.path.join(DATA_DIR, f"{file_key}.csv"), index=False)
                    row = make_summary_row(file_key, CO_HARDCODED["co_unrate"]["name"], df, source="DANE (estimado)")
                    latam_summary.append(row)
                    print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
                else:
                    print()
                    latam_summary.append(empty_summary_row(file_key, f"Desempleo — {country['name']}", source=unrate_source))

        # Actividad económica (solo Chile tiene serie FRED)
        if country["activity"]:
            act = country["activity"]
            file_key = f"{code}_gdp"
            try:
                print(f"  [{file_key}] Activity/GDP...", end=" ", flush=True)
                df = fetch_fred(act["id"])
                df = add_changes(df, act["mom"], act["yoy"])
                df.to_csv(os.path.join(DATA_DIR, f"{file_key}.csv"), index=False)
                row = make_summary_row(file_key, act["name"], df, source="FRED")
                latam_summary.append(row)
                print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
            except Exception as e:
                print(f"ERROR: {e}")
                latam_summary.append(empty_summary_row(file_key, act["name"], source="FRED"))

        # FX rates: desde FRED daily → resample mensual; fallback hardcodeado para CO
        fx_id = f"{code}_usd"
        if country.get("fx_fred"):
            fred_series = country["fx_fred"]
            try:
                print(f"  [{fx_id}] FX rate {fred_series} (FRED daily → monthly)...", end=" ", flush=True)
                df_fx = fetch_fred(fred_series)
                df_fx = df_fx.set_index("date").resample("ME").last().reset_index()
                df_fx.to_csv(os.path.join(DATA_DIR, f"{fx_id}.csv"), index=False)
                row = make_summary_row(fx_id, f"Tipo de Cambio — {country['name']} (vs USD)", df_fx, source="FRED")
                latam_summary.append(row)
                print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
            except Exception as e:
                print(f"ERROR FRED: {e}", end=" ", flush=True)
                if code == "co":
                    print("— fallback hardcodeado...", end=" ", flush=True)
                    df_co_fx = pd.DataFrame(CO_USD_HARDCODED, columns=["date", "value"])
                    df_co_fx["date"] = pd.to_datetime(df_co_fx["date"])
                    df_co_fx.to_csv(os.path.join(DATA_DIR, f"{fx_id}.csv"), index=False)
                    row = make_summary_row(fx_id, "Tipo de Cambio — Colombia (COP/USD)", df_co_fx, source="hardcoded")
                    latam_summary.append(row)
                    print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
                else:
                    print()
                    latam_summary.append(empty_summary_row(fx_id, f"Tipo de Cambio — {country['name']}", source="FRED"))
        elif code == "co":
            print(f"  [{fx_id}] COP/USD (hardcodeado)...", end=" ", flush=True)
            df_co_fx = pd.DataFrame(CO_USD_HARDCODED, columns=["date", "value"])
            df_co_fx["date"] = pd.to_datetime(df_co_fx["date"])
            df_co_fx.to_csv(os.path.join(DATA_DIR, f"{fx_id}.csv"), index=False)
            row = make_summary_row(fx_id, "Tipo de Cambio — Colombia (COP/USD)", df_co_fx, source="hardcoded")
            latam_summary.append(row)
            print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")

        pd.DataFrame(latam_summary).to_csv(os.path.join(DATA_DIR, f"{code}_summary.csv"), index=False)
        ok_c = sum(1 for r in latam_summary if r["rows"] > 0)
        print(f"\n  ✓ {country['name']}: {ok_c}/{len(latam_summary)} series OK — guardado en data/{code}_summary.csv\n")

    # ── México (INEGI + Banxico) ──────────────────────────────────────────────
    print("=" * 55)
    print("  🇲🇽  MÉXICO — INEGI + Banxico SIE")
    print("=" * 55)
    mx_summary = []

    # mx_cpi — INPC serie 910392 → calcular MoM% y YoY% desde el índice
    try:
        print("  [mx_cpi] INPC (INEGI 910392)...", end=" ", flush=True)
        df_inpc = fetch_inegi("910392")
        df_inpc["mom_pct"] = df_inpc["value"].pct_change(periods=1) * 100
        df_inpc["yoy_pct"] = df_inpc["value"].pct_change(periods=12) * 100
        # Guardar: value = índice INPC, mom_pct, yoy_pct
        df_mx_cpi = df_inpc.copy()
        df_mx_cpi.to_csv(os.path.join(DATA_DIR, "mx_cpi.csv"), index=False)
        row = make_summary_row("mx_cpi", "INPC — México (INEGI)", df_mx_cpi, source="INEGI")
        mx_summary.append(row)
        print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
    except Exception as e:
        print(f"ERROR: {e}")
        mx_summary.append(empty_summary_row("mx_cpi", "INPC — México (INEGI)", source="INEGI"))

    # mx_rate — Tasa objetivo Banxico SF61745 → resamplear mensual (último del mes)
    try:
        print("  [mx_rate] Tasa objetivo (Banxico SF61745)...", end=" ", flush=True)
        df_rate = fetch_banxico("SF61745")
        df_mx_rate = (
            df_rate.set_index("date")
            .resample("MS").last()
            .reset_index()
        )
        df_mx_rate.to_csv(os.path.join(DATA_DIR, "mx_rate.csv"), index=False)
        row = make_summary_row("mx_rate", "Tasa Objetivo — Banxico", df_mx_rate, source="Banxico")
        mx_summary.append(row)
        print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
    except Exception as e:
        print(f"ERROR: {e}")
        mx_summary.append(empty_summary_row("mx_rate", "Tasa Objetivo — Banxico", source="Banxico"))

    # mx_unrate — Desempleo ENOE (INEGI 444612)
    try:
        print("  [mx_unrate] Desempleo ENOE (INEGI 444612)...", end=" ", flush=True)
        df_mx_unrate = fetch_inegi("444612")
        df_mx_unrate.to_csv(os.path.join(DATA_DIR, "mx_unrate.csv"), index=False)
        row = make_summary_row("mx_unrate", "Desempleo ENOE — México (INEGI)", df_mx_unrate, source="INEGI")
        mx_summary.append(row)
        print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
    except Exception as e:
        print(f"ERROR: {e}")
        mx_summary.append(empty_summary_row("mx_unrate", "Desempleo ENOE — México (INEGI)", source="INEGI"))

    # mx_usd — FIX MXN/USD Banxico SF43718 → resamplear mensual (último del mes)
    try:
        print("  [mx_usd] FIX MXN/USD (Banxico SF43718)...", end=" ", flush=True)
        df_fix = fetch_banxico("SF43718")
        df_mx_usd = (
            df_fix.set_index("date")
            .resample("ME").last()
            .reset_index()
        )
        df_mx_usd.to_csv(os.path.join(DATA_DIR, "mx_usd.csv"), index=False)
        row = make_summary_row("mx_usd", "Tipo de Cambio FIX — México (Banxico)", df_mx_usd, source="Banxico")
        mx_summary.append(row)
        print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
    except Exception as e:
        print(f"ERROR: {e}")
        mx_summary.append(empty_summary_row("mx_usd", "Tipo de Cambio FIX — México (Banxico)", source="Banxico"))

    pd.DataFrame(mx_summary).to_csv(os.path.join(DATA_DIR, "mx_summary.csv"), index=False)
    ok_mx = sum(1 for r in mx_summary if r["rows"] > 0)
    print(f"\n  ✓ México: {ok_mx}/{len(mx_summary)} series OK — guardado en data/mx_summary.csv\n")

    # ── Argentina (INDEC API + fallback hardcoded) ────────────────────────────
    print("=" * 55)
    print("  🇦🇷  ARGENTINA — INDEC API + BCRA (hardcoded)")
    print("=" * 55)
    ar_summary = []

    # CPI: argentinadatos.com (primario) → INDEC API → hardcoded fallback
    print(f"  [ar_cpi] CPI (argentinadatos.com)...", end=" ", flush=True)
    ar_cpi_source = "argentinadatos.com"
    try:
        df_ar_cpi = fetch_ar_cpi_argentinadatos()
        df_ar_cpi.to_csv(os.path.join(DATA_DIR, "ar_cpi.csv"), index=False)
        row = make_summary_row("ar_cpi", "CPI MoM% — INDEC vía argentinadatos", df_ar_cpi, source=ar_cpi_source)
        ar_summary.append(row)
        print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
    except Exception as e:
        print(f"ERROR: {e} — intentando INDEC API")
        ar_cpi_source = "INDEC API"
        try:
            df_ar_cpi = fetch_ar_cpi_indec()
            df_ar_cpi.to_csv(os.path.join(DATA_DIR, "ar_cpi.csv"), index=False)
            row = make_summary_row("ar_cpi", "CPI (INDEC) — IPC Nacional Base Dic 2016", df_ar_cpi, source=ar_cpi_source)
            ar_summary.append(row)
            print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
        except Exception as e2:
            print(f"ERROR: {e2} — usando fallback hardcoded")
            ar_cpi_source = "INDEC (hardcoded)"
            fb = AR_HARDCODED["ar_cpi_mom_fallback"]
            df_fb = pd.DataFrame(fb["data"], columns=["date", "value"])
            df_fb["date"] = pd.to_datetime(df_fb["date"])
            df_fb["mom_pct"] = df_fb["value"]
            cumprod = (1 + df_fb["value"] / 100).cumprod()
            df_fb["yoy_pct"] = (cumprod / cumprod.shift(12) - 1) * 100
            df_fb.to_csv(os.path.join(DATA_DIR, "ar_cpi.csv"), index=False)
            row = make_summary_row("ar_cpi", "CPI (INDEC) — fallback hardcoded", df_fb, source=ar_cpi_source)
            ar_summary.append(row)

    # EMAE desde CSV directo infra.datos.gob.ar
    print(f"  [ar_emae] EMAE CSV infra.datos.gob.ar...", end=" ", flush=True)
    try:
        df_ar_emae = fetch_ar_emae_csv()
        df_ar_emae.to_csv(os.path.join(DATA_DIR, "ar_emae.csv"), index=False)
        row = make_summary_row("ar_emae", "EMAE (2004=100) — infra.datos.gob.ar", df_ar_emae, source="infra.datos.gob.ar")
        ar_summary.append(row)
        print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
    except Exception as e:
        print(f"ERROR: {e} — usando fallback hardcoded")
        fb = AR_HARDCODED["ar_emae"]
        df_fb = pd.DataFrame(fb["data"], columns=["date", "value"])
        df_fb["date"] = pd.to_datetime(df_fb["date"])
        df_fb = add_changes(df_fb, mom=True, yoy=True)
        df_fb.to_csv(os.path.join(DATA_DIR, "ar_emae.csv"), index=False)
        row = make_summary_row("ar_emae", fb["name"], df_fb, source="INDEC (hardcoded)")
        ar_summary.append(row)

    # Desempleo desde INDEC API
    print(f"  [ar_unrate] Desempleo INDEC API...", end=" ", flush=True)
    try:
        df_ar_unrate = fetch_ar_unrate_indec()
        df_ar_unrate.to_csv(os.path.join(DATA_DIR, "ar_unrate.csv"), index=False)
        row = make_summary_row("ar_unrate", "Desempleo EPH — INDEC", df_ar_unrate, source="INDEC API")
        ar_summary.append(row)
        print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")
    except Exception as e:
        print(f"ERROR: {e} — usando fallback hardcoded")
        fb = AR_HARDCODED["ar_unrate"]
        df_fb = pd.DataFrame(fb["data"], columns=["date", "value"])
        df_fb["date"] = pd.to_datetime(df_fb["date"])
        df_fb.to_csv(os.path.join(DATA_DIR, "ar_unrate.csv"), index=False)
        row = make_summary_row("ar_unrate", fb["name"], df_fb, source="INDEC (hardcoded)")
        ar_summary.append(row)

    # Tasa BCRA (hardcoded)
    print(f"  [ar_rate] Tasa BCRA (hardcoded)...", end=" ", flush=True)
    fb = AR_HARDCODED["ar_rate"]
    df_ar_rate = pd.DataFrame(fb["data"], columns=["date", "value"])
    df_ar_rate["date"] = pd.to_datetime(df_ar_rate["date"])
    df_ar_rate.to_csv(os.path.join(DATA_DIR, "ar_rate.csv"), index=False)
    row = make_summary_row("ar_rate", fb["name"], df_ar_rate, source="BCRA (hardcoded)")
    ar_summary.append(row)
    print(f"OK ({row['rows']} filas, último: {row['last_value']} al {row['last_date']})")

    # Dólar: argentinadatos.com histórico (primario) → dolarapi.com spot (fallback)
    print(f"  [ar_dolar] Dólar histórico (argentinadatos.com)...", end=" ", flush=True)
    try:
        df_dolar = fetch_ar_dolar_argentinadatos()
        df_dolar.to_csv(os.path.join(DATA_DIR, "ar_dolar.csv"), index=False)
        row = make_summary_row("ar_dolar", "Tipo de Cambio Oficial/Blue — ARS/USD", df_dolar, source="argentinadatos.com")
        ar_summary.append(row)
        print(f"OK ({row['rows']} filas, último oficial: {row['last_value']} al {row['last_date']})")
    except Exception as e:
        print(f"ERROR: {e} — usando dolarapi.com (solo spot)")
        try:
            dolares = fetch_ar_dolar()
            with open(os.path.join(DATA_DIR, "ar_dolar.json"), "w", encoding="utf-8") as f:
                json.dump(dolares, f, ensure_ascii=False, indent=2)
            oficial_val, blue_val = None, None
            for d in dolares:
                casa = d.get("casa", "").lower()
                if casa == "oficial":
                    oficial_val = d.get("venta")
                elif casa == "blue":
                    blue_val = d.get("venta")
            today_str = datetime.today().strftime("%Y-%m-%d")
            df_dolar = pd.DataFrame([{
                "date": today_str,
                "value": oficial_val if oficial_val is not None else 0,
                "blue_rate": blue_val if blue_val is not None else 0,
            }])
            df_dolar["date"] = pd.to_datetime(df_dolar["date"])
            df_dolar.to_csv(os.path.join(DATA_DIR, "ar_dolar.csv"), index=False)
            row = make_summary_row("ar_dolar", "Tipo de Cambio — Oficial vs Blue (spot)", df_dolar, source="dolarapi.com")
            ar_summary.append(row)
            print(f"OK (spot) — Oficial: {oficial_val}, Blue: {blue_val}")
        except Exception as e2:
            print(f"ERROR: {e2} (no crítico)")
            today_str = datetime.today().strftime("%Y-%m-%d")
            df_dolar_empty = pd.DataFrame([{"date": today_str, "value": 0, "blue_rate": 0}])
            df_dolar_empty["date"] = pd.to_datetime(df_dolar_empty["date"])
            df_dolar_empty.to_csv(os.path.join(DATA_DIR, "ar_dolar.csv"), index=False)
            ar_summary.append(empty_summary_row("ar_dolar", "Tipo de Cambio (error)", source="argentinadatos.com"))

    pd.DataFrame(ar_summary).to_csv(os.path.join(DATA_DIR, "ar_summary.csv"), index=False)
    ok_ar = sum(1 for r in ar_summary if r.get("rows", 0) > 0)
    print(f"\n  ✓ Argentina: {ok_ar}/{len(ar_summary)} series OK — data/ar_summary.csv\n")

    print("Descarga completa.")


if __name__ == "__main__":
    main()
