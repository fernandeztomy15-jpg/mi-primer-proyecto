"""
generate_dashboard.py
Multi-country macro dashboard: 🇺🇸 USA (FRED) | 🇧🇷 Brasil (BCB)
"""

import os
import json
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
from datetime import datetime

DATA_DIR = "data"

# ─── US Indicators ────────────────────────────────────────────────────────────
US_INDICATORS = [
    {"id": "CPILFESL",        "name": "Core CPI",               "subtitle": "Consumer Price Index ex Food & Energy",                "unit": "Index",           "mom": True,  "yoy": True,  "val_fmt": "num",   "source": "FRED"},
    {"id": "UMCSENT",         "name": "Consumer Sentiment",     "subtitle": "University of Michigan Consumer Sentiment",           "unit": "Index",           "mom": False, "yoy": False, "val_fmt": "num",   "source": "FRED"},
    {"id": "RSXFS",           "name": "Retail Sales",           "subtitle": "Advance Retail Sales: Retail Trade & Food Services",  "unit": "Mil USD",         "mom": True,  "yoy": True,  "val_fmt": "large", "source": "FRED"},
    {"id": "PPIACO",          "name": "Producer Price Index",   "subtitle": "PPI: All Commodities",                                "unit": "Index (1982=100)", "mom": True,  "yoy": True,  "val_fmt": "num",   "source": "FRED"},
    {"id": "ICSA",            "name": "Initial Jobless Claims", "subtitle": "Initial Claims for Unemployment Insurance",           "unit": "Persons",         "mom": False, "yoy": False, "val_fmt": "large", "source": "FRED"},
    {"id": "A191RL1Q225SBEA", "name": "GDP Growth QoQ",        "subtitle": "Real Gross Domestic Product — Quarterly Growth Rate", "unit": "% QoQ",           "mom": False, "yoy": False, "val_fmt": "pct",   "source": "FRED"},
    {"id": "INDPRO",          "name": "Industrial Production",  "subtitle": "Industrial Production Index",                         "unit": "Index (2017=100)", "mom": True,  "yoy": True,  "val_fmt": "num",   "source": "FRED"},
    {"id": "FEDFUNDS",        "name": "Federal Funds Rate",     "subtitle": "Effective Federal Funds Rate",                        "unit": "%",               "mom": False, "yoy": False, "val_fmt": "pct",   "source": "FRED"},
    {"id": "PCEPILFE",        "name": "Core PCE Price Index",   "subtitle": "PCE ex Food & Energy",                                "unit": "Index (2017=100)", "mom": True,  "yoy": True,  "val_fmt": "num",   "source": "FRED"},
    {"id": "JTSJOL",          "name": "JOLTs Job Openings",     "subtitle": "Job Openings: Total Nonfarm",                         "unit": "Thousands",       "mom": False, "yoy": False, "val_fmt": "large", "source": "FRED"},
    {"id": "UEMPJOLT",        "name": "Unemployed/Job Opening", "subtitle": "Unemployment Level / JOLTs Openings (derivado)",     "unit": "Ratio",           "mom": False, "yoy": False, "val_fmt": "num",   "source": "FRED"},
    {"id": "UNRATE",          "name": "Unemployment Rate",      "subtitle": "Civilian Unemployment Rate",                          "unit": "%",               "mom": False, "yoy": False, "val_fmt": "pct",   "source": "FRED"},
]

# ─── BR Indicators ────────────────────────────────────────────────────────────
BR_INDICATORS = [
    {"id": "br_ipca",      "name": "IPCA Monthly Change",       "subtitle": "BCB 433 — Variação mensal (%)",                        "unit": "%",       "mom": False, "yoy": False, "val_fmt": "pct",   "source": "BCB"},
    {"id": "br_icc",       "name": "Consumer Confidence (ICC)", "subtitle": "BCB 4393 — Índice de Confiança do Consumidor",         "unit": "Index",   "mom": False, "yoy": False, "val_fmt": "num",   "source": "BCB"},
    {"id": "br_retail",    "name": "Retail Sales",              "subtitle": "BCB 1455 — Pesquisa Mensal de Comércio (2022=100)",    "unit": "Index",   "mom": False, "yoy": True,  "val_fmt": "num",   "source": "BCB"},
    {"id": "br_ipp",       "name": "Producer Price Index (IPP)","subtitle": "BCB 11426 — IPP variação mensal (%)",                  "unit": "%",       "mom": True,  "yoy": True,  "val_fmt": "pct",   "source": "BCB"},
    {"id": "br_gdp",       "name": "GDP Real Index",            "subtitle": "BCB 22109 — Índice encadeado real (1995=100)",         "unit": "Index",   "mom": True,  "yoy": True,  "val_fmt": "num",   "source": "BCB"},
    {"id": "br_indpro",    "name": "Industrial Production",     "subtitle": "BCB 21859 — Produção Industrial (2012=100)",           "unit": "Index",   "mom": True,  "yoy": True,  "val_fmt": "num",   "source": "BCB"},
    {"id": "br_selic",     "name": "Selic Rate",                "subtitle": "BCB 4189 — Taxa Selic acumulada no mês (%)",           "unit": "%",       "mom": False, "yoy": False, "val_fmt": "pct",   "source": "BCB"},
    {"id": "br_ipca_core", "name": "Core IPCA Monthly",        "subtitle": "BCB 11427 — IPCA Núcleo variação mensal (%)",          "unit": "%",       "mom": False, "yoy": False, "val_fmt": "pct",   "source": "BCB"},
    {"id": "br_caged",     "name": "Formal Employment (CAGED)", "subtitle": "BCB 28763 — Stock de empregos formais",                "unit": "Workers", "mom": True,  "yoy": True,  "val_fmt": "large", "source": "BCB"},
    {"id": "br_unrate",    "name": "Unemployment Rate (PNAD)",  "subtitle": "BCB 24369 — Taxa de desocupação (%)",                  "unit": "%",       "mom": False, "yoy": False, "val_fmt": "pct",   "source": "BCB"},
    {"id": "br_usdbrl",    "name": "BRL/USD Exchange Rate",     "subtitle": "BCB 10813 — Dólar americano fim de período (R$/US$)", "unit": "R$/US$",  "mom": False, "yoy": False, "val_fmt": "num",   "source": "BCB", "resample": "ME"},
]

# ─── CL Indicators ────────────────────────────────────────────────────────────
CL_INDICATORS = [
    {"id": "cl_cpi",    "name": "CPI (IPC)",    "subtitle": "FRED — nivel: YoY% · barra: MoM%",            "unit": "% YoY",  "mom": True,  "yoy": False, "val_fmt": "pct",   "source": "FRED/OECD"},
    {"id": "cl_rate",   "name": "Tasa BCCh",    "subtitle": "FRED IRSTCB01CLM156N — Tasa de política",     "unit": "%",       "mom": False, "yoy": False, "val_fmt": "pct",   "source": "FRED"},
    {"id": "cl_unrate", "name": "Desempleo",    "subtitle": "FRED LRUN74TTCLM156S — Tasa de desocupación", "unit": "%",       "mom": False, "yoy": False, "val_fmt": "pct",   "source": "FRED"},
    {"id": "cl_gdp",    "name": "PIB (índice)", "subtitle": "FRED NAEXKP01CLQ657S — Trimestral",           "unit": "Index",   "mom": True,  "yoy": True,  "val_fmt": "num",   "source": "FRED"},
    {"id": "cl_usd",    "name": "CLP/USD",      "subtitle": "FRED DEXCHUS — Daily resampled to monthly",   "unit": "CLP/USD", "mom": False, "yoy": False, "val_fmt": "large", "source": "FRED"},
]

# ─── CO Indicators ────────────────────────────────────────────────────────────
CO_INDICATORS = [
    {"id": "co_cpi",    "name": "CPI",          "subtitle": "DANE — nivel: YoY% · barra: MoM%",            "unit": "% YoY",  "mom": True,  "yoy": False, "val_fmt": "pct",   "source": "DANE (estimado)"},
    {"id": "co_rate",   "name": "Tasa BanRep",  "subtitle": "FRED COLIR3TIB01STM — Tasa de política",      "unit": "%",       "mom": False, "yoy": False, "val_fmt": "pct",   "source": "FRED"},
    {"id": "co_unrate", "name": "Desempleo",    "subtitle": "DANE GEIH — Tasa de desocupación mensual",    "unit": "%",       "mom": False, "yoy": False, "val_fmt": "pct",   "source": "DANE (estimado)"},
    {"id": "co_usd",    "name": "COP/USD",      "subtitle": "Estimado — Actualización manual",              "unit": "COP/USD", "mom": False, "yoy": False, "val_fmt": "large", "source": "hardcoded"},
]

# ─── MX Indicators ────────────────────────────────────────────────────────────
MX_INDICATORS = [
    {"id": "mx_cpi",    "name": "CPI (INPC)",   "subtitle": "INEGI — INPC General Base 2da Quincena Jul 2018 = 100",        "unit": "Índice", "mom": True,  "yoy": False, "val_fmt": "num",   "source": "INEGI"},
    {"id": "mx_rate",   "name": "Tasa Banxico", "subtitle": "Banxico SIE SF61745 — Tasa objetivo de política monetaria",    "unit": "%",       "mom": False, "yoy": False, "val_fmt": "pct",   "source": "Banxico"},
    {"id": "mx_unrate", "name": "Desempleo",    "subtitle": "INEGI ENOE 444612 — Tasa de desocupación (%)",                 "unit": "%",       "mom": False, "yoy": False, "val_fmt": "pct",   "source": "INEGI"},
    {"id": "mx_usd",    "name": "MXN/USD",      "subtitle": "Banxico SIE SF43718 — Tipo de cambio FIX (MXN/USD)",           "unit": "MXN/USD", "mom": False, "yoy": False, "val_fmt": "num",   "source": "Banxico"},
]

# ─── AR Indicators ────────────────────────────────────────────────────────────
AR_INDICATORS = [
    {"id": "ar_cpi",    "name": "CPI (INDEC)",       "subtitle": "INDEC — IPC Nacional Base Dic 2016",           "unit": "%",       "mom": False, "yoy": True,  "val_fmt": "pct", "source": "INDEC API"},
    {"id": "ar_rate",   "name": "Tasa BCRA",          "subtitle": "BCRA — Tasa política (% anual)",               "unit": "% anual", "mom": False, "yoy": False, "val_fmt": "pct", "source": "BCRA (hardcoded)"},
    {"id": "ar_unrate", "name": "Desempleo EPH",      "subtitle": "INDEC EPH — Trimestral",                       "unit": "%",       "mom": False, "yoy": False, "val_fmt": "pct", "source": "INDEC API"},
    {"id": "ar_emae",   "name": "EMAE (2004=100)",    "subtitle": "INDEC — Actividad mensual",                    "unit": "Index",   "mom": True,  "yoy": True,  "val_fmt": "num", "source": "INDEC API"},
    {"id": "ar_dolar",  "name": "Tipo de Cambio",     "subtitle": "Oficial vs Blue (ARS/USD)",                    "unit": "ARS/USD", "mom": False, "yoy": False, "val_fmt": "large","source": "dolarapi.com"},
]

# ─── CPI indicators: show only YoY% and MoM%, no level ──────────────────────
CPI_INDICATOR_IDS = {
    "mx_cpi", "cl_cpi", "co_cpi", "ar_cpi",
    "br_ipca", "br_ipca_core",
    "CPILFESL", "PCEPILFE", "PPIACO",
}
# value column IS already the MoM% (will derive YoY via cumulative product)
CPI_VALUE_IS_MOM = {"br_ipca", "br_ipca_core"}
# value column IS already the YoY% (just alias it; mom_pct already in CSV)
CPI_VALUE_IS_YOY = {"mx_cpi", "cl_cpi", "co_cpi"}

# ─── Color direction (US + BR + LATAM combined) ───────────────────────────────
COLOR_DIRECTION = {
    # US
    "CPILFESL": "inflation",  "PCEPILFE": "inflation",  "PPIACO": "inflation",
    "INDPRO":   "activity",   "A191RL1Q225SBEA": "activity",
    "RSXFS":    "activity",   "JTSJOL": "activity",     "UMCSENT": "activity",
    "UEMPJOLT": "unemployment", "UNRATE": "unemployment", "ICSA": "unemployment",
    "FEDFUNDS": "neutral",
    # BR
    "br_ipca":      "inflation",  "br_ipca_core": "inflation", "br_ipp": "inflation",
    "br_retail":    "activity",   "br_gdp": "activity",
    "br_indpro":    "activity",   "br_caged": "activity",      "br_icc": "activity",
    "br_unrate":    "unemployment",
    "br_selic":     "neutral",    "br_usdbrl": "neutral",
    # CL
    "cl_cpi": "inflation", "cl_rate": "neutral", "cl_unrate": "unemployment", "cl_gdp": "activity",
    # CO
    "co_cpi": "inflation", "co_rate": "neutral", "co_unrate": "unemployment",
    # MX
    "mx_cpi": "inflation", "mx_rate": "neutral", "mx_unrate": "unemployment",
    # AR
    "ar_cpi": "inflation", "ar_rate": "neutral", "ar_unrate": "unemployment", "ar_emae": "activity", "ar_dolar": "neutral",
    # CL FX
    "cl_usd": "neutral",
    # CO FX
    "co_usd": "neutral",
    # MX FX
    "mx_usd": "neutral",
}

# ─── Palette ──────────────────────────────────────────────────────────────────
BG_COLOR   = "#FFFFFF"
PAPER_BG   = "#F5F5F5"
GRID_COLOR = "#EEEEEE"
TEXT_COLOR = "#27251F"
LINE_LEVEL = "#FFC72C"
LINE_MOM   = "#3498DB"
LINE_YOY   = "#DA291C"
COLOR_POS  = "#2ECC71"
COLOR_NEG  = "#DA291C"

# ─── Helpers ─────────────────────────────────────────────────────────────────

def load_csv(series_id: str, resample_freq: str = None) -> pd.DataFrame | None:
    path = os.path.join(DATA_DIR, f"{series_id}.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, parse_dates=["date"])
    if resample_freq:
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        df = df.set_index("date")[numeric_cols].resample(resample_freq).last().reset_index()
    return df


def fmt_val(v, val_fmt="num"):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "N/A"
    if val_fmt == "pct":
        return f"{v:.2f}%"
    return f"{v:,.2f}"


def fmt_pct(v, direction="activity"):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "", "—", "#BBBBBB"
    arrow = "▲" if v >= 0 else "▼"
    if direction == "neutral":
        color = "#AAAAAA"
    elif direction in ("inflation", "unemployment"):
        color = COLOR_NEG if v >= 0 else COLOR_POS
    else:  # activity
        color = COLOR_POS if v >= 0 else COLOR_NEG
    return arrow, f"{v:+.2f}%", color


def build_chart(ind: dict) -> go.Figure:
    resample = ind.get("resample")
    df = load_csv(ind["id"], resample_freq=resample)
    if df is None or df.empty:
        fig = go.Figure()
        fig.add_annotation(text="Sin datos", x=0.5, y=0.5,
                           xref="paper", yref="paper",
                           font=dict(color=COLOR_NEG, size=18), showarrow=False)
        return fig

    # ── CPI indicators: show only YoY% and MoM%, no level ──
    if ind["id"] in CPI_INDICATOR_IDS:
        df = _compute_cpi_columns(df, ind["id"])

        fig = go.Figure()
        if "yoy_pct" in df.columns:
            yoy_s = df[["date", "yoy_pct"]].dropna()
            fig.add_trace(go.Scatter(
                x=yoy_s["date"], y=yoy_s["yoy_pct"], name="YoY %",
                line=dict(color=LINE_YOY, width=1.8),
                hovertemplate="<b>%{x|%b %Y}</b><br>YoY: %{y:+.4f}%<extra></extra>",
            ))
        if "mom_pct" in df.columns:
            mom_s = df[["date", "mom_pct"]].dropna()
            fig.add_trace(go.Scatter(
                x=mom_s["date"], y=mom_s["mom_pct"], name="MoM %",
                line=dict(color=LINE_MOM, width=1.5, dash="dot"),
                hovertemplate="<b>%{x|%b %Y}</b><br>MoM: %{y:+.4f}%<extra></extra>",
            ))
        if fig.data:
            fig.add_hline(y=0, line_color="#CCCCCC", line_width=1)
        _ax = dict(showgrid=True, gridcolor=GRID_COLOR, gridwidth=1,
                   zeroline=False, tickfont=dict(color="#888888", size=10),
                   linecolor="#E0E0E0")
        fig.update_xaxes(**_ax)
        fig.update_yaxes(**_ax, tickformat=".4f", ticksuffix="%")
        fig.update_layout(
            paper_bgcolor=PAPER_BG, plot_bgcolor=BG_COLOR,
            font=dict(family="Inter, Arial, sans-serif", color=TEXT_COLOR, size=11),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                        bgcolor="rgba(245,245,245,0.8)", font=dict(size=10, color="#555555")),
            margin=dict(l=50, r=20, t=30, b=40),
            hovermode="x unified", dragmode="zoom",
        )
        return fig

    has_mom = ind["mom"] and "mom_pct" in df.columns
    has_yoy = ind["yoy"] and "yoy_pct" in df.columns
    vfmt = ind.get("val_fmt", "num")

    if has_mom or has_yoy:
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            row_heights=[0.55, 0.45], vertical_spacing=0.06)
        row_level, row_pct = 1, 2
    else:
        fig = go.Figure()
        row_level = row_pct = None

    hover_tmpl = (
        f"<b>%{{x|%b %Y}}</b><br>{ind['name']}: %{{y:.2f}}%<extra></extra>"
        if vfmt == "pct"
        else f"<b>%{{x|%b %Y}}</b><br>{ind['name']}: %{{y:,.2f}}<extra></extra>"
    )
    axis_tick_fmt = ".2f" if vfmt == "pct" else ",.2f"
    axis_tick_sfx = "%" if vfmt == "pct" else ""

    trace_kwargs = dict(x=df["date"], y=df["value"], name=ind["name"],
                        line=dict(color=LINE_LEVEL, width=1.8),
                        hovertemplate=hover_tmpl, showlegend=True)
    if row_level:
        fig.add_trace(go.Scatter(**trace_kwargs), row=1, col=1)
    else:
        fig.add_trace(go.Scatter(**trace_kwargs))

    if has_mom:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["mom_pct"], name="MoM %",
            line=dict(color=LINE_MOM, width=1.5, dash="dot"),
            hovertemplate="<b>%{x|%b %Y}</b><br>MoM: %{y:+.2f}%<extra></extra>",
        ), row=row_pct, col=1)
    if has_yoy:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["yoy_pct"], name="YoY %",
            line=dict(color=LINE_YOY, width=1.5),
            hovertemplate="<b>%{x|%b %Y}</b><br>YoY: %{y:+.2f}%<extra></extra>",
        ), row=row_pct, col=1)
    if has_mom or has_yoy:
        fig.add_hline(y=0, line_color="#CCCCCC", line_width=1, row=row_pct, col=1)

    axis_style = dict(showgrid=True, gridcolor=GRID_COLOR, gridwidth=1,
                      zeroline=False, tickfont=dict(color="#888888", size=10),
                      linecolor="#E0E0E0")
    yaxis_style = dict(**axis_style, tickformat=axis_tick_fmt, ticksuffix=axis_tick_sfx)
    pct_axis   = dict(**axis_style, tickformat=".2f", ticksuffix="%")
    fig.update_xaxes(**axis_style)
    fig.update_yaxes(**yaxis_style)
    if has_mom or has_yoy:
        fig.update_yaxes(row=2, col=1, **pct_axis)

    fig.update_layout(
        paper_bgcolor=PAPER_BG, plot_bgcolor=BG_COLOR,
        font=dict(family="Inter, Arial, sans-serif", color=TEXT_COLOR, size=11),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    bgcolor="rgba(245,245,245,0.8)", font=dict(size=10, color="#555555")),
        margin=dict(l=50, r=20, t=30, b=40),
        hovermode="x unified", dragmode="zoom",
    )
    return fig


def build_summary_table(summary_df: pd.DataFrame, prev_values: dict) -> str:
    from datetime import date as _date
    fmt_map = {i["id"]: i["val_fmt"] for i in US_INDICATORS + BR_INDICATORS + CL_INDICATORS + CO_INDICATORS + MX_INDICATORS + AR_INDICATORS}
    today = datetime.today().date()
    rows_html = ""
    for _, row in summary_df.iterrows():
        sid       = row["series_id"]
        name      = row.get("name", sid)
        last_val  = row.get("last_value")
        last_date = row.get("last_date", "")
        mom       = row.get("mom_pct")
        yoy       = row.get("yoy_pct")
        vfmt      = fmt_map.get(sid, "num")
        direc     = COLOR_DIRECTION.get(sid, "activity")

        val_str  = fmt_val(last_val, vfmt)
        prev_val = prev_values.get(sid)
        prev_str = fmt_val(prev_val, vfmt) if prev_val is not None else '<span style="color:#BBBBBB">—</span>'

        mom_val = mom if (mom is not None and pd.notna(mom)) else None
        yoy_val = yoy if (yoy is not None and pd.notna(yoy)) else None

        arrow_m, pct_m, col_m = fmt_pct(mom_val, direc)
        mom_cell = f'<span style="color:{col_m}">{arrow_m} {pct_m}</span>'
        arrow_y, pct_y, col_y = fmt_pct(yoy_val, direc)
        yoy_cell = f'<span style="color:{col_y}">{arrow_y} {pct_y}</span>'

        # Rezago
        try:
            last_dt = pd.to_datetime(last_date).date() if last_date else None
            lag_days = (today - last_dt).days if last_dt else None
        except Exception:
            lag_days = None
        if lag_days is not None:
            if lag_days < 45:
                lag_color = "#2ECC71"
            elif lag_days < 90:
                lag_color = "#FFC72C"
            else:
                lag_color = "#DA291C"
            lag_cell = f'<span style="color:{lag_color};font-family:monospace">{lag_days}d</span>'
        else:
            lag_cell = '<span style="color:#BBBBBB">—</span>'

        rows_html += f"""
        <tr>
            <td class="td-name">{name}</td>
            <td class="td-id">{sid}</td>
            <td class="td-val">{val_str}</td>
            <td class="td-prev">{prev_str}</td>
            <td class="td-chg">{mom_cell}</td>
            <td class="td-chg">{yoy_cell}</td>
            <td class="td-date">{last_date}</td>
            <td class="td-chg">{lag_cell}</td>
        </tr>"""
    return rows_html


def _compute_cpi_columns(df: pd.DataFrame, series_id: str) -> pd.DataFrame:
    """Ensure yoy_pct and mom_pct exist for CPI series.

    Three cases:
    - CPI_VALUE_IS_YOY  (mx/cl/co_cpi):      value IS YoY%; mom_pct already in CSV
    - CPI_VALUE_IS_MOM  (br_ipca/core):       value IS MoM%; derive YoY via cumulative product
    - others (CPILFESL/PCEPILFE/PPIACO/ar):   value is index level; yoy_pct already in CSV
    """
    df = df.copy()
    if series_id in CPI_VALUE_IS_YOY:
        if "yoy_pct" not in df.columns:
            df["yoy_pct"] = df["value"]
    elif series_id in CPI_VALUE_IS_MOM:
        if "mom_pct" not in df.columns:
            df["mom_pct"] = df["value"]
        if "yoy_pct" not in df.columns:
            cum_idx = (1 + df["mom_pct"] / 100).cumprod()
            df["yoy_pct"] = (cum_idx / cum_idx.shift(12) - 1) * 100
    # else: index-level series already have yoy_pct from fetch_data
    return df


def build_country_data(indicators):
    """Genera charts_json, all_dates y all_data para un conjunto de indicadores."""
    charts_json = {}
    all_dates   = {}
    all_data    = {}
    for ind in indicators:
        fig = build_chart(ind)
        charts_json[ind["id"]] = json.loads(pio.to_json(fig))
        resample = ind.get("resample")
        df = load_csv(ind["id"], resample_freq=resample)
        if df is not None and not df.empty:
            if ind["id"] in CPI_INDICATOR_IDS:
                df = _compute_cpi_columns(df, ind["id"])
            all_dates[ind["id"]] = df["date"].dt.strftime("%Y-%m-%d").tolist()
            all_data[ind["id"]]  = {
                "value":   df["value"].tolist(),
                "mom_pct": df["mom_pct"].tolist() if "mom_pct" in df.columns else [],
                "yoy_pct": df["yoy_pct"].tolist() if "yoy_pct" in df.columns else [],
            }
    return charts_json, all_dates, all_data


def get_prev_values(indicators):
    """Retorna dict {series_id: penúltimo valor} para llenar la columna Prev. en la tabla."""
    prev = {}
    for ind in indicators:
        resample = ind.get("resample")
        df = load_csv(ind["id"], resample_freq=resample)
        if df is not None and len(df) >= 2:
            prev[ind["id"]] = df.iloc[-2]["value"]
    return prev


def build_chart_sections(indicators):
    html = ""
    for ind in indicators:
        html += f"""
        <div class="chart-card" id="card-{ind['id']}">
            <div class="chart-header">
                <div>
                    <span class="chart-title">{ind['name']}</span>
                    <span class="chart-sub">{ind['subtitle']}</span>
                    <span class="freshness-badge" id="fresh-{ind['id']}"></span>
                </div>
                <div style="display:flex;align-items:center;gap:8px">
                    <span class="chart-unit">{ind['unit']}</span>
                    <button class="btn-dl-png" onclick="downloadChartPNG('{ind['id']}')" title="Descargar PNG">&#11015;</button>
                </div>
            </div>
            <div id="chart-{ind['id']}" class="chart-div"></div>
        </div>"""
    return html


# ─── Main generator ───────────────────────────────────────────────────────────

def generate_dashboard():
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M UTC")

    # ── US ────────────────────────────────────────────────────────────────────
    us_summary_df = pd.read_csv(os.path.join(DATA_DIR, "summary.csv")) if os.path.exists(os.path.join(DATA_DIR, "summary.csv")) else pd.DataFrame()
    us_prev = {}
    for ind in US_INDICATORS:
        df = load_csv(ind["id"])
        if df is not None and len(df) >= 2:
            us_prev[ind["id"]] = df.iloc[-2]["value"]

    us_charts_json, us_all_dates, us_all_data = build_country_data(US_INDICATORS)
    us_summary_rows    = build_summary_table(us_summary_df, us_prev) if not us_summary_df.empty else ""
    us_charts_sections = build_chart_sections(US_INDICATORS)

    # ── BR ────────────────────────────────────────────────────────────────────
    br_summary_df = pd.read_csv(os.path.join(DATA_DIR, "br_summary.csv")) if os.path.exists(os.path.join(DATA_DIR, "br_summary.csv")) else pd.DataFrame()
    br_prev = {}
    for ind in BR_INDICATORS:
        resample = ind.get("resample")
        df = load_csv(ind["id"], resample_freq=resample)
        if df is not None and len(df) >= 2:
            br_prev[ind["id"]] = df.iloc[-2]["value"]

    br_charts_json, br_all_dates, br_all_data = build_country_data(BR_INDICATORS)
    br_summary_rows    = build_summary_table(br_summary_df, br_prev) if not br_summary_df.empty else ""
    br_charts_sections = build_chart_sections(BR_INDICATORS)

    # ── CL ────────────────────────────────────────────────────────────────────
    cl_summary_df = pd.read_csv(os.path.join(DATA_DIR, "cl_summary.csv")) if os.path.exists(os.path.join(DATA_DIR, "cl_summary.csv")) else pd.DataFrame()
    cl_prev = get_prev_values(CL_INDICATORS)
    cl_charts_json, cl_all_dates, cl_all_data = build_country_data(CL_INDICATORS)
    cl_summary_rows    = build_summary_table(cl_summary_df, cl_prev) if not cl_summary_df.empty else ""
    cl_charts_sections = build_chart_sections(CL_INDICATORS)

    # ── CO ────────────────────────────────────────────────────────────────────
    co_summary_df = pd.read_csv(os.path.join(DATA_DIR, "co_summary.csv")) if os.path.exists(os.path.join(DATA_DIR, "co_summary.csv")) else pd.DataFrame()
    co_prev = get_prev_values(CO_INDICATORS)
    co_charts_json, co_all_dates, co_all_data = build_country_data(CO_INDICATORS)
    co_summary_rows    = build_summary_table(co_summary_df, co_prev) if not co_summary_df.empty else ""
    co_charts_sections = build_chart_sections(CO_INDICATORS)

    # ── MX ────────────────────────────────────────────────────────────────────
    mx_summary_df = pd.read_csv(os.path.join(DATA_DIR, "mx_summary.csv")) if os.path.exists(os.path.join(DATA_DIR, "mx_summary.csv")) else pd.DataFrame()
    mx_prev = get_prev_values(MX_INDICATORS)
    mx_charts_json, mx_all_dates, mx_all_data = build_country_data(MX_INDICATORS)
    mx_summary_rows    = build_summary_table(mx_summary_df, mx_prev) if not mx_summary_df.empty else ""
    mx_charts_sections = build_chart_sections(MX_INDICATORS)

    # ── AR ────────────────────────────────────────────────────────────────────
    ar_summary_df = pd.read_csv(os.path.join(DATA_DIR, "ar_summary.csv")) if os.path.exists(os.path.join(DATA_DIR, "ar_summary.csv")) else pd.DataFrame()
    ar_prev = get_prev_values(AR_INDICATORS)
    ar_charts_json, ar_all_dates, ar_all_data = build_country_data(AR_INDICATORS)
    ar_summary_rows    = build_summary_table(ar_summary_df, ar_prev) if not ar_summary_df.empty else ""
    ar_charts_sections = build_chart_sections(AR_INDICATORS)

    # BRL/USD para el header
    usdbrl_df  = load_csv("br_usdbrl")
    usdbrl_val = f"{usdbrl_df.iloc[-1]['value']:.4f}" if usdbrl_df is not None and not usdbrl_df.empty else "N/A"

    # JS metadata
    def _ind_js(i):
        return {"id": i["id"], "name": i["name"], "mom": i["mom"], "yoy": i["yoy"], "val_fmt": i["val_fmt"], "source": i.get("source", "")}
    us_inds_json = json.dumps([_ind_js(i) for i in US_INDICATORS])
    br_inds_json = json.dumps([_ind_js(i) for i in BR_INDICATORS])
    cl_inds_json = json.dumps([_ind_js(i) for i in CL_INDICATORS])
    co_inds_json = json.dumps([_ind_js(i) for i in CO_INDICATORS])
    mx_inds_json = json.dumps([_ind_js(i) for i in MX_INDICATORS])
    ar_inds_json = json.dumps([_ind_js(i) for i in AR_INDICATORS])
    cpi_ids_json = json.dumps(sorted(CPI_INDICATOR_IDS))

    # Combinar datos para JS
    all_charts_js = json.dumps({"us": us_charts_json, "br": br_charts_json, "cl": cl_charts_json, "co": co_charts_json, "mx": mx_charts_json, "ar": ar_charts_json})
    all_dates_js  = json.dumps({"us": us_all_dates,   "br": br_all_dates,   "cl": cl_all_dates,   "co": co_all_dates,   "mx": mx_all_dates,   "ar": ar_all_dates})
    all_data_js   = json.dumps({"us": us_all_data,    "br": br_all_data,    "cl": cl_all_data,    "co": co_all_data,    "mx": mx_all_data,    "ar": ar_all_data})

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Macro Terminal — US, Brasil &amp; LATAM</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: #FFFFFF; color: #27251F;
    font-family: Inter, 'Helvetica Neue', Arial, sans-serif; font-size: 13px;
  }}

  /* ── Header ── */
  .header {{
    background: #27251F; border-bottom: 3px solid #FFC72C;
    padding: 14px 28px; display: flex; align-items: center;
    justify-content: space-between; position: sticky; top: 0; z-index: 100;
  }}
  .header-left  {{ display: flex; align-items: center; gap: 20px; }}
  .header-logo  {{ font-size: 17px; font-weight: 700; letter-spacing: 2px; color: #FFC72C; }}
  .header-title {{ color: #A89060; font-size: 11px; letter-spacing: 1.5px; font-weight: 400; }}
  .header-right {{ display: flex; align-items: center; gap: 16px; }}
  .header-updated {{ color: #7A6A50; font-size: 10px; }}
  .brl-usd-badge {{
    background: #3A3530; border: 1px solid #FFC72C; border-radius: 3px;
    padding: 3px 10px; font-size: 11px; color: #FFC72C; font-family: 'Courier New', monospace;
    font-weight: 700; letter-spacing: 0.5px;
  }}

  /* ── Country Selector ── */
  .country-toggle {{ display: flex; gap: 6px; }}
  .btn-country {{
    background: transparent; border: 1px solid #555555; color: #888888;
    padding: 5px 16px; cursor: pointer; font-family: inherit;
    font-size: 12px; font-weight: 700; border-radius: 3px;
    transition: all 0.15s; letter-spacing: 0.3px;
  }}
  .btn-country:hover {{ border-color: #FFC72C; color: #FFC72C; }}
  .btn-country.active {{ background: #FFC72C; border-color: #FFC72C; color: #27251F; }}
  .controls-sep {{ color: #CCCCCC; font-size: 16px; padding: 0 4px; }}

  /* ── Date Range Controls ── */
  .controls {{
    background: #F5F5F5; border-bottom: 1px solid #E0E0E0;
    padding: 10px 28px; display: flex; align-items: center;
    gap: 10px; flex-wrap: wrap;
  }}
  .controls label {{ color: #555555; font-size: 11px; font-weight: 600; letter-spacing: 0.5px; }}
  .btn-range {{
    background: #FFFFFF; border: 1px solid #E0E0E0; color: #555555;
    padding: 4px 14px; cursor: pointer; font-family: inherit;
    font-size: 11px; font-weight: 600; border-radius: 3px; transition: all 0.15s;
  }}
  .btn-range:hover  {{ border-color: #FFC72C; color: #27251F; background: #FFFBEE; }}
  .btn-range.active {{ background: #FFC72C; border-color: #FFC72C; color: #27251F; }}
  .date-input {{
    background: #FFFFFF; border: 1px solid #E0E0E0; color: #27251F;
    padding: 4px 8px; font-family: inherit; font-size: 11px;
    border-radius: 3px; width: 120px;
  }}
  .date-input:focus {{ outline: none; border-color: #FFC72C; }}

  /* ── Executive Summary ── */
  .exec-summary {{
    margin: 16px 28px 0; background: #F5F5F5;
    border-left: 4px solid #FFC72C; border-radius: 0 4px 4px 0;
    padding: 14px 18px; display: grid;
    grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 10px 24px;
  }}
  .exec-block  {{ display: flex; flex-direction: column; gap: 3px; }}
  .exec-label  {{ font-size: 9px; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: #888888; }}
  .exec-text   {{ font-size: 12px; color: #27251F; line-height: 1.5; }}
  .exec-badge  {{ display: inline-block; font-size: 10px; font-weight: 700; padding: 1px 7px; border-radius: 3px; margin-right: 4px; vertical-align: middle; }}
  .badge-ok    {{ background: #D4EDDA; color: #1A7A3C; }}
  .badge-warn  {{ background: #FFF3CD; color: #856404; }}
  .badge-alert {{ background: #F8D7DA; color: #9B1C1C; }}
  .badge-neu   {{ background: #E9ECEF; color: #555555; }}

  /* ── Summary Table ── */
  .summary-section {{ padding: 20px 28px 12px; background: #FFFFFF; }}
  .section-label {{
    color: #27251F; font-size: 10px; font-weight: 700; letter-spacing: 2px;
    text-transform: uppercase; margin-bottom: 12px;
    border-left: 3px solid #FFC72C; padding-left: 10px;
  }}
  table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
  thead th {{
    background: #F5F5F5; color: #555555; padding: 7px 10px; text-align: left;
    font-weight: 600; font-size: 10px; letter-spacing: 1px; text-transform: uppercase;
    border-bottom: 2px solid #E0E0E0;
  }}
  tbody tr {{ border-bottom: 1px solid #F0F0F0; transition: background 0.1s; }}
  tbody tr:hover {{ background: #FFFBEE; }}
  .td-name  {{ color: #27251F; font-weight: 600; white-space: nowrap; padding: 5px 10px; }}
  .td-id    {{ color: #AAAAAA; font-size: 10px; font-family: 'Courier New', monospace; padding: 5px 10px; }}
  .td-val   {{ color: #27251F; font-weight: 700; font-family: 'Courier New', monospace; text-align: right; padding: 5px 10px; }}
  .td-prev  {{ color: #888888; font-family: 'Courier New', monospace; font-size: 11px; text-align: right; padding: 5px 10px; }}
  .td-chg   {{ text-align: right; font-family: 'Courier New', monospace; font-size: 11px; white-space: nowrap; padding: 5px 10px; }}
  .td-date  {{ color: #AAAAAA; font-size: 10px; white-space: nowrap; padding: 5px 10px; }}

  /* ── Charts Grid ── */
  .charts-section {{ padding: 16px 28px 32px; background: #FFFFFF; }}
  .charts-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(580px, 1fr)); gap: 16px; }}
  .chart-card {{
    background: #F5F5F5; border: 1px solid #E0E0E0; border-radius: 6px;
    overflow: hidden; transition: box-shadow 0.2s, border-color 0.2s;
  }}
  .chart-card:hover {{ border-color: #FFC72C; box-shadow: 0 2px 12px rgba(255,199,44,0.15); }}
  .chart-header {{
    display: flex; justify-content: space-between; align-items: flex-start;
    padding: 12px 16px 8px; background: #FFFFFF; border-bottom: 1px solid #E0E0E0;
  }}
  .chart-title {{ color: #27251F; font-size: 13px; font-weight: 700; }}
  .chart-sub   {{ display: block; color: #888888; font-size: 10px; margin-top: 3px; font-weight: 400; }}
  .chart-unit  {{ color: #888888; font-size: 10px; background: #F0F0F0; padding: 3px 8px; border: 1px solid #E0E0E0; border-radius: 3px; white-space: nowrap; font-weight: 500; }}
  .chart-div   {{ height: 280px; }}
  .freshness-badge {{ font-size: 10px; display: block; margin-top: 2px; font-family: 'Courier New', monospace; }}
  .btn-dl-png {{ background:transparent; border:1px solid #E0E0E0; color:#888888; padding:2px 7px; cursor:pointer; font-size:11px; border-radius:3px; transition:all 0.15s; }}
  .btn-dl-png:hover {{ border-color:#FFC72C; color:#FFC72C; }}
  .modal-overlay {{ display:none; position:fixed; inset:0; background:rgba(0,0,0,0.6); z-index:1000; align-items:center; justify-content:center; }}
  .modal-overlay.open {{ display:flex; }}
  .modal-box {{ background:#FFFFFF; border:2px solid #FFC72C; border-radius:6px; padding:24px; min-width:700px; max-width:90vw; }}
  .modal-title {{ font-size:14px; font-weight:700; color:#27251F; margin-bottom:16px; }}
  .modal-controls {{ display:flex; gap:16px; margin-bottom:16px; align-items:flex-end; flex-wrap:wrap; }}
  .modal-select {{ background:#F5F5F5; border:1px solid #E0E0E0; color:#27251F; padding:6px 10px; font-family:inherit; font-size:12px; border-radius:3px; }}
  .modal-chart {{ height:320px; }}
  .btn-close {{ background:transparent; border:1px solid #DA291C; color:#DA291C; padding:4px 12px; cursor:pointer; font-family:inherit; font-size:11px; border-radius:3px; }}

  /* ── Ticker bar ── */
  .ticker {{
    background: #27251F; border-bottom: 1px solid #3A3530;
    padding: 7px 28px; font-size: 11px;
    display: flex; gap: 28px; flex-wrap: wrap; align-items: center;
  }}
  .ticker-item  {{ display: flex; gap: 8px; align-items: center; }}
  .ticker-label {{ color: #7A6A50; font-weight: 600; font-size: 10px; letter-spacing: 0.5px; }}
  .ticker-val   {{ color: #F5F0E8; font-family: 'Courier New', monospace; font-weight: 700; }}
  .ticker-pos   {{ color: #2ECC71; font-size: 10px; }}
  .ticker-neg   {{ color: #DA291C; font-size: 10px; }}

  /* ── PDF Export Button ── */
  .btn-pdf {{
    background: transparent; border: 1px solid #FFC72C; color: #FFC72C;
    padding: 5px 14px; cursor: pointer; font-family: inherit;
    font-size: 11px; font-weight: 700; border-radius: 3px;
    letter-spacing: 0.5px; transition: all 0.15s;
  }}
  .btn-pdf:hover {{ background: #FFC72C; color: #27251F; }}

  /* ── Upcoming Events ── */
  .events-section {{ padding: 16px 28px 32px; background: #FFFFFF; }}
  .events-table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
  .events-table thead th {{
    background: #F5F5F5; color: #555555; padding: 7px 12px; text-align: left;
    font-weight: 600; font-size: 10px; letter-spacing: 1px; text-transform: uppercase;
    border-bottom: 2px solid #E0E0E0;
  }}
  .events-table tbody tr {{ border-bottom: 1px solid #F0F0F0; transition: background 0.1s; }}
  .events-table tbody tr:hover {{ background: #FFFBEE; }}
  .events-table tbody td {{ padding: 6px 12px; }}
  .ev-date {{ color: #27251F; font-weight: 700; white-space: nowrap; font-size: 11px; }}
  .ev-name {{ color: #27251F; font-weight: 600; }}
  .ev-period {{ color: #888888; font-size: 11px; }}
  .ev-consensus {{ color: #27251F; font-family: 'Courier New', monospace; font-size: 11px; }}
  .ev-importance-high {{ display: inline-block; width: 6px; height: 6px; border-radius: 50%; background: #DA291C; margin-right: 6px; vertical-align: middle; }}
  .ev-importance-med  {{ display: inline-block; width: 6px; height: 6px; border-radius: 50%; background: #FFC72C; margin-right: 6px; vertical-align: middle; }}
  .ev-importance-low  {{ display: inline-block; width: 6px; height: 6px; border-radius: 50%; background: #CCCCCC; margin-right: 6px; vertical-align: middle; }}
  .ev-fomc  {{ background: #FFFBEE !important; }}
  .ev-today {{ background: #F0F7FF !important; }}
  .ev-week-header td {{ background: #F5F5F5; color: #888888; font-size: 10px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; padding: 5px 12px; border-bottom: 1px solid #E0E0E0; }}

  /* ── Print footer ── */
  .print-footer {{ display: none; text-align: center; font-size: 10px; color: #888888; padding: 12px 0 4px; border-top: 1px solid #E0E0E0; margin-top: 20px; }}

  /* ── @media print ── */
  @media print {{
    @page {{ size: A4 landscape; margin: 12mm 10mm; }}
    body {{ font-size: 11px; }}
    .header {{ position: static; }}
    .btn-pdf, .controls, .ticker {{ display: none !important; }}
    .chart-card {{ break-inside: avoid; page-break-inside: avoid; }}
    .exec-summary {{ break-inside: avoid; page-break-inside: avoid; }}
    .charts-grid {{ grid-template-columns: repeat(2, 1fr); gap: 10px; }}
    .chart-div {{ height: 220px !important; }}
    .print-footer {{ display: block; }}
    .chart-card {{ background: #FFFFFF !important; border: 1px solid #CCCCCC !important; }}
  }}
</style>
</head>
<body>

<!-- HEADER -->
<div class="header">
  <div class="header-left">
    <div class="header-logo">MACRO TERMINAL</div>
    <div class="header-title">GLOBAL ECONOMIC INDICATORS — FRED · BCB · INDEC · BCRA</div>
  </div>
  <div class="header-right">
    <div id="brl-usd-display" style="display:none">
      <span class="brl-usd-badge">BRL/USD <span id="brl-usd-val">{usdbrl_val}</span></span>
    </div>
    <div class="header-updated">Updated: {updated_at}</div>
    <span id="btn-refresh" style="margin-right:8px; font-size:0.8rem; color:#8899aa;">&#128260; Última actualización: {updated_at}</span>
    <button class="btn-pdf" onclick="exportPDF()">&#11015; Export PDF</button>
  </div>
</div>

<!-- TICKER BAR -->
<div class="ticker" id="ticker-bar">Loading...</div>

<!-- CONTROLS -->
<div class="controls">
  <div class="country-toggle">
    <button class="btn-country active" id="btn-us" onclick="setCountry('us')">&#127482;&#127480; United States</button>
    <button class="btn-country"        id="btn-br" onclick="setCountry('br')">&#127463;&#127479; Brasil</button>
    <button class="btn-country"        id="btn-ar" onclick="setCountry('ar')">&#127462;&#127479; Argentina</button>
    <button class="btn-country"        id="btn-cl" onclick="setCountry('cl')">&#127464;&#127473; Chile</button>
    <button class="btn-country"        id="btn-co" onclick="setCountry('co')">&#127464;&#127476; Colombia</button>
    <button class="btn-country"        id="btn-mx" onclick="setCountry('mx')">&#127474;&#127485; M&#233;xico</button>
  </div>
  <button class="btn-pdf" onclick="openComparator()" style="margin-left:8px">&#9878; Comparar</button>
  <span class="controls-sep">|</span>
  <label>RANGE:</label>
  <button class="btn-range" onclick="setRange(6)">6M</button>
  <button class="btn-range active" onclick="setRange(12)">1Y</button>
  <button class="btn-range" onclick="setRange(24)">2Y</button>
  <button class="btn-range" onclick="setRange(36)">3Y</button>
  <button class="btn-range" onclick="setRange(0)">ALL</button>
  <label style="margin-left:16px">FROM:</label>
  <input type="date" id="date-from" class="date-input" onchange="applyCustomRange()">
  <label>TO:</label>
  <input type="date" id="date-to" class="date-input" onchange="applyCustomRange()">
</div>

<!-- EXECUTIVE SUMMARY -->
<div class="exec-summary" id="exec-summary">
  <div class="exec-block">
    <div class="exec-label">Inflation</div>
    <div class="exec-text" id="exec-inflation">Loading...</div>
  </div>
  <div class="exec-block">
    <div class="exec-label">Labor Market</div>
    <div class="exec-text" id="exec-labor">Loading...</div>
  </div>
  <div class="exec-block">
    <div class="exec-label">Economic Activity</div>
    <div class="exec-text" id="exec-activity">Loading...</div>
  </div>
  <div class="exec-block">
    <div class="exec-label">Monetary Policy</div>
    <div class="exec-text" id="exec-policy">Loading...</div>
  </div>
</div>

<!-- SEMÁFORO DE COYUNTURA -->
<div id="semaforo" style="padding:6px 28px; background:#F5F5F5; border-bottom:1px solid #E0E0E0; font-size:11px; display:flex; gap:16px; flex-wrap:wrap;"></div>

<!-- SUMMARY TABLE US -->
<div class="summary-section" id="summary-section-us">
  <div class="section-label">Last Values — United States</div>
  <table>
    <thead>
      <tr>
        <th>Indicator</th><th>Series ID</th>
        <th style="text-align:right">Last Value</th>
        <th style="text-align:right">Prev. Value</th>
        <th style="text-align:right">MoM</th>
        <th style="text-align:right">YoY</th>
        <th>As Of</th>
        <th style="text-align:right">Rezago</th>
      </tr>
    </thead>
    <tbody>{us_summary_rows}</tbody>
  </table>
</div>

<!-- SUMMARY TABLE BR -->
<div class="summary-section" id="summary-section-br" style="display:none">
  <div class="section-label">&#218;ltimos Valores — Brasil</div>
  <table>
    <thead>
      <tr>
        <th>Indicador</th><th>S&#233;rie BCB</th>
        <th style="text-align:right">&#218;ltimo Valor</th>
        <th style="text-align:right">Valor Anterior</th>
        <th style="text-align:right">MoM</th>
        <th style="text-align:right">YoY</th>
        <th>Data</th>
        <th style="text-align:right">Rezago</th>
      </tr>
    </thead>
    <tbody>{br_summary_rows}</tbody>
  </table>
</div>

<!-- SUMMARY TABLE AR -->
<div class="summary-section" id="summary-section-ar" style="display:none">
  <div class="section-label">&#218;ltimos Valores — Argentina (INDEC API + BCRA)</div>
  <table>
    <thead>
      <tr>
        <th>Indicador</th><th>Serie</th>
        <th style="text-align:right">&#218;ltimo Valor</th>
        <th style="text-align:right">Valor Anterior</th>
        <th style="text-align:right">MoM</th>
        <th style="text-align:right">YoY</th>
        <th>Fecha</th>
        <th style="text-align:right">Rezago</th>
      </tr>
    </thead>
    <tbody>{ar_summary_rows}</tbody>
  </table>
</div>

<!-- SUMMARY TABLE CL -->
<div class="summary-section" id="summary-section-cl" style="display:none">
  <div class="section-label">&#218;ltimos Valores — Chile</div>
  <table>
    <thead>
      <tr>
        <th>Indicador</th><th>Serie FRED</th>
        <th style="text-align:right">&#218;ltimo Valor</th>
        <th style="text-align:right">Valor Anterior</th>
        <th style="text-align:right">MoM</th>
        <th style="text-align:right">YoY</th>
        <th>Fecha</th>
        <th style="text-align:right">Rezago</th>
      </tr>
    </thead>
    <tbody>{cl_summary_rows}</tbody>
  </table>
</div>

<!-- SUMMARY TABLE CO -->
<div class="summary-section" id="summary-section-co" style="display:none">
  <div class="section-label">&#218;ltimos Valores — Colombia</div>
  <table>
    <thead>
      <tr>
        <th>Indicador</th><th>Serie</th>
        <th style="text-align:right">&#218;ltimo Valor</th>
        <th style="text-align:right">Valor Anterior</th>
        <th style="text-align:right">MoM</th>
        <th style="text-align:right">YoY</th>
        <th>Fecha</th>
        <th style="text-align:right">Rezago</th>
      </tr>
    </thead>
    <tbody>{co_summary_rows}</tbody>
  </table>
</div>

<!-- SUMMARY TABLE MX -->
<div class="summary-section" id="summary-section-mx" style="display:none">
  <div class="section-label">&#218;ltimos Valores — M&#233;xico</div>
  <table>
    <thead>
      <tr>
        <th>Indicador</th><th>Serie FRED</th>
        <th style="text-align:right">&#218;ltimo Valor</th>
        <th style="text-align:right">Valor Anterior</th>
        <th style="text-align:right">MoM</th>
        <th style="text-align:right">YoY</th>
        <th>Fecha</th>
        <th style="text-align:right">Rezago</th>
      </tr>
    </thead>
    <tbody>{mx_summary_rows}</tbody>
  </table>
</div>

<!-- CHARTS US -->
<div class="charts-section" id="charts-section-us">
  <div class="section-label" style="margin-bottom:12px">Interactive Charts — United States</div>
  <div class="charts-grid">
{us_charts_sections}
  </div>
</div>

<!-- CHARTS BR -->
<div class="charts-section" id="charts-section-br" style="display:none">
  <div class="section-label" style="margin-bottom:12px">Gr&#225;ficos Interativos — Brasil</div>
  <div class="charts-grid">
{br_charts_sections}
  </div>
</div>

<!-- CHARTS AR -->
<div class="charts-section" id="charts-section-ar" style="display:none">
  <div class="section-label" style="margin-bottom:12px">Gr&#225;ficos Interactivos — Argentina (INDEC API + BCRA)</div>
  <div class="charts-grid">
{ar_charts_sections}
  </div>
</div>

<!-- CHARTS CL -->
<div class="charts-section" id="charts-section-cl" style="display:none">
  <div class="section-label" style="margin-bottom:12px">Gr&#225;ficos Interactivos — Chile</div>
  <div class="charts-grid">
{cl_charts_sections}
  </div>
</div>

<!-- CHARTS CO -->
<div class="charts-section" id="charts-section-co" style="display:none">
  <div class="section-label" style="margin-bottom:12px">Gr&#225;ficos Interactivos — Colombia</div>
  <div class="charts-grid">
{co_charts_sections}
  </div>
</div>

<!-- CHARTS MX -->
<div class="charts-section" id="charts-section-mx" style="display:none">
  <div class="section-label" style="margin-bottom:12px">Gr&#225;ficos Interactivos — M&#233;xico</div>
  <div class="charts-grid">
{mx_charts_sections}
  </div>
</div>

<!-- UPCOMING EVENTS (US only) -->
<div class="events-section" id="events-section">
  <div class="section-label">Upcoming Economic Releases — Next 2 Weeks</div>
  <table class="events-table">
    <thead>
      <tr>
        <th style="width:110px">Date</th>
        <th>Indicator</th><th>Period</th><th>Consensus</th><th>Previous</th>
      </tr>
    </thead>
    <tbody id="events-tbody"></tbody>
  </table>
  <div style="margin-top:8px; font-size:10px; color:#AAAAAA;">
    <span class="ev-importance-high"></span>High &nbsp;
    <span class="ev-importance-med"></span>Medium &nbsp;
    <span class="ev-importance-low"></span>Low &nbsp;&nbsp;
    Updated: {updated_at} — update manually each week.
  </div>
</div>

<!-- COMPARATOR MODAL -->
<div class="modal-overlay" id="comparator-modal" onclick="if(event.target===this)closeComparator()">
  <div class="modal-box">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <div class="modal-title">&#9878; Comparador de Pa&#237;ses</div>
      <button class="btn-close" onclick="closeComparator()">&#10005; Cerrar</button>
    </div>
    <div class="modal-controls">
      <div>
        <div style="font-size:10px;color:#888;margin-bottom:4px">PA&#205;S 1</div>
        <select id="comp-country1" class="modal-select" onchange="updateComparatorIndicators()"></select>
      </div>
      <div>
        <div style="font-size:10px;color:#888;margin-bottom:4px">PA&#205;S 2</div>
        <select id="comp-country2" class="modal-select" onchange="updateComparatorIndicators()"></select>
      </div>
      <div>
        <div style="font-size:10px;color:#888;margin-bottom:4px">INDICADOR</div>
        <select id="comp-indicator" class="modal-select"></select>
      </div>
      <button class="btn-range active" onclick="renderComparatorChart()">&#9654; Comparar</button>
    </div>
    <div id="comp-chart" class="modal-chart"></div>
    <div style="font-size:10px;color:#AAAAAA;margin-top:8px">Nota: Solo se comparan indicadores con el mismo ID de serie. Usa "Comparar" para actualizar.</div>
  </div>
</div>

<!-- PRINT FOOTER -->
<div class="print-footer" id="print-footer">
  Macro Terminal — Global Economic Dashboard &nbsp;|&nbsp; Generated: <span id="print-ts"></span>
</div>

<script>
// ── Embedded data ─────────────────────────────────────────────────────────────
const ALL_CHARTS = {all_charts_js};
const ALL_DATES  = {all_dates_js};
const ALL_DATA   = {all_data_js};
const ALL_INDS   = {{ us: {us_inds_json}, br: {br_inds_json}, cl: {cl_inds_json}, co: {co_inds_json}, mx: {mx_inds_json}, ar: {ar_inds_json} }};
const CPI_IDS    = new Set({cpi_ids_json});

// ── Reference lines (US only) ─────────────────────────────────────────────────
const REFERENCE_LINES = {{
  "CPILFESL": [{{ y: 2.0, label: "Fed Target 2%", panel: "pct" }}],
  "PCEPILFE": [{{ y: 2.0, label: "Fed Target 2%", panel: "pct" }}],
  "UNRATE":   [{{ y: 4.0, label: "NAIRU ~4%",     panel: "level" }}],
  "FEDFUNDS": [{{ y: 2.5, label: "Neutral Rate",  panel: "level" }}],
}};

// ── State ─────────────────────────────────────────────────────────────────────
let currentCountry = "us";
let currentMonths  = 12;
let dateFrom = null;
let dateTo   = null;

// ── Country switch ────────────────────────────────────────────────────────────
function setCountry(c) {{
  currentCountry = c;
  ["us", "br", "ar", "cl", "co", "mx"].forEach(cc => {{
    const btn = document.getElementById("btn-" + cc);
    if (btn) btn.classList.toggle("active", cc === c);
    const ss = document.getElementById("summary-section-" + cc);
    if (ss) ss.style.display = cc === c ? "" : "none";
    const cs = document.getElementById("charts-section-" + cc);
    if (cs) cs.style.display = cc === c ? "" : "none";
  }});
  document.getElementById("events-section").style.display  = c === "us" ? "" : "none";
  document.getElementById("brl-usd-display").style.display = c === "br" ? "" : "none";
  renderAllCharts();
  buildTicker();
  buildExecSummary();
  buildSemaforo();
  updateFreshnessBadges();
}}

// ── Plotly layout defaults ────────────────────────────────────────────────────
const BASE_LAYOUT = {{
  paper_bgcolor: "#F5F5F5", plot_bgcolor: "#FFFFFF",
  font: {{ family: "Inter, Arial, sans-serif", color: "#27251F", size: 11 }},
  margin: {{ l: 52, r: 20, t: 30, b: 40 }},
  hovermode: "x unified",
  legend: {{ orientation: "h", yanchor: "bottom", y: 1.02, xanchor: "right", x: 1,
             bgcolor: "rgba(245,245,245,0.9)", font: {{ size: 10, color: "#555555" }} }},
  xaxis:  {{ showgrid: true, gridcolor: "#EEEEEE", zeroline: false, tickfont: {{ color: "#888888", size: 10 }}, linecolor: "#E0E0E0" }},
  yaxis:  {{ showgrid: true, gridcolor: "#EEEEEE", zeroline: false, tickfont: {{ color: "#888888", size: 10 }}, linecolor: "#E0E0E0" }},
  xaxis2: {{ showgrid: true, gridcolor: "#EEEEEE", zeroline: false, tickfont: {{ color: "#888888", size: 10 }}, linecolor: "#E0E0E0" }},
  yaxis2: {{ showgrid: true, gridcolor: "#EEEEEE", zeroline: false, tickfont: {{ color: "#888888", size: 10 }}, linecolor: "#E0E0E0" }},
}};

const PLOTLY_CONFIG = {{
  displayModeBar: true, displaylogo: false,
  modeBarButtonsToRemove: ["select2d", "lasso2d", "autoScale2d"],
  responsive: true,
}};

// ── Date range ────────────────────────────────────────────────────────────────
function getDateRange(months) {{
  if (months === 0) return {{ from: null, to: null }};
  const to = new Date(), from = new Date();
  from.setMonth(from.getMonth() - months);
  return {{ from: from.toISOString().slice(0,10), to: to.toISOString().slice(0,10) }};
}}

function setRange(months) {{
  currentMonths = months; dateFrom = null; dateTo = null;
  document.getElementById("date-from").value = "";
  document.getElementById("date-to").value = "";
  document.querySelectorAll(".btn-range").forEach(b => b.classList.remove("active"));
  event.target.classList.add("active");
  renderAllCharts();
}}

function applyCustomRange() {{
  dateFrom = document.getElementById("date-from").value;
  dateTo   = document.getElementById("date-to").value;
  document.querySelectorAll(".btn-range").forEach(b => b.classList.remove("active"));
  if (dateFrom || dateTo) renderAllCharts();
}}

function filterByRange(dates, arrays) {{
  let range = (dateFrom || dateTo) ? {{ from: dateFrom, to: dateTo }} : getDateRange(currentMonths);
  if (!range.from && !range.to) return {{ dates, arrays }};
  const from = range.from || "0000-01-01";
  const to   = range.to   || "9999-12-31";
  const filteredDates = [];
  const filteredArrays = Object.fromEntries(Object.keys(arrays).map(k => [k, []]));
  dates.forEach((d, i) => {{
    if (d >= from && d <= to) {{
      filteredDates.push(d);
      Object.keys(arrays).forEach(k => filteredArrays[k].push(arrays[k][i]));
    }}
  }});
  return {{ dates: filteredDates, arrays: filteredArrays }};
}}

// ── Chart rendering ───────────────────────────────────────────────────────────
function renderChart(ind) {{
  const el = document.getElementById("chart-" + ind.id);
  if (!el) return;
  const base = (ALL_CHARTS[currentCountry] || {{}})[ind.id];
  if (!base) return;

  const rawDates = (ALL_DATES[currentCountry] || {{}})[ind.id] || [];
  const rawData  = (ALL_DATA[currentCountry]  || {{}})[ind.id] || {{}};

  const {{ dates, arrays }} = filterByRange(rawDates, {{
    value:   rawData.value   || [],
    mom_pct: rawData.mom_pct || [],
    yoy_pct: rawData.yoy_pct || [],
  }});

  // ── CPI indicators: single panel, YoY% + MoM% only, no level trace ──
  if (CPI_IDS.has(ind.id)) {{
    const cpiTraces = [];
    const hasYoy = arrays.yoy_pct && arrays.yoy_pct.some(v => v !== null && !isNaN(v));
    const hasMom = arrays.mom_pct && arrays.mom_pct.some(v => v !== null && !isNaN(v));
    if (hasYoy) {{
      cpiTraces.push({{ x: dates, y: arrays.yoy_pct, type: "scatter", mode: "lines", name: "YoY %",
        line: {{ color: "#DA291C", width: 1.8 }},
        hovertemplate: "<b>%{{x}}</b><br>YoY: %{{y:+.4f}}%<extra></extra>" }});
    }}
    if (hasMom) {{
      cpiTraces.push({{ x: dates, y: arrays.mom_pct, type: "scatter", mode: "lines", name: "MoM %",
        line: {{ color: "#3498DB", width: 1.5, dash: "dot" }},
        hovertemplate: "<b>%{{x}}</b><br>MoM: %{{y:+.4f}}%<extra></extra>" }});
    }}
    const cpiLayout = {{ ...BASE_LAYOUT,
      yaxis: {{ ...BASE_LAYOUT.yaxis, tickformat: ".4f", ticksuffix: "%" }},
      shapes: [{{ type: "line", xref: "paper", x0: 0, x1: 1, yref: "y", y0: 0, y1: 0,
                  line: {{ color: "#CCCCCC", width: 1 }} }}],
    }};
    const src = ind.source || "";
    if (src) {{
      cpiLayout.annotations = [{{ xref:"paper", yref:"paper", x:1, y:-0.08,
        text:"Fuente: " + src, showarrow:false, xanchor:"right",
        font:{{size:9, color:"#AAAAAA", family:"Inter, Arial"}} }}];
    }}
    Plotly.react(el, cpiTraces, cpiLayout, PLOTLY_CONFIG);
    return;
  }}

  const isPct  = ind.val_fmt === "pct";
  const levelFmt    = isPct ? ".2f"  : ",.2f";
  const levelSuffix = isPct ? "%"    : "";
  const levelHover  = isPct
    ? "<b>%{{x}}</b><br>" + ind.name + ": %{{y:.2f}}%<extra></extra>"
    : "<b>%{{x}}</b><br>" + ind.name + ": %{{y:,.2f}}<extra></extra>";

  const traces = [];
  const levelTrace = {{
    x: dates, y: arrays.value, type: "scatter", mode: "lines",
    name: ind.name, line: {{ color: "#FFC72C", width: 2.2 }},
    hovertemplate: levelHover, xaxis: "x", yaxis: "y",
  }};
  traces.push(levelTrace);

  const levelYAxis = {{ ...BASE_LAYOUT.yaxis, tickformat: levelFmt, ticksuffix: levelSuffix }};
  const pctYAxis   = {{ ...BASE_LAYOUT.yaxis, tickformat: ".2f", ticksuffix: "%" }};
  let layout = {{ ...BASE_LAYOUT, yaxis: levelYAxis }};

  if ((ind.mom && arrays.mom_pct.length) || (ind.yoy && arrays.yoy_pct.length)) {{
    levelTrace.yaxis = "y"; levelTrace.xaxis = "x2";
    if (ind.mom && arrays.mom_pct.length) {{
      traces.push({{ x: dates, y: arrays.mom_pct, type: "scatter", mode: "lines", name: "MoM %",
        line: {{ color: "#3498DB", width: 1.6, dash: "dot" }},
        hovertemplate: "<b>%{{x}}</b><br>MoM: %{{y:+.2f}}%<extra></extra>",
        xaxis: "x", yaxis: "y2" }});
    }}
    if (ind.yoy && arrays.yoy_pct.length) {{
      traces.push({{ x: dates, y: arrays.yoy_pct, type: "scatter", mode: "lines", name: "YoY %",
        line: {{ color: "#DA291C", width: 1.6 }},
        hovertemplate: "<b>%{{x}}</b><br>YoY: %{{y:+.2f}}%<extra></extra>",
        xaxis: "x", yaxis: "y2" }});
    }}
    layout = {{
      ...BASE_LAYOUT,
      grid: {{ rows: 2, columns: 1, subplots: [["xy2"], ["xy"]], roworder: "top to bottom" }},
      xaxis:  {{ ...BASE_LAYOUT.xaxis,  domain: [0,1], anchor: "y2" }},
      yaxis2: {{ ...levelYAxis, domain: [0.55, 1], anchor: "x" }},
      xaxis2: {{ ...BASE_LAYOUT.xaxis,  domain: [0,1], anchor: "y" }},
      yaxis:  {{ ...pctYAxis, domain: [0, 0.45], anchor: "x2",
                 title: {{ text: "% change", font: {{ size: 9, color: "#888888" }} }} }},
      shapes: [{{ type: "line", xref: "paper", x0: 0, x1: 1,
                  yref: "y", y0: 0, y1: 0, line: {{ color: "#CCCCCC", width: 1 }} }}],
    }};
  }}

  // Reference lines (US only)
  if (currentCountry === "us") {{
    const refLines = REFERENCE_LINES[ind.id];
    if (refLines) {{
      const isTwoPanel = (ind.mom && arrays.mom_pct.length) || (ind.yoy && arrays.yoy_pct.length);
      layout.shapes      = layout.shapes      || [];
      layout.annotations = layout.annotations || [];
      refLines.forEach(ref => {{
        const yref = isTwoPanel ? (ref.panel === "pct" ? "y" : "y2") : "y";
        layout.shapes.push({{ type: "line", xref: "paper", x0: 0, x1: 1,
          yref, y0: ref.y, y1: ref.y,
          line: {{ color: "#888888", width: 1.2, dash: "dot" }}, layer: "above" }});
        layout.annotations.push({{ xref: "paper", x: 0.01, yref, y: ref.y,
          text: ref.label, showarrow: false, xanchor: "left", yanchor: "bottom",
          font: {{ size: 9, color: "#888888", family: "Inter, Arial, sans-serif" }},
          bgcolor: "rgba(255,255,255,0.75)" }});
      }});
    }}
  }}

  // Source annotation
  const sourceText = ind.source || "";
  if (sourceText) {{
    layout.annotations = layout.annotations || [];
    layout.annotations.push({{
      xref:"paper", yref:"paper", x:1, y:-0.08,
      text:"Fuente: " + sourceText, showarrow:false, xanchor:"right",
      font:{{size:9, color:"#AAAAAA", family:"Inter, Arial"}},
    }});
  }}

  Plotly.react(el, traces, layout, PLOTLY_CONFIG);
}}

function renderAllCharts() {{
  (ALL_INDS[currentCountry] || []).forEach(ind => renderChart(ind));
}}

// ── Helpers for exec summary ──────────────────────────────────────────────────
function badge(cls, text) {{ return `<span class="exec-badge ${{cls}}">${{text}}</span>`; }}

function lastVal(id, field) {{
  const data = (ALL_DATA[currentCountry] || {{}})[id];
  if (!data) return null;
  const arr = field ? data[field] : data.value;
  if (!arr || !arr.length) return null;
  for (let i = arr.length - 1; i >= 0; i--) {{
    if (arr[i] !== null && arr[i] !== undefined && !isNaN(arr[i])) return arr[i];
  }}
  return null;
}}

// ── Executive Summary ─────────────────────────────────────────────────────────
function buildExecSummary() {{
  const fn = {{
    us: buildExecSummaryUS, br: buildExecSummaryBR,
    cl: buildExecSummaryCL, co: buildExecSummaryCO,
    mx: buildExecSummaryMX, ar: buildExecSummaryAR,
  }};
  (fn[currentCountry] || buildExecSummaryUS)();
}}

function buildExecSummaryUS() {{
  const cpiYoy = lastVal("CPILFESL", "yoy_pct");
  const pceYoy = lastVal("PCEPILFE", "yoy_pct");
  let infText = "";
  if (cpiYoy !== null && pceYoy !== null) {{
    const avg = (cpiYoy + pceYoy) / 2;
    const cls = avg > 2.5 ? "badge-alert" : avg > 2.0 ? "badge-warn" : "badge-ok";
    const lbl = avg > 2.5 ? "Above Target" : avg > 2.0 ? "Near Target" : "On Target";
    infText = badge(cls, lbl) + `Core CPI YoY <b>${{cpiYoy.toFixed(2)}}%</b> · Core PCE YoY <b>${{pceYoy.toFixed(2)}}%</b> vs. Fed target <b>2.0%</b>.`;
  }} else {{ infText = badge("badge-neu", "N/A") + "Insufficient data."; }}
  document.getElementById("exec-inflation").innerHTML = infText;

  const unrate = lastVal("UNRATE"); const jolts = lastVal("JTSJOL"); const icsa = lastVal("ICSA");
  let labText = "";
  if (unrate !== null) {{
    const cls = unrate > 5.0 ? "badge-alert" : unrate > 4.2 ? "badge-warn" : "badge-ok";
    const lbl = unrate > 5.0 ? "Weakening" : unrate > 4.2 ? "Softening" : "Solid";
    labText = badge(cls, lbl) + `Unemployment <b>${{unrate.toFixed(1)}}%</b>`;
    if (jolts !== null) labText += ` · JOLTs <b>${{(jolts/1000).toFixed(1)}}M</b>`;
    if (icsa  !== null) labText += ` · Claims <b>${{icsa.toLocaleString("en-US")}}</b>`;
    labText += ".";
  }} else {{ labText = badge("badge-neu", "N/A") + "Insufficient data."; }}
  document.getElementById("exec-labor").innerHTML = labText;

  const gdp = lastVal("A191RL1Q225SBEA"); const indpro = lastVal("INDPRO","mom_pct"); const retail = lastVal("RSXFS","mom_pct");
  let actText = "";
  if (gdp !== null) {{
    const cls = gdp >= 2.0 ? "badge-ok" : gdp >= 0 ? "badge-warn" : "badge-alert";
    const lbl = gdp >= 2.0 ? "Expanding" : gdp >= 0 ? "Slowing" : "Contracting";
    actText = badge(cls, lbl) + `GDP QoQ <b>${{gdp.toFixed(1)}}%</b>`;
    if (indpro !== null) actText += ` · IndProd MoM <b>${{indpro >= 0 ? "+" : ""}}${{indpro.toFixed(2)}}%</b>`;
    if (retail !== null) actText += ` · Retail MoM <b>${{retail >= 0 ? "+" : ""}}${{retail.toFixed(2)}}%</b>`;
    actText += ".";
  }} else {{ actText = badge("badge-neu", "N/A") + "Insufficient data."; }}
  document.getElementById("exec-activity").innerHTML = actText;

  const ffr = lastVal("FEDFUNDS");
  let polText = "";
  if (ffr !== null) {{
    const cls = ffr > 4.0 ? "badge-alert" : ffr > 2.5 ? "badge-warn" : "badge-ok";
    const lbl = ffr > 4.0 ? "Restrictive" : ffr > 2.5 ? "Above Neutral" : "Neutral/Accom.";
    polText = badge(cls, lbl) + `Fed Funds Rate <b>${{ffr.toFixed(2)}}%</b> · Neutral <b>2.50%</b>.`;
    if (ffr > 2.5) polText += ` Spread: <b>+${{(ffr-2.5).toFixed(2)}}pp</b>.`;
  }} else {{ polText = badge("badge-neu", "N/A") + "Insufficient data."; }}
  document.getElementById("exec-policy").innerHTML = polText;
}}

function buildExecSummaryBR() {{
  const ipcaMom  = lastVal("br_ipca");
  const ipcaCore = lastVal("br_ipca_core");
  let infText = "";
  if (ipcaMom !== null) {{
    const ann = ipcaMom * 12;
    const cls = ann > 5 ? "badge-alert" : ann > 3 ? "badge-warn" : "badge-ok";
    const lbl = ann > 5 ? "Acima da Meta" : ann > 3 ? "Pr&#243;ximo da Meta" : "Na Meta";
    infText = badge(cls, lbl) + `IPCA MoM <b>${{ipcaMom.toFixed(2)}}%</b> (&#8776;<b>${{ann.toFixed(1)}}%</b> anualiz.)`;
    if (ipcaCore !== null) infText += ` · Core MoM <b>${{ipcaCore.toFixed(2)}}%</b>`;
    infText += ` · Meta BCB 2025: <b>3.0%</b>.`;
  }} else {{ infText = badge("badge-neu", "N/A") + "Dados insuficientes."; }}
  document.getElementById("exec-inflation").innerHTML = infText;

  const unrate = lastVal("br_unrate"); const caged = lastVal("br_caged");
  let labText = "";
  if (unrate !== null) {{
    const cls = unrate > 9 ? "badge-alert" : unrate > 6.5 ? "badge-warn" : "badge-ok";
    const lbl = unrate > 9 ? "Fraco" : unrate > 6.5 ? "Moderado" : "S&#243;lido";
    labText = badge(cls, lbl) + `Desemprego PNAD <b>${{unrate.toFixed(1)}}%</b>`;
    if (caged !== null) labText += ` · Empregos Formais <b>${{(caged/1e6).toFixed(2)}}M</b>`;
    labText += ".";
  }} else {{ labText = badge("badge-neu", "N/A") + "Dados insuficientes."; }}
  document.getElementById("exec-labor").innerHTML = labText;

  const gdpMom = lastVal("br_gdp","mom_pct"); const indpro = lastVal("br_indpro","mom_pct"); const retail = lastVal("br_retail","mom_pct");
  let actText = "";
  if (gdpMom !== null) {{
    const cls = gdpMom >= 0.5 ? "badge-ok" : gdpMom >= 0 ? "badge-warn" : "badge-alert";
    const lbl = gdpMom >= 0.5 ? "Expandindo" : gdpMom >= 0 ? "Desacelerando" : "Contraindo";
    actText = badge(cls, lbl) + `PIB &#237;ndice QoQ <b>${{gdpMom >= 0 ? "+" : ""}}${{gdpMom.toFixed(2)}}%</b>`;
    if (indpro !== null) actText += ` · Ind. MoM <b>${{indpro >= 0 ? "+" : ""}}${{indpro.toFixed(2)}}%</b>`;
    if (retail !== null) actText += ` · Varejo MoM <b>${{retail >= 0 ? "+" : ""}}${{retail.toFixed(2)}}%</b>`;
    actText += ".";
  }} else {{ actText = badge("badge-neu", "N/A") + "Dados insuficientes."; }}
  document.getElementById("exec-activity").innerHTML = actText;

  const selic = lastVal("br_selic");
  let polText = "";
  if (selic !== null) {{
    const cls = selic > 10 ? "badge-alert" : selic > 6.5 ? "badge-warn" : "badge-ok";
    const lbl = selic > 10 ? "Restritiva" : selic > 6.5 ? "Acima do Neutro" : "Neutra/Acomod.";
    polText = badge(cls, lbl) + `Taxa Selic <b>${{selic.toFixed(2)}}%</b> · Neutro estimado <b>&#8764;6.5%</b>.`;
    if (selic > 6.5) polText += ` Spread: <b>+${{(selic-6.5).toFixed(2)}}pp</b>.`;
  }} else {{ polText = badge("badge-neu", "N/A") + "Dados insuficientes."; }}
  document.getElementById("exec-policy").innerHTML = polText;
}}

function buildExecSummaryCL() {{
  const cpiYoy = lastVal("cl_cpi");
  const cpiMom = lastVal("cl_cpi", "mom_pct");
  let infText = "";
  if (cpiYoy !== null) {{
    const cls = cpiYoy > 4.5 ? "badge-alert" : cpiYoy > 3.0 ? "badge-warn" : "badge-ok";
    const lbl = cpiYoy > 4.5 ? "Sobre Meta" : cpiYoy > 3.0 ? "Cerca Meta" : "En Meta";
    infText = badge(cls, lbl) + `CPI YoY <b>${{cpiYoy.toFixed(2)}}%</b>`;
    if (cpiMom !== null) infText += ` &middot; MoM <b>${{cpiMom.toFixed(2)}}%</b>`;
    infText += ` &middot; Meta BCCh <b>3.0%</b>.`;
  }} else {{ infText = badge("badge-neu", "N/A") + "Datos insuficientes."; }}
  document.getElementById("exec-inflation").innerHTML = infText;

  const unrate = lastVal("cl_unrate");
  let labText = "";
  if (unrate !== null) {{
    const cls = unrate > 8.5 ? "badge-alert" : unrate > 7.0 ? "badge-warn" : "badge-ok";
    const lbl = unrate > 8.5 ? "D&eacute;bil" : unrate > 7.0 ? "Moderado" : "S&oacute;lido";
    labText = badge(cls, lbl) + `Desempleo <b>${{unrate.toFixed(1)}}%</b>.`;
  }} else {{ labText = badge("badge-neu", "N/A") + "Datos insuficientes."; }}
  document.getElementById("exec-labor").innerHTML = labText;

  const gdpMom = lastVal("cl_gdp", "mom_pct");
  let actText = "";
  if (gdpMom !== null) {{
    const cls = gdpMom >= 0.5 ? "badge-ok" : gdpMom >= 0 ? "badge-warn" : "badge-alert";
    const lbl = gdpMom >= 0.5 ? "Expandiendo" : gdpMom >= 0 ? "Desacelerando" : "Contrayendo";
    actText = badge(cls, lbl) + `PIB MoM <b>${{gdpMom >= 0 ? "+" : ""}}${{gdpMom.toFixed(2)}}%</b>.`;
  }} else {{ actText = badge("badge-neu", "N/A") + "Datos insuficientes."; }}
  document.getElementById("exec-activity").innerHTML = actText;

  const rate = lastVal("cl_rate");
  let polText = "";
  if (rate !== null) {{
    const cls = rate > 6.0 ? "badge-alert" : rate > 4.0 ? "badge-warn" : "badge-ok";
    const lbl = rate > 6.0 ? "Restrictiva" : rate > 4.0 ? "Sobre Neutro" : "Neutra/Acomod.";
    polText = badge(cls, lbl) + `Tasa BCCh <b>${{rate.toFixed(2)}}%</b>.`;
  }} else {{ polText = badge("badge-neu", "N/A") + "Datos insuficientes."; }}
  document.getElementById("exec-policy").innerHTML = polText;
}}

function buildExecSummaryCO() {{
  const cpiYoy = lastVal("co_cpi");
  const cpiMom = lastVal("co_cpi", "mom_pct");
  let infText = "";
  if (cpiYoy !== null) {{
    const cls = cpiYoy > 5.0 ? "badge-alert" : cpiYoy > 3.0 ? "badge-warn" : "badge-ok";
    const lbl = cpiYoy > 5.0 ? "Sobre Meta" : cpiYoy > 3.0 ? "Cerca Meta" : "En Meta";
    infText = badge(cls, lbl) + `CPI YoY <b>${{cpiYoy.toFixed(2)}}%</b>`;
    if (cpiMom !== null) infText += ` &middot; MoM <b>${{cpiMom.toFixed(2)}}%</b>`;
    infText += ` &middot; Meta BanRep <b>3.0%</b>.`;
  }} else {{ infText = badge("badge-neu", "N/A") + "Datos insuficientes."; }}
  document.getElementById("exec-inflation").innerHTML = infText;

  const unrate = lastVal("co_unrate");
  let labText = "";
  if (unrate !== null) {{
    const cls = unrate > 12 ? "badge-alert" : unrate > 9 ? "badge-warn" : "badge-ok";
    const lbl = unrate > 12 ? "D&eacute;bil" : unrate > 9 ? "Moderado" : "S&oacute;lido";
    labText = badge(cls, lbl) + `Desempleo <b>${{unrate.toFixed(1)}}%</b>.`;
  }} else {{ labText = badge("badge-neu", "N/A") + "Datos insuficientes."; }}
  document.getElementById("exec-labor").innerHTML = labText;
  document.getElementById("exec-activity").innerHTML = badge("badge-neu", "N/D") + "Sin datos de actividad.";

  const rate = lastVal("co_rate");
  let polText = "";
  if (rate !== null) {{
    const cls = rate > 7.0 ? "badge-alert" : rate > 5.0 ? "badge-warn" : "badge-ok";
    const lbl = rate > 7.0 ? "Restrictiva" : rate > 5.0 ? "Sobre Neutro" : "Neutra/Acomod.";
    polText = badge(cls, lbl) + `Tasa BanRep <b>${{rate.toFixed(2)}}%</b>.`;
  }} else {{ polText = badge("badge-neu", "N/A") + "Datos insuficientes."; }}
  document.getElementById("exec-policy").innerHTML = polText;
}}

function buildExecSummaryMX() {{
  const cpiYoy = lastVal("mx_cpi");
  const cpiMom = lastVal("mx_cpi", "mom_pct");
  let infText = "";
  if (cpiYoy !== null) {{
    const cls = cpiYoy > 5.0 ? "badge-alert" : cpiYoy > 3.5 ? "badge-warn" : "badge-ok";
    const lbl = cpiYoy > 5.0 ? "Sobre Meta" : cpiYoy > 3.5 ? "Cerca Meta" : "En Meta";
    infText = badge(cls, lbl) + `INPC YoY <b>${{cpiYoy.toFixed(2)}}%</b>`;
    if (cpiMom !== null) infText += ` &middot; MoM <b>${{cpiMom.toFixed(2)}}%</b>`;
    infText += ` &middot; Meta Banxico <b>3.0%</b>.`;
  }} else {{ infText = badge("badge-neu", "N/A") + "Datos insuficientes."; }}
  document.getElementById("exec-inflation").innerHTML = infText;

  const unrate = lastVal("mx_unrate");
  let labText = "";
  if (unrate !== null) {{
    const cls = unrate > 4.5 ? "badge-alert" : unrate > 3.5 ? "badge-warn" : "badge-ok";
    const lbl = unrate > 4.5 ? "D&eacute;bil" : unrate > 3.5 ? "Moderado" : "S&oacute;lido";
    labText = badge(cls, lbl) + `Desempleo <b>${{unrate.toFixed(1)}}%</b>.`;
  }} else {{ labText = badge("badge-neu", "N/A") + "Datos insuficientes."; }}
  document.getElementById("exec-labor").innerHTML = labText;
  document.getElementById("exec-activity").innerHTML = badge("badge-neu", "N/D") + "Sin datos de actividad.";

  const rate = lastVal("mx_rate");
  let polText = "";
  if (rate !== null) {{
    const cls = rate > 8.0 ? "badge-alert" : rate > 6.0 ? "badge-warn" : "badge-ok";
    const lbl = rate > 8.0 ? "Restrictiva" : rate > 6.0 ? "Sobre Neutro" : "Neutra/Acomod.";
    polText = badge(cls, lbl) + `Tasa Banxico <b>${{rate.toFixed(2)}}%</b>.`;
  }} else {{ polText = badge("badge-neu", "N/A") + "Datos insuficientes."; }}
  document.getElementById("exec-policy").innerHTML = polText;
}}

function buildExecSummaryAR() {{
  const cpiYoy = lastVal("ar_cpi", "yoy_pct");
  const cpiVal = lastVal("ar_cpi");
  let infText = "";
  if (cpiYoy !== null) {{
    const cls = cpiYoy > 100 ? "badge-alert" : cpiYoy > 50 ? "badge-warn" : "badge-ok";
    const lbl = cpiYoy > 100 ? "Hiperinflaci&oacute;n" : cpiYoy > 50 ? "Muy Alta" : "Moderando";
    infText = badge(cls, lbl) + `IPC YoY <b>${{cpiYoy.toFixed(1)}}%</b>`;
    if (cpiVal !== null) infText += ` &middot; MoM <b>${{cpiVal.toFixed(2)}}%</b>`;
    infText += " &middot; Fuente: INDEC API";
  }} else if (cpiVal !== null) {{
    const ann = cpiVal * 12;
    const cls = ann > 50 ? "badge-alert" : ann > 20 ? "badge-warn" : "badge-ok";
    const lbl = ann > 50 ? "Muy Alta" : ann > 20 ? "Elevada" : "Moderando";
    infText = badge(cls, lbl) + `IPC MoM <b>${{cpiVal.toFixed(2)}}%</b> (&asymp;<b>${{ann.toFixed(0)}}%</b> anualiz.)`;
  }} else {{ infText = badge("badge-neu", "N/A") + "Datos insuficientes."; }}
  document.getElementById("exec-inflation").innerHTML = infText;

  const unrate = lastVal("ar_unrate");
  let labText = "";
  if (unrate !== null) {{
    const cls = unrate > 9 ? "badge-alert" : unrate > 7 ? "badge-warn" : "badge-ok";
    const lbl = unrate > 9 ? "D&eacute;bil" : unrate > 7 ? "Moderado" : "S&oacute;lido";
    labText = badge(cls, lbl) + `Desocupaci&oacute;n EPH <b>${{unrate.toFixed(1)}}%</b> &middot; Dato trimestral &#9888;`;
  }} else {{ labText = badge("badge-neu", "N/A") + "Datos insuficientes."; }}
  document.getElementById("exec-labor").innerHTML = labText;

  const emaeMom = lastVal("ar_emae", "mom_pct");
  let actText = "";
  if (emaeMom !== null) {{
    const cls = emaeMom >= 0.3 ? "badge-ok" : emaeMom >= 0 ? "badge-warn" : "badge-alert";
    const lbl = emaeMom >= 0.3 ? "Expandiendo" : emaeMom >= 0 ? "Estable" : "Contrayendo";
    actText = badge(cls, lbl) + `EMAE MoM <b>${{emaeMom >= 0 ? "+" : ""}}${{emaeMom.toFixed(2)}}%</b> &middot; Fuente: INDEC &#9888;`;
  }} else {{ actText = badge("badge-neu", "N/A") + "Datos insuficientes."; }}
  document.getElementById("exec-activity").innerHTML = actText;

  const rate = lastVal("ar_rate");
  let polText = "";
  if (rate !== null) {{
    polText = badge("badge-alert", "Restrictiva") + `Tasa BCRA <b>${{rate.toFixed(2)}}% anual</b> &middot; Fuente: BCRA &#9888;`;
  }} else {{ polText = badge("badge-neu", "N/A") + "Datos insuficientes."; }}
  document.getElementById("exec-policy").innerHTML = polText;
}}

// ── Ticker bar ────────────────────────────────────────────────────────────────
function buildTicker() {{
  const bar = document.getElementById("ticker-bar");
  const US_ITEMS = [
    {{ id: "FEDFUNDS",        label: "FFR"          }},
    {{ id: "UNRATE",          label: "UNRATE"       }},
    {{ id: "CPILFESL",        label: "CORE CPI YoY", pct: "yoy_pct" }},
    {{ id: "PCEPILFE",        label: "CORE PCE YoY", pct: "yoy_pct" }},
    {{ id: "ICSA",            label: "JOBLESS"      }},
    {{ id: "A191RL1Q225SBEA", label: "GDP QoQ"      }},
    {{ id: "JTSJOL",          label: "JOLTS"        }},
  ];
  const BR_ITEMS = [
    {{ id: "br_selic",        label: "SELIC"        }},
    {{ id: "br_unrate",       label: "DESEMPREGO"   }},
    {{ id: "br_ipca",         label: "IPCA MoM"     }},
    {{ id: "br_ipca_core",    label: "IPCA CORE"    }},
    {{ id: "br_gdp",          label: "PIB MoM",      pct: "mom_pct" }},
    {{ id: "br_caged",        label: "CAGED YoY",    pct: "yoy_pct" }},
    {{ id: "br_usdbrl",       label: "BRL/USD"      }},
  ];
  const CL_ITEMS = [
    {{ id: "cl_rate",   label: "BCCh RATE"   }},
    {{ id: "cl_unrate", label: "DESEMPLEO"   }},
    {{ id: "cl_cpi",    label: "CPI YoY"     }},
    {{ id: "cl_cpi",    label: "CPI MoM",    pct: "mom_pct" }},
    {{ id: "cl_gdp",    label: "PIB MoM",    pct: "mom_pct" }},
  ];
  const CO_ITEMS = [
    {{ id: "co_rate",   label: "BANREP RATE" }},
    {{ id: "co_unrate", label: "DESEMPLEO"   }},
    {{ id: "co_cpi",    label: "CPI YoY"     }},
    {{ id: "co_cpi",    label: "CPI MoM",    pct: "mom_pct" }},
  ];
  const MX_ITEMS = [
    {{ id: "mx_rate",   label: "BANXICO RATE" }},
    {{ id: "mx_unrate", label: "DESEMPLEO"    }},
    {{ id: "mx_cpi",    label: "CPI YoY"      }},
    {{ id: "mx_cpi",    label: "CPI MoM",     pct: "mom_pct" }},
  ];
  const AR_ITEMS = [
    {{ id: "ar_cpi",     label: "CPI YoY",   pct: "yoy_pct" }},
    {{ id: "ar_cpi",     label: "CPI MoM"    }},
    {{ id: "ar_rate",    label: "BCRA RATE"  }},
    {{ id: "ar_unrate",  label: "DESEMPLEO"  }},
    {{ id: "ar_emae",    label: "EMAE MoM",  pct: "mom_pct" }},
    {{ id: "ar_dolar",   label: "BLUE ARS",  pct: "blue_rate" }},
  ];
  const TICKER_MAP = {{ us: US_ITEMS, br: BR_ITEMS, cl: CL_ITEMS, co: CO_ITEMS, mx: MX_ITEMS, ar: AR_ITEMS }};
  const items = TICKER_MAP[currentCountry] || US_ITEMS;
  const data  = ALL_DATA[currentCountry] || {{}};

  bar.innerHTML = items.map(item => {{
    const d = data[item.id];
    if (!d || !d.value.length) return "";
    const vals = item.pct ? (d[item.pct] || []) : d.value;
    const filtered = vals.filter(v => v !== null && v !== undefined && !isNaN(v));
    if (!filtered.length) return "";
    const last = filtered[filtered.length - 1];
    const prev = filtered.length > 1 ? filtered[filtered.length - 2] : last;
    const chg  = last - prev;
    const chgCls = chg >= 0 ? "ticker-pos" : "ticker-neg";
    const lastFmt = last.toLocaleString("en-US", {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
    const chgFmt  = (chg >= 0 ? "+" : "") + chg.toLocaleString("en-US", {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
    return `<div class="ticker-item">
      <span class="ticker-label">${{item.label}}</span>
      <span class="ticker-val">${{lastFmt}}</span>
      <span class="${{chgCls}}">${{chgFmt}}</span>
    </div>`;
  }}).join("");
}}

// ── Upcoming Events (US) ──────────────────────────────────────────────────────
const ECONOMIC_EVENTS = [
  {{ week: "Week of Mar 9 \u2013 14, 2026" }},
  {{ date: "2026-03-11", name: "NFIB Small Business Optimism",  period: "Feb 2026",    consensus: "103.0",           previous: "102.8",     imp: "low"  }},
  {{ date: "2026-03-12", name: "CPI (All Items)",               period: "Feb 2026",    consensus: "+0.3% MoM",       previous: "+0.5%",     imp: "high" }},
  {{ date: "2026-03-12", name: "Core CPI",                      period: "Feb 2026",    consensus: "+0.3% MoM",       previous: "+0.4%",     imp: "high" }},
  {{ date: "2026-03-13", name: "PPI Final Demand",              period: "Feb 2026",    consensus: "+0.3% MoM",       previous: "+0.4%",     imp: "high" }},
  {{ date: "2026-03-13", name: "Initial Jobless Claims",        period: "Wk Mar 8",    consensus: "220K",            previous: "213K",      imp: "high" }},
  {{ date: "2026-03-14", name: "Michigan Consumer Sentiment",   period: "Mar 2026 P",  consensus: "57.5",            previous: "56.4",      imp: "med"  }},
  {{ week: "Week of Mar 16 \u2013 20, 2026" }},
  {{ date: "2026-03-17", name: "Retail Sales",                  period: "Feb 2026",    consensus: "+0.6% MoM",       previous: "-0.9%",     imp: "high" }},
  {{ date: "2026-03-17", name: "Empire State Mfg Survey",       period: "Mar 2026",    consensus: "-5.0",            previous: "-20.1",     imp: "med"  }},
  {{ date: "2026-03-18", name: "Industrial Production",         period: "Feb 2026",    consensus: "+0.2% MoM",       previous: "+0.5%",     imp: "high" }},
  {{ date: "2026-03-18", name: "Housing Starts",                period: "Feb 2026",    consensus: "1.38M",           previous: "1.37M",     imp: "med"  }},
  {{ date: "2026-03-19", name: "FOMC Decision & Press Conf.",   period: "Mar 2026",    consensus: "Hold 4.25\u20134.50%", previous: "Hold", imp: "high", fomc: true }},
  {{ date: "2026-03-20", name: "Initial Jobless Claims",        period: "Wk Mar 15",   consensus: "222K",            previous: "\u2014",    imp: "high" }},
  {{ date: "2026-03-20", name: "Philadelphia Fed Mfg Survey",   period: "Mar 2026",    consensus: "2.5",             previous: "18.1",      imp: "med"  }},
  {{ date: "2026-03-20", name: "Existing Home Sales",           period: "Feb 2026",    consensus: "3.95M",           previous: "4.08M",     imp: "med"  }},
];

function buildEventsTable() {{
  const today = new Date().toISOString().slice(0,10);
  const tbody = document.getElementById("events-tbody");
  if (!tbody) return;
  let html = "";
  ECONOMIC_EVENTS.forEach(ev => {{
    if (ev.week) {{ html += `<tr class="ev-week-header"><td colspan="5">${{ev.week}}</td></tr>`; return; }}
    const isPast  = ev.date < today;
    const isToday = ev.date === today;
    const rowCls  = ev.fomc ? "ev-fomc" : (isToday ? "ev-today" : "");
    const pastSt  = isPast ? 'style="opacity:0.45"' : "";
    const dot     = `<span class="ev-importance-${{ev.imp}}"></span>`;
    const d = new Date(ev.date + "T12:00:00");
    const dateFmt = d.toLocaleDateString("en-US", {{ weekday:"short", month:"short", day:"numeric" }});
    html += `<tr class="${{rowCls}}" ${{pastSt}}>
      <td class="ev-date">${{dateFmt}}</td>
      <td class="ev-name">${{dot}}${{ev.name}}</td>
      <td class="ev-period">${{ev.period}}</td>
      <td class="ev-consensus">${{ev.consensus}}</td>
      <td class="ev-consensus" style="color:#888888">${{ev.previous}}</td>
    </tr>`;
  }});
  tbody.innerHTML = html;
}}

// ── PNG Export per chart ──────────────────────────────────────────────────────
function downloadChartPNG(id) {{
  Plotly.downloadImage(document.getElementById("chart-"+id), {{format:"png", filename:"macro_"+id, width:900, height:500}});
}}

// ── Freshness badges ──────────────────────────────────────────────────────────
function updateFreshnessBadges() {{
  const today = new Date();
  (ALL_INDS[currentCountry] || []).forEach(ind => {{
    const badge = document.getElementById("fresh-" + ind.id);
    if (!badge) return;
    const dates = (ALL_DATES[currentCountry] || {{}})[ind.id] || [];
    if (!dates.length) {{ badge.textContent = "Sin datos"; badge.style.color = "#DA291C"; return; }}
    const lastDate = new Date(dates[dates.length - 1]);
    const lagDays = Math.floor((today - lastDate) / (1000 * 60 * 60 * 24));
    const color = lagDays < 45 ? "#2ECC71" : lagDays < 90 ? "#FFC72C" : "#DA291C";
    badge.textContent = "Al " + lastDate.toLocaleDateString("es-AR") + " (" + lagDays + "d)";
    badge.style.color = color;
  }});
}}

// ── Semáforo de coyuntura ─────────────────────────────────────────────────────
const COLOR_DIR = {json.dumps({
    "CPILFESL":"inflation","PCEPILFE":"inflation","PPIACO":"inflation",
    "br_ipca":"inflation","br_ipca_core":"inflation","br_ipp":"inflation",
    "cl_cpi":"inflation","co_cpi":"inflation","mx_cpi":"inflation","ar_cpi":"inflation",
    "UNRATE":"unemployment","ICSA":"unemployment","UEMPJOLT":"unemployment",
    "br_unrate":"unemployment","cl_unrate":"unemployment","co_unrate":"unemployment",
    "mx_unrate":"unemployment","ar_unrate":"unemployment",
    "INDPRO":"activity","A191RL1Q225SBEA":"activity","RSXFS":"activity",
    "JTSJOL":"activity","UMCSENT":"activity","br_retail":"activity",
    "br_gdp":"activity","br_indpro":"activity","br_caged":"activity","br_icc":"activity",
    "cl_gdp":"activity","ar_emae":"activity",
})};

function buildSemaforo() {{
  const container = document.getElementById("semaforo");
  if (!container) return;
  const inds = ALL_INDS[currentCountry] || [];
  const data = ALL_DATA[currentCountry] || {{}};
  const labels = [];
  inds.forEach(ind => {{
    const d = data[ind.id];
    if (!d || !d.value.length) return;
    const vals = d.value.filter(v => v !== null && !isNaN(v));
    if (vals.length < 2) return;
    const last = vals[vals.length - 1];
    const prev = vals[vals.length - 2];
    const dir = COLOR_DIR[ind.id] || "activity";
    const up = last > prev;
    let emoji;
    if (dir === "inflation" || dir === "unemployment") {{
      emoji = up ? "&#128308;" : last < prev ? "&#128994;" : "&#129025;";
    }} else {{
      emoji = up ? "&#128994;" : last < prev ? "&#128308;" : "&#129025;";
    }}
    labels.push(`<span title="${{ind.name}}: ${{last.toFixed(2)}}">${{emoji}} ${{ind.name}}</span>`);
  }});
  container.innerHTML = labels.join(" &nbsp; ");
}}

// ── Comparador de países ──────────────────────────────────────────────────────
const COMP_LABELS = {{us:"&#127482;&#127480; US", br:"&#127463;&#127479; Brasil", ar:"&#127462;&#127479; Argentina", cl:"&#127464;&#127473; Chile", co:"&#127464;&#127476; Colombia", mx:"&#127474;&#127485; M\u00e9xico"}};

function openComparator() {{
  document.getElementById("comparator-modal").classList.add("open");
  populateComparatorSelects();
}}
function closeComparator() {{
  document.getElementById("comparator-modal").classList.remove("open");
}}
function populateComparatorSelects() {{
  const countries = ["us","br","ar","cl","co","mx"];
  ["comp-country1","comp-country2"].forEach((selId, idx) => {{
    const sel = document.getElementById(selId);
    sel.innerHTML = countries.map(c => `<option value="${{c}}" ${{(idx===0&&c==="us")||(idx===1&&c==="br")?"selected":""}}>${{COMP_LABELS[c]}}</option>`).join("");
  }});
  updateComparatorIndicators();
}}
function updateComparatorIndicators() {{
  const c1 = document.getElementById("comp-country1").value;
  const c2 = document.getElementById("comp-country2").value;
  const allInds = [...(ALL_INDS[c1]||[]).map(i=>{{return {{...i, _country:c1}}}}), ...(ALL_INDS[c2]||[]).map(i=>{{return {{...i, _country:c2}}}})];
  const sel = document.getElementById("comp-indicator");
  sel.innerHTML = allInds.map(i => `<option value="${{i._country}}:${{i.id}}">${{COMP_LABELS[i._country]||i._country}} — ${{i.name}}</option>`).join("");
}}
function renderComparatorChart() {{
  const c1 = document.getElementById("comp-country1").value;
  const c2 = document.getElementById("comp-country2").value;
  const indSel = document.getElementById("comp-indicator").value;
  const [origCountry, indId] = indSel.split(":");
  const colors = ["#FFC72C", "#3498DB"];
  const traces = [];
  [[c1, colors[0]], [c2, colors[1]]].forEach(([cc, color]) => {{
    const dates = (ALL_DATES[cc]||{{}})[indId] || [];
    const vals  = (ALL_DATA[cc]||{{}})[indId]?.value || [];
    if (!dates.length) return;
    const cName = (COMP_LABELS[cc]||cc).replace(/&#[0-9]+;/g,"").trim();
    traces.push({{x:dates, y:vals, type:"scatter", mode:"lines", name:cName, line:{{color,width:2}}}});
  }});
  Plotly.react("comp-chart", traces, {{
    paper_bgcolor:"#F5F5F5", plot_bgcolor:"#FFFFFF",
    font:{{family:"Inter, Arial",color:"#27251F",size:11}},
    margin:{{l:50,r:20,t:20,b:40}},
    legend:{{orientation:"h",y:1.08}},
    hovermode:"x unified",
  }}, {{responsive:true, displaylogo:false}});
}}

// ── Refresh data (static — actualizado por GitHub Actions) ────────────────────

// ── PDF Export ────────────────────────────────────────────────────────────────
function exportPDF() {{
  const now = new Date();
  document.getElementById("print-ts").textContent =
    now.toLocaleDateString("en-US", {{ year:"numeric", month:"long", day:"numeric" }}) + " " +
    now.toLocaleTimeString("en-US", {{ hour:"2-digit", minute:"2-digit" }});
  window.print();
}}

// ── Init ──────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {{
  buildEventsTable();
  buildExecSummary();
  buildTicker();
  buildSemaforo();
  const range = getDateRange(12);
  document.getElementById("date-from").value = range.from || "";
  document.getElementById("date-to").value   = range.to   || "";
  renderAllCharts();
  updateFreshnessBadges();
}});
</script>
</body>
</html>
"""

    # Encode with surrogate handling — replaces any lone surrogates
    html_bytes = html.encode("utf-8", errors="replace")
    with open("index.html", "wb") as f:
        f.write(html_bytes)
    print("index.html generado exitosamente.")


if __name__ == "__main__":
    generate_dashboard()
