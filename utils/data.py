"""
utils/data.py
Helpers de dados, cache e layout de gráficos — Churn & Desativações.
"""
from __future__ import annotations

import json
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from datetime import date
from dateutil.relativedelta import relativedelta
from google.oauth2 import service_account
from google.cloud import bigquery

# ─────────────────────────────────────────────
# PALETA & TEMPLATE
# ─────────────────────────────────────────────
PALETTE = [
    "#6eda2c",  # 0 — verde primário
    "#ffffff",  # 1 — branco
    "#57d124",  # 2 — verde secundário
    "#a0a0a0",  # 3 — cinza médio
    "#4c4c4c",  # 4 — cinza escuro
    "#292929",  # 5 — borda
    "#8ae650",  # 6 — verde claro
    "#3ba811",  # 7 — verde profundo
    "#cccccc",  # 8 — cinza claro
    "#111111",  # 9 — quase preto
]
CHART_TEMPLATE = "plotly_dark"

MODULE_LABELS = {
    "kids":             "Kids",
    "jornada":          "Jornada",
    "loja_inteligente": "Loja Inteligente",
}

MODULE_COLORS = {
    "kids":             "#6eda2c",
    "jornada":          "#ffffff",
    "loja_inteligente": "#a0a0a0",
    "base":             "#4c4c4c",
}

PLAN_LABELS = {
    "pro":     "PRO",
    "lite":    "LITE",
    "starter": "STARTER",
    "basic":   "BASIC",
    "filha":   "FILHA",
    "squad":   "Squad as a Service",
    "outros":  "Outros",
}

PLAN_COLORS = {
    "pro":     "#6eda2c",
    "lite":    "#ffffff",
    "starter": "#a0a0a0",
    "basic":   "#8ae650",
    "filha":   "#4c4c4c",
    "squad":   "#f0a500",
    "outros":  "#292929",
}

# Filtro SQL para excluir linhas de módulos
_EXCL_MODULOS = """
    {col} NOT LIKE '%[KIDS]%'
    AND {col} NOT LIKE '%[JORNADA]%'
    AND {col} NOT LIKE '%[LOJAINTELIGENTE]%'
    AND {col} NOT LIKE '%[LOJAINTELIGENTE_INC]%'
    AND {col} NOT LIKE '%[TOTEM]%'
    AND {col} NOT LIKE '%[V_DEOS]%'
    AND NOT ({col} LIKE '%[STARTER]%' AND {col} LIKE '%Módulo%')
"""

# CASE SQL para classificar plano base
_PLAN_CASE = """
    CASE
      WHEN {col} LIKE '%[PRO]%'               THEN 'pro'
      WHEN {col} LIKE '%[LITE]%'              THEN 'lite'
      WHEN {col} LIKE '%[STARTER]%'           THEN 'starter'
      WHEN {col} LIKE '%[FILHA]%'             THEN 'filha'
      WHEN {col} LIKE '%[BASIC]%'             THEN 'basic'
      WHEN {col} LIKE '%0 - 9 Igrejas%'       THEN 'pro'
      WHEN {col} LIKE '%10+ Igrejas%'         THEN 'pro'
      WHEN {col} LIKE '%App Lite%'            THEN 'lite'
      WHEN {col} LIKE '%App da Igreja%'       THEN 'starter'
      WHEN {col} LIKE '%Squad as a Service%'  THEN 'squad'
      ELSE 'outros'
    END
"""


# ─────────────────────────────────────────────
# LAYOUT PADRÃO DE GRÁFICOS
# ─────────────────────────────────────────────
def chart_layout(fig: go.Figure, height: int = 380, legend_bottom: bool = False) -> go.Figure:
    legend_cfg = dict(
        bgcolor="rgba(0,0,0,0)",
        font=dict(family="Outfit", size=12, color="#a0a0a0"),
    )
    if legend_bottom:
        legend_cfg.update(orientation="h", yanchor="top", y=-0.18, xanchor="center", x=0.5)

    fig.update_layout(
        height=height,
        template=CHART_TEMPLATE,
        margin=dict(l=4, r=4, t=32, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Outfit, sans-serif", color="#ffffff", size=13),
        legend=legend_cfg,
        xaxis=dict(
            showgrid=True, gridcolor="#292929", gridwidth=1,
            zeroline=False, title="", type="category",
        ),
        yaxis=dict(
            showgrid=True, gridcolor="#292929", gridwidth=1,
            zeroline=False, title="",
        ),
        hoverlabel=dict(
            bgcolor="#141414", bordercolor="#292929",
            font_size=13, font_family="Outfit, sans-serif", font_color="#ffffff",
        ),
    )
    return fig


def mes_fmt_ordered(df: pd.DataFrame, date_col: str = "mes") -> tuple[pd.DataFrame, list[str]]:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col)
    df["mes_fmt"] = df[date_col].dt.strftime("%b/%y").str.capitalize()
    ordered = df["mes_fmt"].drop_duplicates().tolist()
    return df, ordered


# ─────────────────────────────────────────────
# SELETORES DE PERÍODO
# ─────────────────────────────────────────────
def period_selector() -> int:
    with st.sidebar:
        st.markdown("### 🗓️ Período")
        n = st.selectbox(
            "Últimos N meses",
            options=[3, 6, 12, 15, 0],
            index=3,
            format_func=lambda x: "Todos" if x == 0 else f"Últimos {x} meses",
            key="period_desativacoes",
        )
    return n


def filter_months(df: pd.DataFrame, n_months: int, date_col: str = "mes") -> pd.DataFrame:
    if df.empty or n_months == 0:
        return df
    cutoff = date.today() - relativedelta(months=n_months)
    cutoff_ts = pd.Timestamp(cutoff)
    col = df[date_col]
    if not pd.api.types.is_datetime64_any_dtype(col):
        col = pd.to_datetime(col, errors="coerce")
    return df[col >= cutoff_ts].copy()


# ─────────────────────────────────────────────
# HELPERS DE KPI
# ─────────────────────────────────────────────
def last_val(df: pd.DataFrame, col: str, date_col: str = "mes"):
    if df.empty or col not in df.columns:
        return None
    ordered = df.sort_values(date_col)
    return ordered[col].iloc[-1] if len(ordered) >= 1 else None


def prev_val(df: pd.DataFrame, col: str, date_col: str = "mes"):
    if df.empty or col not in df.columns:
        return None
    ordered = df.sort_values(date_col)
    return ordered[col].iloc[-2] if len(ordered) >= 2 else None


def delta_str(curr, prev, fmt: str = "+,.0f", suffix: str = "") -> str | None:
    if curr is None or prev is None:
        return None
    diff = curr - prev
    try:
        return f"{diff:{fmt}}{suffix}"
    except Exception:
        return f"{diff:+.2f}{suffix}"


def fmt_brl(value, decimals: int = 2) -> str:
    s = f"{value:,.{decimals}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def no_data(label: str = "Dados não disponíveis") -> None:
    st.info(label, icon="ℹ️")


# ─────────────────────────────────────────────
# CONEXÃO BIGQUERY
# ─────────────────────────────────────────────
def _get_bq_client(project_key: str) -> bigquery.Client:
    cfg = st.secrets["connections"][project_key]
    project = cfg["project"]
    creds_raw = cfg["credentials"]
    if isinstance(creds_raw, str):
        creds_dict = json.loads(creds_raw)
    else:
        creds_dict = dict(creds_raw)
    credentials = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=project, credentials=credentials)


@st.cache_resource
def _bq_client_bi() -> bigquery.Client:
    return _get_bq_client("bigquery_bi")


def _bq_query(query: str) -> pd.DataFrame:
    try:
        return _bq_client_bi().query(query).to_dataframe()
    except Exception as e:
        st.error(f"Erro ao consultar BigQuery: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────
# QUERIES — DESATIVAÇÕES
# ─────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_desativacoes_mensais() -> pd.DataFrame:
    """
    MRR perdido e clientes desativados por módulo por mês (últimos 15 meses).
    Exclui renovações (dt_fim no último dia do mês + reinício no mês seguinte).
    Exclui itens de Setup (não representam churn).
    """
    query = """
    WITH desativados AS (
      SELECT
        st_sincro_sac,
        st_descricao_prd,
        CAST(dt_fim_mens AS DATE)                                    AS dt_fim,
        DATE_TRUNC(CAST(dt_fim_mens AS DATE), MONTH)                 AS mes,
        valor_total
      FROM `business-intelligence-467516.Splgc.vw-splgc-tabela_mrr_validos`
      WHERE dt_fim_mens IS NOT NULL
        AND CAST(dt_fim_mens AS DATE) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 15 MONTH)
        AND CAST(dt_fim_mens AS DATE) <= LAST_DAY(CURRENT_DATE())
        AND st_descricao_prd NOT LIKE '%Setup%'
    ),
    renovacoes AS (
      SELECT DISTINCT d.st_sincro_sac, d.st_descricao_prd, d.mes
      FROM desativados d
      INNER JOIN `business-intelligence-467516.Splgc.vw-splgc-tabela_mrr_validos` r
        ON  d.st_sincro_sac    = r.st_sincro_sac
        AND d.st_descricao_prd = r.st_descricao_prd
        AND DATE_TRUNC(CAST(r.dt_inicio_mens AS DATE), MONTH) = DATE_ADD(d.mes, INTERVAL 1 MONTH)
      WHERE d.dt_fim = LAST_DAY(d.dt_fim)
        AND r.dt_inicio_mens IS NOT NULL
    )
    SELECT
      d.mes,
      CASE
        WHEN d.st_descricao_prd LIKE '%[KIDS]%'                THEN 'kids'
        WHEN d.st_descricao_prd LIKE '%[JORNADA]%'             THEN 'jornada'
        WHEN d.st_descricao_prd LIKE '%[LOJAINTELIGENTE]%'     THEN 'loja_inteligente'
        WHEN d.st_descricao_prd LIKE '%[LOJAINTELIGENTE_INC]%' THEN 'loja_inteligente'
        ELSE                                                         'base'
      END                                                       AS modulo,
      COUNT(DISTINCT d.st_sincro_sac)                           AS clientes_desativados,
      SUM(d.valor_total)                                        AS mrr_perdido
    FROM desativados d
    LEFT JOIN renovacoes rv
      ON  d.st_sincro_sac    = rv.st_sincro_sac
      AND d.st_descricao_prd = rv.st_descricao_prd
      AND d.mes              = rv.mes
    WHERE rv.st_sincro_sac IS NULL
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    df = _bq_query(query)
    if not df.empty:
        df["mes"]    = pd.to_datetime(df["mes"])
        df["modulo"] = df["modulo"].str.lower()
    return df


@st.cache_data(ttl=3600)
def load_desativacoes_por_plano() -> pd.DataFrame:
    """Desativações de plano base por mês. Exclui módulos, setups e renovações."""
    query = f"""
    WITH desativados AS (
      SELECT
        st_sincro_sac,
        st_descricao_prd,
        CAST(dt_fim_mens AS DATE)                                    AS dt_fim,
        DATE_TRUNC(CAST(dt_fim_mens AS DATE), MONTH)                 AS mes,
        valor_total
      FROM `business-intelligence-467516.Splgc.vw-splgc-tabela_mrr_validos`
      WHERE dt_fim_mens IS NOT NULL
        AND CAST(dt_fim_mens AS DATE) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 15 MONTH)
        AND CAST(dt_fim_mens AS DATE) <= LAST_DAY(CURRENT_DATE())
        AND st_descricao_prd NOT LIKE '%Setup%'
        AND {_EXCL_MODULOS.format(col="st_descricao_prd")}
    ),
    renovacoes AS (
      SELECT DISTINCT d.st_sincro_sac, d.st_descricao_prd, d.mes
      FROM desativados d
      INNER JOIN `business-intelligence-467516.Splgc.vw-splgc-tabela_mrr_validos` r
        ON  d.st_sincro_sac    = r.st_sincro_sac
        AND d.st_descricao_prd = r.st_descricao_prd
        AND DATE_TRUNC(CAST(r.dt_inicio_mens AS DATE), MONTH) = DATE_ADD(d.mes, INTERVAL 1 MONTH)
      WHERE d.dt_fim = LAST_DAY(d.dt_fim)
        AND r.dt_inicio_mens IS NOT NULL
    )
    SELECT
      d.mes,
      {_PLAN_CASE.format(col="d.st_descricao_prd")}                  AS plano,
      COUNT(DISTINCT d.st_sincro_sac)                                 AS clientes_desativados,
      SUM(d.valor_total)                                              AS mrr_perdido
    FROM desativados d
    LEFT JOIN renovacoes rv
      ON  d.st_sincro_sac    = rv.st_sincro_sac
      AND d.st_descricao_prd = rv.st_descricao_prd
      AND d.mes              = rv.mes
    WHERE rv.st_sincro_sac IS NULL
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    df = _bq_query(query)
    if not df.empty:
        df["mes"]   = pd.to_datetime(df["mes"])
        df["plano"] = df["plano"].str.lower()
    return df


@st.cache_data(ttl=3600)
def load_desativacoes_detalhado() -> pd.DataFrame:
    """
    Desativações no nível de cliente — mês, módulo, plano, nome e MRR perdido.
    Join com splgc-clientes-inchurch para nome do cliente.
    """
    query = f"""
    WITH desativados AS (
      SELECT
        m.st_sincro_sac,
        m.st_descricao_prd,
        CAST(m.dt_fim_mens AS DATE)                              AS dt_fim,
        DATE_TRUNC(CAST(m.dt_fim_mens AS DATE), MONTH)           AS mes,
        m.valor_total
      FROM `business-intelligence-467516.Splgc.vw-splgc-tabela_mrr_validos` m
      WHERE m.dt_fim_mens IS NOT NULL
        AND CAST(m.dt_fim_mens AS DATE) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 15 MONTH)
        AND CAST(m.dt_fim_mens AS DATE) <= LAST_DAY(CURRENT_DATE())
        AND m.st_descricao_prd NOT LIKE '%Setup%'
    ),
    renovacoes AS (
      SELECT DISTINCT d.st_sincro_sac, d.st_descricao_prd, d.mes
      FROM desativados d
      INNER JOIN `business-intelligence-467516.Splgc.vw-splgc-tabela_mrr_validos` r
        ON  d.st_sincro_sac    = r.st_sincro_sac
        AND d.st_descricao_prd = r.st_descricao_prd
        AND DATE_TRUNC(CAST(r.dt_inicio_mens AS DATE), MONTH) = DATE_ADD(d.mes, INTERVAL 1 MONTH)
      WHERE d.dt_fim = LAST_DAY(d.dt_fim)
        AND r.dt_inicio_mens IS NOT NULL
    )
    SELECT
      d.mes,
      CASE
        WHEN d.st_descricao_prd LIKE '%[KIDS]%'                THEN 'Kids'
        WHEN d.st_descricao_prd LIKE '%[JORNADA]%'             THEN 'Jornada'
        WHEN d.st_descricao_prd LIKE '%[LOJAINTELIGENTE]%'     THEN 'Loja Inteligente'
        WHEN d.st_descricao_prd LIKE '%[LOJAINTELIGENTE_INC]%' THEN 'Loja Inteligente'
        ELSE                                                        'Base'
      END                                                       AS modulo,
      {_PLAN_CASE.format(col="d.st_descricao_prd")}            AS plano,
      d.st_sincro_sac,
      COALESCE(c.st_nome_sac, d.st_sincro_sac)                 AS nome_cliente,
      d.st_descricao_prd                                        AS produto,
      d.valor_total                                             AS mrr_perdido
    FROM desativados d
    LEFT JOIN renovacoes rv
      ON  d.st_sincro_sac    = rv.st_sincro_sac
      AND d.st_descricao_prd = rv.st_descricao_prd
      AND d.mes              = rv.mes
    LEFT JOIN `business-intelligence-467516.Splgc.splgc-clientes-inchurch` c
      ON d.st_sincro_sac = c.st_sincro_sac
    WHERE rv.st_sincro_sac IS NULL
    ORDER BY d.mes DESC, d.valor_total DESC
    """
    df = _bq_query(query)
    if not df.empty:
        df["mes"]   = pd.to_datetime(df["mes"])
        df["plano"] = df["plano"].str.upper()
    return df


@st.cache_data(ttl=3600)
def load_base_ativa_por_plano() -> pd.DataFrame:
    """
    Clientes ativos por plano base no início de cada mês (últimos 15 meses).
    Denominador para cálculo de churn %.
    """
    query = f"""
    SELECT
      cal.mes,
      {_PLAN_CASE.format(col="mrr.st_descricao_prd")}                AS plano,
      COUNT(DISTINCT mrr.st_sincro_sac)                               AS clientes_ativos
    FROM (
      SELECT mes
      FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 15 MONTH),
        DATE_TRUNC(CURRENT_DATE(), MONTH),
        INTERVAL 1 MONTH
      )) AS mes
    ) cal
    CROSS JOIN `business-intelligence-467516.Splgc.vw-splgc-tabela_mrr_validos` mrr
    WHERE CAST(mrr.dt_inicio_mens AS DATE) <= cal.mes
      AND (mrr.dt_fim_mens IS NULL OR CAST(mrr.dt_fim_mens AS DATE) > cal.mes)
      AND {_EXCL_MODULOS.format(col="mrr.st_descricao_prd")}
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    df = _bq_query(query)
    if not df.empty:
        df["mes"]   = pd.to_datetime(df["mes"])
        df["plano"] = df["plano"].str.lower()
    return df
