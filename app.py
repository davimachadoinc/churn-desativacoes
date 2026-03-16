"""
app.py
Dashboard Churn & Desativações — InChurch
Autenticação Google OIDC + conteúdo da página de desativações.
"""
import streamlit as st
import plotly.graph_objects as go
import pandas as pd

st.set_page_config(
    page_title="Churn & Desativações | InChurch",
    page_icon="❌",
    layout="wide",
    initial_sidebar_state="expanded",
)

from utils.style import inject_css
from utils.data import (
    PALETTE, MODULE_LABELS, MODULE_COLORS, PLAN_LABELS, PLAN_COLORS,
    chart_layout, mes_fmt_ordered, period_selector, filter_months,
    last_val, prev_val, delta_str, no_data, fmt_brl,
    load_desativacoes_mensais, load_desativacoes_por_plano,
    load_base_ativa_por_plano, load_desativacoes_detalhado,
)

inject_css()


# ─────────────────────────────────────────────
# AUTENTICAÇÃO
# ─────────────────────────────────────────────
def _check_allowed(email: str) -> bool:
    return email.endswith("@inchurch.com.br")


if not st.user.is_logged_in:
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.markdown(
            """
            <div style="text-align:center; padding: 60px 0 32px 0;">
              <h1 style="font-size:2.4rem; margin-bottom:4px;">
                In<span>Church</span>
              </h1>
              <p style="color:#a0a0a0; font-size:1rem; margin-top:0;">
                Churn & Desativações
              </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("🔐  Entrar com Google", use_container_width=True):
            st.login()
    st.stop()

user_email = st.user.email
user_name  = getattr(st.user, "name", user_email)

if not _check_allowed(user_email):
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.error(
            f"❌ O e-mail **{user_email}** não tem permissão de acesso.\n\n"
            "Entre em contato com o administrador."
        )
        if st.button("↩️  Sair", use_container_width=True):
            st.logout()
    st.stop()

# ── Sidebar ────────────────────────────────────
with st.sidebar:
    st.markdown(
        f"<p style='color:#a0a0a0; font-size:0.82rem; margin-bottom:2px;'>👤 {user_name}</p>"
        f"<p style='color:#4c4c4c; font-size:0.75rem; margin-bottom:16px;'>{user_email}</p>",
        unsafe_allow_html=True,
    )
    st.divider()
    n_months = period_selector()
    st.divider()
    if st.button("🚪 Sair", use_container_width=True):
        st.logout()

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
st.markdown("<h1>Churn <span>& Desativações</span></h1>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────────
with st.spinner("Carregando dados..."):
    df_raw           = load_desativacoes_mensais()
    df_desativ_plano = load_desativacoes_por_plano()
    df_base_plano    = load_base_ativa_por_plano()

if df_raw.empty:
    no_data("Nenhum dado de desativação encontrado.")
    st.stop()

df_raw           = filter_months(df_raw,           n_months, "mes")
df_desativ_plano = filter_months(df_desativ_plano, n_months, "mes")
df_base_plano    = filter_months(df_base_plano,    n_months, "mes")

if df_raw.empty:
    no_data("Nenhuma desativação no período selecionado.")
    st.stop()

df_total = (
    df_raw
    .groupby("mes", as_index=False)
    .agg(mrr_perdido=("mrr_perdido", "sum"), clientes_desativados=("clientes_desativados", "sum"))
    .sort_values("mes")
)

# ─────────────────────────────────────────────
# KPI CARDS
# ─────────────────────────────────────────────
st.subheader("Visão Geral do Período")
k1, k2, k3, k4 = st.columns(4)

curr_mrr = last_val(df_total, "mrr_perdido", "mes")
prev_mrr = prev_val(df_total, "mrr_perdido", "mes")
with k1:
    st.metric(
        "MRR Perdido (último mês)",
        f"R$ {fmt_brl(curr_mrr)}" if curr_mrr else "—",
        delta=delta_str(curr_mrr, prev_mrr, fmt="+,.2f", suffix=" R$"),
    )

with k2:
    st.metric("MRR Perdido (período)", f"R$ {fmt_brl(df_raw['mrr_perdido'].sum())}")

curr_cli = last_val(df_total, "clientes_desativados", "mes")
prev_cli = prev_val(df_total, "clientes_desativados", "mes")
with k3:
    st.metric(
        "Clientes Desativados (último mês)",
        f"{int(curr_cli):,}".replace(",", ".") if curr_cli else "—",
        delta=delta_str(curr_cli, prev_cli),
    )

with k4:
    st.metric(
        "Clientes Desativados (período)",
        f"{int(df_raw['clientes_desativados'].sum()):,}".replace(",", "."),
    )

st.divider()

# ─────────────────────────────────────────────
# SEÇÃO 1 — MRR Perdido Total por Mês
# ─────────────────────────────────────────────
st.subheader("MRR Perdido por Mês")
df_plot_total, x_order_total = mes_fmt_ordered(df_total)

fig = go.Figure()
fig.add_bar(
    x=df_plot_total["mes_fmt"],
    y=df_plot_total["mrr_perdido"],
    marker_color=PALETTE[0],
    text=df_plot_total["mrr_perdido"].apply(lambda v: f"R$ {fmt_brl(v, 0)}"),
    textposition="outside",
    textfont=dict(size=11, color="#a0a0a0"),
)
fig.add_scatter(
    x=df_plot_total["mes_fmt"],
    y=df_plot_total["mrr_perdido"],
    mode="lines+markers",
    line=dict(color=PALETTE[1], width=2, dash="dot"),
    marker=dict(size=6),
    name="Tendência",
    showlegend=True,
)
fig.update_layout(
    showlegend=True,
    xaxis=dict(categoryorder="array", categoryarray=x_order_total, type="category"),
)
st.plotly_chart(chart_layout(fig, height=420), use_container_width=True)

st.divider()

# ─────────────────────────────────────────────
# SEÇÃO 2 — Por Módulo
# ─────────────────────────────────────────────
st.subheader("Por Módulo")
col_a, col_b = st.columns(2)

df_mod = df_raw[df_raw["modulo"].isin(MODULE_LABELS.keys())].copy()
df_plot_mod, x_order_mod = mes_fmt_ordered(df_mod) if not df_mod.empty else (pd.DataFrame(), [])

with col_a:
    st.subheader("MRR Perdido (R$) — Empilhado por Módulo")
    if df_plot_mod.empty:
        no_data()
    else:
        fig = go.Figure()
        for modulo, label in MODULE_LABELS.items():
            sub = df_plot_mod[df_plot_mod["modulo"] == modulo].sort_values("mes")
            if sub.empty:
                continue
            fig.add_bar(
                x=sub["mes_fmt"], y=sub["mrr_perdido"],
                name=label, marker_color=MODULE_COLORS[modulo],
            )
        fig.update_layout(
            barmode="stack",
            xaxis=dict(categoryorder="array", categoryarray=x_order_mod, type="category"),
        )
        st.plotly_chart(chart_layout(fig, legend_bottom=True), use_container_width=True)

with col_b:
    st.subheader("Clientes Perdidos por Módulo — Tendência")
    if df_plot_mod.empty:
        no_data()
    else:
        fig = go.Figure()
        for modulo, label in MODULE_LABELS.items():
            sub = df_plot_mod[df_plot_mod["modulo"] == modulo].sort_values("mes")
            if sub.empty:
                continue
            fig.add_scatter(
                x=sub["mes_fmt"], y=sub["clientes_desativados"],
                name=label, mode="lines+markers",
                line=dict(color=MODULE_COLORS[modulo], width=2.5),
                marker=dict(size=7),
            )
        fig.update_layout(
            xaxis=dict(categoryorder="array", categoryarray=x_order_mod, type="category"),
        )
        st.plotly_chart(chart_layout(fig, legend_bottom=True), use_container_width=True)

st.divider()

# ─────────────────────────────────────────────
# SEÇÃO 3 — Por Plano
# ─────────────────────────────────────────────
st.subheader("Por Plano")

if df_desativ_plano.empty:
    no_data("Nenhuma desativação de plano no período.")
else:
    col_c, col_d = st.columns(2)
    df_plot_plano, x_order_plano = mes_fmt_ordered(df_desativ_plano)

    with col_c:
        st.subheader("MRR Perdido por Plano (R$) — Empilhado")
        fig = go.Figure()
        for plano, label in PLAN_LABELS.items():
            sub = df_plot_plano[df_plot_plano["plano"] == plano].sort_values("mes")
            if sub.empty:
                continue
            fig.add_bar(
                x=sub["mes_fmt"], y=sub["mrr_perdido"],
                name=label, marker_color=PLAN_COLORS[plano],
            )
        fig.update_layout(
            barmode="stack",
            xaxis=dict(categoryorder="array", categoryarray=x_order_plano, type="category"),
        )
        st.plotly_chart(chart_layout(fig, legend_bottom=True), use_container_width=True)

    with col_d:
        st.subheader("Clientes Desativados por Plano — Tendência")
        fig = go.Figure()
        for plano, label in PLAN_LABELS.items():
            sub = df_plot_plano[df_plot_plano["plano"] == plano].sort_values("mes")
            if sub.empty:
                continue
            fig.add_scatter(
                x=sub["mes_fmt"], y=sub["clientes_desativados"],
                name=label, mode="lines+markers",
                line=dict(color=PLAN_COLORS[plano], width=2.5),
                marker=dict(size=7),
            )
        fig.update_layout(
            xaxis=dict(categoryorder="array", categoryarray=x_order_plano, type="category"),
        )
        st.plotly_chart(chart_layout(fig, legend_bottom=True), use_container_width=True)

st.divider()

# ─────────────────────────────────────────────
# SEÇÃO 4 — Churn % por Plano
# ─────────────────────────────────────────────
st.subheader("Churn por Plano")

if not df_desativ_plano.empty and not df_base_plano.empty:
    df_churn = df_base_plano.merge(
        df_desativ_plano[["mes", "plano", "clientes_desativados"]],
        on=["mes", "plano"], how="left"
    )
    df_churn["clientes_desativados"] = df_churn["clientes_desativados"].fillna(0)
    df_churn["churn_pct"] = (
        df_churn["clientes_desativados"] / df_churn["clientes_ativos"] * 100
    ).round(2)
    df_churn, x_order_churn = mes_fmt_ordered(df_churn)

    col_e, col_f = st.columns(2)

    with col_e:
        st.subheader("Churn % — Tendência por Plano")
        fig = go.Figure()
        for plano, label in PLAN_LABELS.items():
            sub = df_churn[df_churn["plano"] == plano].sort_values("mes")
            if sub.empty or sub["clientes_ativos"].sum() == 0:
                continue
            fig.add_scatter(
                x=sub["mes_fmt"], y=sub["churn_pct"],
                name=label, mode="lines+markers",
                line=dict(color=PLAN_COLORS[plano], width=2.5),
                marker=dict(size=7),
            )
        fig.update_layout(
            yaxis=dict(ticksuffix="%", range=[0, 20]),
            xaxis=dict(categoryorder="array", categoryarray=x_order_churn, type="category"),
        )
        st.plotly_chart(chart_layout(fig, legend_bottom=True), use_container_width=True)

    with col_f:
        st.subheader("Base vs Perdidos — Último Mês por Plano")
        last_mes = df_churn["mes"].max()
        df_last = df_churn[df_churn["mes"] == last_mes]
        fig = go.Figure()
        fig.add_bar(
            x=[PLAN_LABELS.get(p, p) for p in df_last["plano"]],
            y=df_last["clientes_ativos"],
            name="Base ativa", marker_color=PALETTE[4], opacity=0.8,
        )
        fig.add_bar(
            x=[PLAN_LABELS.get(p, p) for p in df_last["plano"]],
            y=df_last["clientes_desativados"],
            name="Desativados", marker_color=PALETTE[0],
        )
        fig.update_layout(barmode="group")
        st.plotly_chart(chart_layout(fig, legend_bottom=True), use_container_width=True)

    st.markdown("**Churn % por plano no último mês:**")
    cols_kpi = st.columns(len(PLAN_LABELS))
    for col_kpi, (plano, label) in zip(cols_kpi, PLAN_LABELS.items()):
        row = df_last[df_last["plano"] == plano]
        if row.empty or row["clientes_ativos"].values[0] == 0:
            churn_val = "—"
        else:
            churn_val = f"{row['churn_pct'].values[0]:.1f}%"
        with col_kpi:
            st.metric(label, churn_val)
else:
    no_data("Dados insuficientes para calcular churn.")

st.divider()

# ─────────────────────────────────────────────
# TABELA DETALHADA
# ─────────────────────────────────────────────
with st.expander("📋 Tabela Detalhada por Cliente"):
    df_det = load_desativacoes_detalhado()
    df_det = filter_months(df_det, n_months, "mes")
    if df_det.empty:
        no_data()
    else:
        df_show = (
            df_det
            .assign(mes=df_det["mes"].dt.strftime("%b/%y").str.capitalize())
            [["mes", "modulo", "plano", "nome_cliente", "produto", "mrr_perdido"]]
            .rename(columns={
                "mes":          "Mês",
                "modulo":       "Módulo",
                "plano":        "Plano",
                "nome_cliente": "Cliente",
                "produto":      "Produto",
                "mrr_perdido":  "MRR Perdido (R$)",
            })
        )
        st.dataframe(
            df_show,
            use_container_width=True,
            hide_index=True,
            column_config={
                "MRR Perdido (R$)": st.column_config.NumberColumn(format="R$ %.2f"),
            },
        )
