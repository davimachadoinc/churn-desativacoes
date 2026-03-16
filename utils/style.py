"""
utils/style.py
Design system InChurch — Premium Brand Dark Mode.
Chamar inject_css() no início de cada página.
"""
import streamlit as st

# ─────────────────────────────────────────────
# FONTES
# ─────────────────────────────────────────────
_FONTS = """
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
"""

# ─────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────
_CSS = """
<style>
/* ── Variáveis ────────────────────────────── */
:root {
  --bg-main:    #000000;
  --bg-card:    #121212;
  --bg-hover:   #1E1E1E;
  --border:     #292929;
  --border-hi:  #4c4c4c;
  --accent-1:   #6eda2c;
  --accent-2:   #57d124;
  --text-main:  #ffffff;
  --text-muted: #a0a0a0;
  --font-base:  'Outfit', sans-serif;
}

/* ── Fonte global ─────────────────────────── */
html, body, [class*="css"], .stApp, button, input, select, textarea {
  font-family: var(--font-base) !important;
}

/* ── Fundo principal ──────────────────────── */
.stApp { background: var(--bg-main) !important; }
section[data-testid="stSidebar"] { background: var(--bg-card) !important; border-right: 1px solid var(--border); }

/* ── Títulos de página (h1) ───────────────── */
h1 {
  font-size: 2rem !important;
  font-weight: 700 !important;
  color: var(--text-main) !important;
  margin-bottom: 4px !important;
  letter-spacing: -0.02em;
}
h1 span { color: var(--accent-1) !important; }

/* ── Subtítulos de seção (h2/h3) ─────────── */
h2, h3 {
  font-family: var(--font-base) !important;
  font-size: 1.1rem !important;
  font-weight: 600 !important;
  color: var(--text-main) !important;
  border-bottom: 1px solid var(--border);
  border-left: 3px solid var(--accent-1);
  padding: 0 0 10px 12px !important;
  margin: 18px 0 16px 0 !important;
}

/* ── Metric Cards ─────────────────────────── */
[data-testid="stMetric"] {
  background: var(--bg-card) !important;
  border: 1px solid var(--border) !important;
  border-bottom: 3px solid var(--accent-2) !important;
  border-radius: 12px !important;
  padding: 20px 20px !important;
  min-height: 130px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  transition: all 0.3s cubic-bezier(0.16, 1, 0.3, 1);
}
[data-testid="stMetric"]:hover {
  border-color: var(--accent-1) !important;
  transform: translateY(-4px);
  box-shadow: 0 12px 24px rgba(110, 218, 44, 0.12);
}
[data-testid="stMetricLabel"] > div {
  font-size: 0.8rem !important;
  font-weight: 500 !important;
  color: var(--text-muted) !important;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
[data-testid="stMetricValue"] > div {
  font-size: 2.1rem !important;
  font-weight: 700 !important;
  color: var(--accent-1) !important;
  line-height: 1.15 !important;
}
[data-testid="stMetricDelta"] > div {
  font-size: 0.82rem !important;
  font-weight: 500 !important;
}

/* ── Tabs ─────────────────────────────────── */
div[data-baseweb="tab-list"] {
  background: var(--bg-card) !important;
  border-radius: 12px !important;
  padding: 5px !important;
  border: 1px solid var(--border) !important;
  gap: 4px !important;
}
button[data-baseweb="tab"] {
  background: transparent !important;
  border-radius: 8px !important;
  color: var(--text-muted) !important;
  font-weight: 500 !important;
  font-size: 0.88rem !important;
  padding: 8px 18px !important;
  transition: all 0.2s ease;
}
button[data-baseweb="tab"]:hover {
  background: rgba(110, 218, 44, 0.06) !important;
  color: var(--text-main) !important;
}
button[data-baseweb="tab"][aria-selected="true"] {
  background: rgba(110, 218, 44, 0.12) !important;
  border: 1px solid rgba(110, 218, 44, 0.28) !important;
  color: var(--accent-1) !important;
  font-weight: 600 !important;
}
div[data-testid="stTabPanel"] {
  padding-top: 16px !important;
}
div[data-baseweb="tab-highlight"],
div[data-baseweb="tab-border"] { display: none !important; }

/* ── Selectbox / Filtros ──────────────────── */
div[data-baseweb="select"] > div {
  background: var(--bg-card) !important;
  border: 1px solid var(--border) !important;
  border-radius: 10px !important;
  min-height: 44px !important;
  color: var(--text-main) !important;
  transition: border-color 0.2s ease, box-shadow 0.2s ease;
}
div[data-baseweb="select"] > div:hover {
  border-color: var(--accent-1) !important;
  box-shadow: 0 4px 12px rgba(110, 218, 44, 0.1) !important;
}

/* ── Toggle / Checkbox ────────────────────── */
label[data-baseweb="checkbox"] span { color: var(--text-main) !important; }

/* ── Divider ──────────────────────────────── */
hr { border-color: var(--border) !important; margin: 20px 0 !important; }

/* ── Scrollbar ────────────────────────────── */
::-webkit-scrollbar { width: 7px; height: 7px; }
::-webkit-scrollbar-track { background: var(--bg-main); }
::-webkit-scrollbar-thumb { background: #4c4c4c; border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: #6c6c6c; }

/* ── Dataframe ────────────────────────────── */
[data-testid="stDataFrame"] {
  border: 1px solid var(--border) !important;
  border-radius: 10px !important;
  overflow: hidden !important;
}

/* ── Info/Warning/Error boxes ─────────────── */
div[data-testid="stAlert"] {
  background: var(--bg-card) !important;
  border-radius: 10px !important;
  border: 1px solid var(--border) !important;
}

/* ── Toggle switch ────────────────────────── */
div[data-testid="stToggle"] label { color: var(--text-main) !important; font-size: 0.9rem !important; }

/* ── Top padding reduction ────────────────── */
div[data-testid="stAppViewBlockContainer"] { padding-top: 1.5rem !important; }
</style>
"""

_JS = """
<script>
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('svg title').forEach(el => el.remove());
});
</script>
"""


def inject_css() -> None:
    st.markdown(_FONTS, unsafe_allow_html=True)
    st.markdown(_CSS, unsafe_allow_html=True)
    st.markdown(_JS, unsafe_allow_html=True)
