# streamlit_erp_produtos.py
import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import urllib
import pyodbc

# carrega .env (procura por .env na raiz do projeto)
load_dotenv()

st.set_page_config(page_title="Produtos - Embalagens e Tabelas", layout="wide")

# ---------- CONFIGURAÇÃO DB (via .env, com fallback) ----------
DB_HOST = os.getenv("DB_HOST", "adms3.costafaria.ind.br")
DB_PORT = int(os.getenv("DB_PORT", 1433))
DB_NAME = os.getenv("DB_NAME", "erp")
DB_USER = os.getenv("DB_USER", "acessoext_mineirinho")
DB_PASS = os.getenv("DB_PASS", "")  # deixe vazio no .env se quiser inserir depois

PREFERRED_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "SQL Server"
]

def choose_driver():
    installed = pyodbc.drivers()
    for pref in PREFERRED_DRIVERS:
        if pref in installed:
            return pref
    for d in installed[::-1]:
        if "ODBC Driver" in d:
            return d
    if installed:
        return installed[0]
    raise RuntimeError("Nenhum driver ODBC encontrado no sistema. Instale ODBC Driver 17/18 para SQL Server.")

@st.cache_resource
def get_sql_engine(host, port, database, user, password):
    driver = choose_driver()
    extra = ""
    if "ODBC Driver 18" in driver:
        extra = "Encrypt=no;TrustServerCertificate=yes;"
    odbc_str = f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};UID={user};PWD={password};{extra}"
    params = urllib.parse.quote_plus(odbc_str)
    url = f"mssql+pyodbc:///?odbc_connect={params}"
    engine = create_engine(url, fast_executemany=True)
    return engine, driver

# use leading underscore so Streamlit doesn't try to hash the Engine object
@st.cache_data(ttl=300)
def read_table(_engine, table_name):
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql_query(query, _engine)

# ---------- UTILITÁRIOS DE FORMATAÇÃO ----------
def parse_number_to_float(val):
    if pd.isna(val):
        return np.nan
    if isinstance(val, (int, float, np.integer, np.floating)):
        return float(val)
    s = str(val).strip()
    if s == "":
        return np.nan
    s = s.replace('\xa0', '').replace(' ', '')
    if '.' in s and ',' in s:
        s = s.replace('.', '').replace(',', '.')
    else:
        if ',' in s and s.count(',') == 1 and '.' not in s:
            s = s.replace(',', '.')
        else:
            s = s.replace(',', '')
    try:
        return float(s)
    except Exception:
        return np.nan

def format_brl(value):
    if value is None:
        return pd.NA
    try:
        v = float(value)
    except Exception:
        return pd.NA
    if np.isnan(v):
        return pd.NA
    s = f"{v:,.2f}"
    s = s.replace(',', 'X').replace('.', ',').replace('X', '.')
    return f"R$ {s}"

# ---------- LER DADOS ----------
try:
    engine, used_driver = get_sql_engine(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
    with engine.connect():
        df_t075 = read_table(engine, "erp.dbo.usu_t075pro")
        df_t081 = read_table(engine, "erp.dbo.usu_t081itp")
except Exception as e:
    st.error("Erro ao conectar/ler o banco de dados. Verifique credenciais, driver ODBC e conexão de rede.")
    st.exception(e)
    try:
        st.info(f"Drivers ODBC instalados: {pyodbc.drivers()}")
    except Exception:
        pass
    st.stop()

# ---------- NORMALIZA COLUNAS ----------
df_t075.columns = [c.lower() for c in df_t075.columns]
df_t081.columns = [c.lower() for c in df_t081.columns]

# ---------- UI / FILTROS ----------
st.title("Tabelas de Preço - ERP")
st.markdown(f"Conectado usando driver: **{used_driver}**.")

st.sidebar.header("Filtros")
tabelas_disponiveis = sorted(df_t081['usu_codtpr'].dropna().unique())
tabela_selecionada = st.sidebar.selectbox("Escolha tabela de preço (usu_codtpr)", options=["Todas"] + list(tabelas_disponiveis))
produto_filtro = st.sidebar.text_input("Filtrar por código do produto (parte do código) — vazio = todos")
mostrar_sem_preco = st.sidebar.checkbox("Incluir produtos mesmo sem preço na tabela", value=False)

# aplica filtro de produto na tabela de embalagens (t075)
df_packages = df_t075.copy()
if produto_filtro:
    df_packages = df_packages[df_packages['usu_codpro'].str.contains(produto_filtro, case=False, na=False)]

# Para cada tabela (usu_codtpr) vamos montar um card com a tabela pedida
def build_table_for_codtpr(codtpr, df_t075_local, df_t081_local):
    df_prices = df_t081_local[df_t081_local['usu_codtpr'] == codtpr].copy()
    if 'usu_datini' in df_prices.columns:
        df_prices['usu_datini'] = pd.to_datetime(df_prices['usu_datini'], errors='coerce')

    if not df_prices.empty and 'usu_codpro' in df_prices.columns:
        if 'usu_datini' in df_prices.columns and not df_prices['usu_datini'].isna().all():
            idx = df_prices.groupby('usu_codpro')['usu_datini'].idxmax()
            idx = idx.dropna().astype(int)
            df_latest = df_prices.loc[idx].copy()
        else:
            df_latest = df_prices.sort_values('usu_codpro').drop_duplicates(subset=['usu_codpro'], keep='first').copy()
    else:
        df_latest = pd.DataFrame(columns=df_prices.columns)

    df_pack_unique = df_t075_local.sort_values(['usu_codpro']).drop_duplicates(subset=['usu_codpro'], keep='first').copy()

    df_pack_unique = df_pack_unique.rename(columns={
        'usu_codpro': 'codigo_produto',
        'usu_qtdpct': 'qtde_pacotes_na_caixa',
        'usu_kgpct': 'kg_pacote'
    })[['codigo_produto', 'qtde_pacotes_na_caixa', 'kg_pacote']]

    if not df_latest.empty:
        df_latest = df_latest.rename(columns={
            'usu_codpro': 'codigo_produto',
            'usu_prebas': 'preco_pacote'
        })[['codigo_produto', 'preco_pacote']]
    else:
        df_latest = pd.DataFrame(columns=['codigo_produto', 'preco_pacote'])

    if mostrar_sem_preco:
        df_merged = pd.merge(df_pack_unique, df_latest, on='codigo_produto', how='left')
    else:
        df_merged = pd.merge(df_pack_unique, df_latest, on='codigo_produto', how='inner')

    # conversões numéricas e cálculo de preco_caixa
    if 'qtde_pacotes_na_caixa' in df_merged.columns:
        df_merged['qtde_pacotes_na_caixa_num'] = df_merged['qtde_pacotes_na_caixa'].apply(parse_number_to_float)
    else:
        df_merged['qtde_pacotes_na_caixa_num'] = np.nan

    if 'preco_pacote' in df_merged.columns:
        df_merged['preco_pacote_num_raw'] = df_merged['preco_pacote'].apply(parse_number_to_float)
    else:
        df_merged['preco_pacote_num_raw'] = np.nan

    # tratar 9.999,99 como placeholder apenas no cálculo
    def _raw_to_calc(x):
        if pd.isna(x):
            return np.nan
        if abs(float(x) - 9999.99) < 1e-6:
            return 0.0
        return float(x)

    df_merged['preco_pacote_num_for_calc'] = df_merged['preco_pacote_num_raw'].apply(_raw_to_calc)
    df_merged['preco_caixa_num'] = df_merged['preco_pacote_num_for_calc'] * df_merged['qtde_pacotes_na_caixa_num']

    # formata colunas monetárias para exibição (preco_pacote exibido com valor raw)
    df_merged['preco_pacote'] = df_merged['preco_pacote_num_raw'].apply(format_brl)
    df_merged['preco_caixa'] = df_merged['preco_caixa_num'].apply(format_brl)

    df_merged = df_merged.sort_values('codigo_produto').reset_index(drop=True)
    cols_order = ['codigo_produto', 'preco_pacote', 'qtde_pacotes_na_caixa', 'preco_caixa', 'kg_pacote']
    for c in cols_order:
        if c not in df_merged.columns:
            df_merged[c] = pd.NA
    df_merged = df_merged[cols_order]

    return df_merged

# EXIBIÇÃO
tables_to_show = tabelas_disponiveis if tabela_selecionada == "Todas" else [tabela_selecionada]

for codtpr in tables_to_show:
    df_card = build_table_for_codtpr(codtpr, df_packages, df_t081)
    if df_card.empty:
        st.info(f"Tabela {codtpr}: sem produtos para exibir com os filtros aplicados.")
        continue

    with st.container():
        st.markdown(f"## Tabela: **{codtpr}**")
        st.dataframe(df_card.reset_index(drop=True), width="stretch")

        csv_bytes = df_card.to_csv(index=False, sep=';').encode('utf-8-sig')
        st.download_button(
            label=f"Baixar - {codtpr}.csv",
            data=csv_bytes,
            file_name=f"{codtpr}.csv",
            mime="text/csv"
        )
