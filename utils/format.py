# utils/format.py
import pandas as pd
import re
import streamlit as st
import numpy as np

PALETTE = ["#007dc3", "#00a8e0", "#7ad1e6", "#004b8d", "#0095d9"]

# Mapeamento de sinônimos para colunas (Normalização)
COLUMN_ALIASES = {
    # Data
    "ref.": "data_ref", "ref": "data_ref", "competencia": "data_ref", "competência": "data_ref", "data": "data_ref", "mês/ano": "data_ref",
    # Cliente
    "descrição": "Cliente", "descricao": "Cliente", "cliente": "Cliente", "agência": "Agencia", "agencia": "Agencia", "nome fantasia": "Cliente", "razao social": "Cliente",
    # Emissora
    "empresa": "Emissora", "veículo": "Emissora", "veiculo": "Emissora", "radio": "Emissora", "rádio": "Emissora", "emissora": "Emissora",
    # Executivo
    "contato coml.": "Executivo", "contato coml": "Executivo", "vendedor": "Executivo", "executivo": "Executivo", "contato": "Executivo",
    # Faturamento
    "valor": "Faturamento", "venda": "Faturamento", "faturamento": "Faturamento", "vlr total": "Faturamento", "valor líquido": "Faturamento", "valor liquido": "Faturamento",
    # Inserções
    "inserções": "Insercoes", "insercoes": "Insercoes", "inserts": "Insercoes", "qtd": "Insercoes"
}

def brl(valor):
    """Formata número para Real (R$)."""
    try:
        if pd.isna(valor): return "—"
        return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception: return str(valor)

def parse_currency_br(valor):
    """Converte string monetária BR ou suja para float de forma robusta."""
    if pd.isna(valor) or str(valor).strip() == "": return 0.0
    if isinstance(valor, (int, float)): return float(valor)
    
    s = str(valor).strip().replace("R$", "").replace(" ", "").replace("\u00a0", "")
    neg = s.startswith("-") or s.startswith("(")
    s = re.sub(r"[\(\)]", "", s)
    s = s.replace(".", "").replace(",", ".")
    
    try:
        v = float(s)
        return -v if neg and v > 0 else v
    except Exception:
        return 0.0

def normalize_text(texto):
    """Normaliza nomes para Título (Primeira Letra Maiúscula) mantendo siglas."""
    if pd.isna(texto): return ""
    texto = str(texto).strip()
    if texto == "": return ""
    if texto.isupper() and len(texto) <= 3:
        return texto
    return " ".join(p.capitalize() for p in texto.split())

def consolidate_executives(name):
    """
    Padroniza nomes de executivos e filtra Vendas Externas.
    """
    if not isinstance(name, str): return name
    name_upper = name.upper()
    
    # REGRA ATUALIZADA: Vendas Externas são removidas e viram N/A
    if "VENDA EXTERNA" in name_upper: return None 

    # Regras de Aglomeração
    if "EDUARDO" in name_upper: return "Eduardo Notomi"
    if "JULIA" in name_upper: return "Julia Bergo"
    if "OLGA" in name_upper: return "Olga Luiza"
    if "WALNER" in name_upper: return "Walner Francisco"
    
    return name

@st.cache_data(ttl=600)
def normalize_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Normaliza estrutura de planilhas de vendas (Novabrasil) com alias robustos."""
    df = df_raw.copy()
    
    # 1. Renomear colunas
    new_cols = {}
    for col in df.columns:
        col_lower = str(col).strip().lower()
        if col_lower in COLUMN_ALIASES:
            new_cols[col] = COLUMN_ALIASES[col_lower]
        else:
            new_cols[col] = col
            
    df = df.rename(columns=new_cols)

    # 2. Garante colunas básicas
    required = ["Emissora", "Cliente", "Executivo", "Faturamento"]
    for col in required:
        if col not in df.columns:
            df[col] = "" 

    # 3. Normaliza Textos (Capitalização)
    for col in ["Emissora", "Cliente", "Executivo"]:
        df[col] = df[col].apply(normalize_text)

    # 4. Consolidação de Executivos (Aglomeração e Filtro)
    df["Executivo"] = df["Executivo"].apply(consolidate_executives)
    
    # Substitui vazios e None (incluindo as Vendas Externas removidas) por "N/A"
    df["Executivo"] = df["Executivo"].replace(["", "nan", "None"], np.nan).fillna("N/A")

    # 5. Detecção e Conversão de Datas
    if "data_ref" in df.columns:
        df["data_ref"] = df["data_ref"].astype(str).str.strip().str.replace("'", "", regex=False)
        
        def try_parse_date(val):
            if not isinstance(val, str): return pd.to_datetime(val, errors="coerce")
            if re.match(r"^\d{4}-\d{2}-\d{2}$", val): return pd.to_datetime(val, format="%Y-%m-%d", errors="coerce")
            if re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", val): return pd.to_datetime(val, dayfirst=True, errors="coerce")
            if re.match(r"^\d{1,2}/\d{4}$", val): return pd.to_datetime("01/" + val, dayfirst=True, errors="coerce")
            if val.replace(".", "").isdigit() and len(val) >= 4:
                try: return pd.to_datetime(float(val), unit="D", origin="1899-12-30")
                except: pass
            return pd.to_datetime(val, errors="coerce")

        df["data_ref"] = df["data_ref"].apply(try_parse_date)

    elif "Ano" in df.columns and "Mês" in df.columns:
        df["data_ref"] = pd.to_datetime(dict(year=df["Ano"], month=df["Mês"], day=1), errors="coerce")

    df = df.dropna(subset=["data_ref"])
    
    if df.empty:
        return pd.DataFrame()

    # 6. Colunas derivadas de tempo
    df["Ano"] = df["data_ref"].dt.year
    df["Mes"] = df["data_ref"].dt.month
    df["MesLabel"] = df["data_ref"].dt.strftime("%b/%y")

    # 7. Faturamento
    df["Faturamento"] = df["Faturamento"].apply(parse_currency_br)

    # 8. Tratamento de Inserções e Custo Unitário
    if "Insercoes" in df.columns:
        df["Insercoes"] = pd.to_numeric(df["Insercoes"], errors='coerce')
        insercoes_para_custo = df["Insercoes"].fillna(1).replace(0, 1)
        df["Custo_Unitario"] = df["Faturamento"] / insercoes_para_custo
    else:
        df["Insercoes"] = np.nan
        df["Custo_Unitario"] = df["Faturamento"]

    df.columns = df.columns.map(str)
    df = df.reset_index(drop=True)

    return df