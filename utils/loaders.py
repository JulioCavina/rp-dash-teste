# utils/loaders.py
import os
import gc
import time
import pandas as pd
import streamlit as st
import pyarrow.parquet as pq
from datetime import datetime, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from .format import normalize_dataframe

# --- CONFIGURAÇÃO ---
DATA_FOLDER = "data"
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

PATH_VENDAS = os.path.join(DATA_FOLDER, "vendas.parquet")
PATH_CROWLEY = os.path.join(DATA_FOLDER, "crowley.parquet") # Um único arquivo

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")

# --- AUTH DRIVE ---
def get_drive_service():
    if "gcp_service_account" not in st.secrets or "drive_files" not in st.secrets:
        st.error("❌ Erro: Secrets não configurados.")
        return None
    try:
        service_account_info = dict(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        st.error(f"Erro Auth Drive: {e}")
        return None

# --- LIMPEZA DE AMBIENTE ---
def nuke_environment(files_to_delete):
    """Apaga arquivos e força o Garbage Collector do Python."""
    log("☢️ INICIANDO LIMPEZA DE MEMÓRIA E DISCO...")
    gc.collect()
    
    for file_path in files_to_delete:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                log(f"🗑️ Deletado: {file_path}")
            except Exception as e:
                log(f"⚠️ Erro ao deletar {file_path}: {e}")
    
    time.sleep(1) # Deixa o SO respirar
    gc.collect()

# --- DOWNLOADER SIMPLES ---
def download_file(service, file_id, dest_path):
    try:
        log(f"📥 Baixando arquivo para: {dest_path}")
        with open(dest_path, "wb") as f:
            request = service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        log("✅ Download concluído.")
        return True
    except Exception as e:
        log(f"❌ Erro Download: {e}")
        return False

# ==========================================
# LOADERS
# ==========================================

@st.cache_resource(ttl=180, show_spinner="Atualizando Vendas...")
def fetch_from_drive():
    log("🔄 Atualizando Vendas...")
    nuke_environment([PATH_VENDAS])
    
    service = get_drive_service()
    if not service: return None, None
    file_id = st.secrets["drive_files"]["faturamento_xlsx"]
    
    if download_file(service, file_id, PATH_VENDAS):
        try:
            try: df = pd.read_parquet(PATH_VENDAS)
            except: df = pd.read_excel(PATH_VENDAS, engine="openpyxl")
            
            df = normalize_dataframe(df)
            
            # Data Ref
            ultima = "N/A"
            if "data_ref" in df.columns:
                m = df["data_ref"].max()
                if pd.notna(m): ultima = m.strftime("%m/%Y")
            
            gc.collect()
            return df, ultima
        except Exception as e:
            log(f"Erro leitura Vendas: {e}")
            return None, None
    return None, None

def load_main_base():
    if "uploaded_dataframe" in st.session_state and st.session_state.uploaded_dataframe is not None:
        return st.session_state.uploaded_dataframe, st.session_state.get("uploaded_timestamp", "Upload Manual")
    return fetch_from_drive()


# --- CROWLEY (CRÍTICO) ---
@st.cache_resource(ttl=180, show_spinner="Atualizando Crowley...")
def load_crowley_base():
    log("🚨 TIMER CROWLEY: Iniciando rotina leve...")
    
    # 1. LIMPEZA TOTAL (Remove arquivo anterior para garantir espaço)
    nuke_environment([PATH_CROWLEY])
    
    service = get_drive_service()
    if not service: return None, "Erro Conexão"

    file_id = st.secrets["drive_files"]["crowley_parquet"]
    
    # 2. DOWNLOAD DIRETO
    if not download_file(service, file_id, PATH_CROWLEY):
        return None, "Erro Download"

    # 3. LEITURA DIRETA (Sem reescrita/ETL pesado)
    try:
        log("📖 Lendo arquivo com Memory Map...")
        gc.collect()
        
        # LEITURA OTIMIZADA: Tenta ler apenas as colunas que importam para reduzir largura
        # Se der erro (coluna nova mudou de nome), lê tudo (fallback)
        try:
            cols_to_load = [
                "Data", "Praca", "Emissora", "Anunciante", "Anuncio", 
                "Tipo", "DayPart", "Volume de Insercoes", "Duracao"
            ]
            # Valida schema para não crashar se faltar coluna
            pq_file = pq.ParquetFile(PATH_CROWLEY)
            existing_cols = [c for c in cols_to_load if c in pq_file.schema.names]
            
            df = pd.read_parquet(PATH_CROWLEY, columns=existing_cols, memory_map=True)
            log(f"✅ Leitura parcial de colunas: {len(existing_cols)} colunas carregadas.")
        except:
            log("⚠️ Fallback: Lendo todas as colunas...")
            df = pd.read_parquet(PATH_CROWLEY, memory_map=True)

        # 4. CONVERSÃO LEVE (In-Place e Opcional)
        # Fazemos a conversão apenas na memória, SEM salvar no disco de volta
        log("⚙️ Ajustando tipos em memória...")
        
        cat_cols = ["Praca", "Emissora", "Anunciante", "Anuncio", "Tipo", "DayPart"]
        for col in cat_cols:
            if col in df.columns:
                # Converte para categoria para economizar RAM durante o uso do app
                df[col] = df[col].astype("category")

        # Ajuste de Datas
        ultima = "N/A"
        if "Data" in df.columns:
            # Converte data mas mantém erros como NaT para não crashar
            df["Data_Dt"] = pd.to_datetime(df["Data"], dayfirst=True, errors="coerce")
            
            # Tenta pegar max data
            try:
                m = df["Data_Dt"].max()
                if pd.notna(m): ultima = m.strftime("%d/%m/%Y")
            except: pass
            
            # Opcional: Remover coluna original texto para liberar RAM
            # df.drop(columns=["Data"], inplace=True) 

        if ultima == "N/A":
             ts = os.path.getmtime(PATH_CROWLEY)
             ultima = datetime.fromtimestamp(ts).strftime("%d/%m/%Y")

        log(f"🚀 Base pronta! ({len(df)} linhas)")
        return df, ultima

    except Exception as e:
        log(f"❌ Erro Leitura: {e}")
        # Se falhar, apaga para tentar limpo na próxima
        if os.path.exists(PATH_CROWLEY): os.remove(PATH_CROWLEY)
        return None, "Erro Leitura"
