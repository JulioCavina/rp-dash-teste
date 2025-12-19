# utils/loaders.py
import os
import gc
import time
import pandas as pd
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from .format import normalize_dataframe

# --- CONFIGURAÇÃO ---
DATA_FOLDER = "data"
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

PATH_VENDAS = os.path.join(DATA_FOLDER, "vendas.parquet")

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

# --- ROTINA DESTRUTIVA ---
def nuke_and_prepare(files_list):
    """
    Remove arquivos e limpa memória agressivamente ANTES do download.
    """
    # 1. Limpeza de RAM preliminar
    gc.collect()
    
    # 2. Remoção de arquivos
    for f in files_list:
        if os.path.exists(f):
            try:
                os.remove(f)
            except Exception:
                pass
    
    # 3. Pausa para o Sistema Operacional liberar os handles
    time.sleep(1)
    gc.collect()

# --- DOWNLOADER ---
def download_file(service, file_id, dest_path):
    try:
        with open(dest_path, "wb") as f:
            request = service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        return True
    except Exception:
        return False

# ==========================================
# LOADERS
# ==========================================

@st.cache_resource(ttl=3600, show_spinner="Atualizando Vendas...")
def fetch_from_drive():
    nuke_and_prepare([PATH_VENDAS])
    
    service = get_drive_service()
    if not service: return None, None
    file_id = st.secrets["drive_files"]["faturamento_xlsx"]
    
    if download_file(service, file_id, PATH_VENDAS):
        try:
            try: df = pd.read_parquet(PATH_VENDAS)
            except: df = pd.read_excel(PATH_VENDAS, engine="openpyxl")
            
            df = normalize_dataframe(df)
            
            ultima = "N/A"
            if "data_ref" in df.columns:
                m = df["data_ref"].max()
                if pd.notna(m): ultima = m.strftime("%m/%Y")
            
            gc.collect()
            return df, ultima
        except Exception:
            return None, None
    return None, None

def load_main_base():
    if "uploaded_dataframe" in st.session_state and st.session_state.uploaded_dataframe is not None:
        return st.session_state.uploaded_dataframe, st.session_state.get("uploaded_timestamp", "Upload Manual")
    return fetch_from_drive()