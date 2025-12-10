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
PATH_CROWLEY_RAW = os.path.join(DATA_FOLDER, "crowley_raw.parquet")
PATH_CROWLEY_OPT = os.path.join(DATA_FOLDER, "crowley_opt.parquet")

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

# --- ROTINA NUCLEAR DE LIMPEZA ---
def reset_environment(files_to_delete):
    """
    Remove arquivos físicos e força limpeza da RAM.
    Executada ANTES de qualquer tentativa de download.
    """
    log("☢️ INICIANDO RESET NUCLEAR DO AMBIENTE...")
    
    # 1. Força o Python a largar referências de memória
    gc.collect()
    
    # 2. Deleta arquivos físicos
    for file_path in files_to_delete:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                log(f"🗑️ Deletado: {file_path}")
            except OSError as e:
                log(f"⚠️ Erro ao deletar {file_path} (Arquivo preso?): {e}")
    
    # 3. Segunda rodada de limpeza de memória e espera o SO liberar disco
    gc.collect()
    time.sleep(1) # Pausa estratégica para o OS respirar
    log("✨ Ambiente limpo.")

# --- ETL OTIMIZADO ---
def optimize_crowley(raw_path, opt_path):
    try:
        log("⚙️ ETL: Convertendo arquivo RAW para OTIMIZADO...")
        
        # Lê o arquivo bruto
        df = pd.read_parquet(raw_path, engine='pyarrow')
        
        # 1. Categorias (Crucial para RAM)
        cols_cat = ["Praca", "Emissora", "Anunciante", "Anuncio", "Tipo", "DayPart"]
        for col in cols_cat:
            if col in df.columns: 
                df[col] = df[col].astype(str).astype("category") # Cast para str primeiro evita erros

        # 2. Numéricos
        cols_num = ["Volume de Insercoes", "Duracao"]
        for col in cols_num:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype("int32")

        # 3. Datas
        if "Data" in df.columns:
            df["Data_Dt"] = pd.to_datetime(df["Data"], dayfirst=True, errors="coerce")
            df.drop(columns=["Data"], inplace=True)
            
        # Salva a versão final
        df.to_parquet(opt_path, index=False)
        log(f"💾 Arquivo otimizado salvo: {opt_path}")
        
        del df
        gc.collect()
        return True
    except Exception as e:
        log(f"❌ Erro no ETL: {e}")
        return False

# --- DOWNLOADER ---
def download_file(service, file_id, dest_path):
    try:
        log(f"📥 Baixando do Drive para {dest_path}...")
        with open(dest_path, "wb") as f:
            request = service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        return True
    except Exception as e:
        log(f"❌ Erro Download: {e}")
        return False

# ==========================================
# LOADERS COM LOGICA NUCLEAR
# ==========================================

@st.cache_resource(ttl=180, show_spinner="Atualizando Vendas...")
def fetch_from_drive():
    log("🔄 Atualizando Vendas...")
    
    # 1. RESET (Apaga o antigo antes de pensar no novo)
    reset_environment([PATH_VENDAS])
    
    service = get_drive_service()
    if not service: return None, None
    file_id = st.secrets["drive_files"]["faturamento_xlsx"]
    
    # 2. DOWNLOAD
    if download_file(service, file_id, PATH_VENDAS):
        try:
            try: df_raw = pd.read_parquet(PATH_VENDAS)
            except: df_raw = pd.read_excel(PATH_VENDAS, engine="openpyxl")
            
            df = normalize_dataframe(df_raw)
            
            # Data Ref
            ultima = "N/A"
            if "data_ref" in df.columns:
                m = df["data_ref"].max()
                if pd.notna(m): ultima = m.strftime("%m/%Y")
            
            del df_raw
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
    log("🚨 TIMER CROWLEY EXPIRADO: Iniciando Sequência Nuclear...")
    
    # ==========================================================
    # PASSO 1: TERRA ARRASADA
    # Apaga tanto o RAW quanto o OTIMIZADO antes de começar.
    # Garante que temos zero consumo de disco/cache relacionado a base antiga.
    # ==========================================================
    reset_environment([PATH_CROWLEY_RAW, PATH_CROWLEY_OPT])
    
    service = get_drive_service()
    if not service: return None, "Erro Conexão"

    file_id = st.secrets["drive_files"]["crowley_parquet"]
    
    # ==========================================================
    # PASSO 2: DOWNLOAD DO NOVO ARQUIVO BRUTO
    # ==========================================================
    if not download_file(service, file_id, PATH_CROWLEY_RAW):
        return None, "Erro Download"
        
    # ==========================================================
    # PASSO 3: OTIMIZAÇÃO (RAW -> OPT)
    # Transforma o arquivo pesado em leve, salva e apaga o pesado.
    # ==========================================================
    success = optimize_crowley(PATH_CROWLEY_RAW, PATH_CROWLEY_OPT)
    
    # Limpa o RAW imediatamente após o uso
    if os.path.exists(PATH_CROWLEY_RAW):
        os.remove(PATH_CROWLEY_RAW)
        log("🗑️ Arquivo RAW removido.")
    
    if not success:
        return None, "Erro Processamento"

    # ==========================================================
    # PASSO 4: LEITURA FINAL (Memory Map)
    # Só agora o Streamlit "vê" os dados.
    # ==========================================================
    try:
        log("📖 Lendo arquivo Otimizado...")
        gc.collect()
        
        # Leitura limpa
        df = pd.read_parquet(PATH_CROWLEY_OPT, memory_map=True)
        
        # Pega a data
        ultima = "N/A"
        try:
            if "Data_Dt" in df.columns:
                 m = df["Data_Dt"].max()
                 if pd.notna(m): ultima = m.strftime("%d/%m/%Y")
            elif "Data" in df.columns: # Fallback
                 # Tenta converter só pra pegar o max sem salvar na memória
                 m = pd.to_datetime(df["Data"], dayfirst=True, errors="coerce").max()
                 if pd.notna(m): ultima = m.strftime("%d/%m/%Y")
        except: pass

        if ultima == "N/A":
             ts = os.path.getmtime(PATH_CROWLEY_OPT)
             ultima = datetime.fromtimestamp(ts).strftime("%d/%m/%Y")

        log(f"✅ Sucesso! {len(df)} linhas carregadas.")
        return df, ultima

    except Exception as e:
        log(f"❌ Erro Leitura Final: {e}")
        # Se falhar aqui, apaga o otimizado também para não ficar lixo
        if os.path.exists(PATH_CROWLEY_OPT): os.remove(PATH_CROWLEY_OPT)
        return None, "Erro Leitura"
