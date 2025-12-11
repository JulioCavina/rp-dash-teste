# utils/loaders.py
import os
import gc
import time
import pandas as pd
import streamlit as st
import pyarrow.parquet as pq
import pyarrow as pa
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
PATH_CROWLEY = os.path.join(DATA_FOLDER, "crowley.parquet")

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

# --- ROTINA DESTRUTIVA ---
def nuke_and_prepare(files_list):
    """
    Remove arquivos e limpa memória agressivamente ANTES do download.
    Isso garante que o disco e a RAM estejam o mais livres possível.
    """
    log("☢️ NUCLEAR: Iniciando limpeza de ambiente...")
    
    # 1. Limpeza de RAM preliminar
    gc.collect()
    
    # 2. Remoção de arquivos
    for f in files_list:
        if os.path.exists(f):
            try:
                os.remove(f)
                log(f"🗑️ Deletado: {f}")
            except Exception as e:
                log(f"⚠️ Falha ao deletar {f}: {e}")
    
    # 3. Pausa para o Sistema Operacional liberar os handles
    time.sleep(1)
    gc.collect()
    log("✨ Ambiente limpo.")

# --- DOWNLOADER ---
def download_file(service, file_id, dest_path):
    try:
        log(f"📥 Baixando arquivo novo...")
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

@st.cache_resource(ttl=3600, show_spinner="Atualizando Vendas...")
def fetch_from_drive():
    log("🔄 Vendas: Iniciando refresh...")
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
        except Exception as e:
            log(f"Erro Vendas: {e}")
            return None, None
    return None, None

def load_main_base():
    if "uploaded_dataframe" in st.session_state and st.session_state.uploaded_dataframe is not None:
        return st.session_state.uploaded_dataframe, st.session_state.get("uploaded_timestamp", "Upload Manual")
    return fetch_from_drive()


# --- CROWLEY (AQUI ESTÁ A CORREÇÃO DE MEMÓRIA) ---
@st.cache_resource(ttl=180, show_spinner="Atualizando Crowley...")
def load_crowley_base():
    log("🚨 CROWLEY: Cache expirado. Executando protocolo de atualização...")
    
    # 1. DELETA TUDO ANTES
    nuke_and_prepare([PATH_CROWLEY])
    
    service = get_drive_service()
    if not service: return None, "Erro Conexão"

    file_id = st.secrets["drive_files"]["crowley_parquet"]
    
    # 2. BAIXA O ARQUIVO NOVO
    if not download_file(service, file_id, PATH_CROWLEY):
        return None, "Erro Download"

    # 3. LEITURA "ZERO-COPY" (SELF DESTRUCT)
    # Esta é a única maneira de evitar o pico de RAM na conversão Parquet -> Pandas
    try:
        log("📖 Convertendo PyArrow -> Pandas (Modo Self-Destruct)...")
        gc.collect()

        # Lê como tabela PyArrow primeiro (gerencia memória melhor que Pandas direto)
        # memory_map=True usa o disco como extensão da RAM
        arrow_table = pq.read_table(PATH_CROWLEY, memory_map=True)
        
        # O SEGRED0: self_destruct=True
        # Libera a memória do PyArrow À MEDIDA que cria o Pandas.
        # Evita ter 2x o tamanho do arquivo na RAM.
        df = arrow_table.to_pandas(self_destruct=True, split_blocks=True)
        
        # Limpa o objeto arrow imediatamente
        del arrow_table
        gc.collect()
        
        # 4. OTIMIZAÇÃO DE TIPOS IN-PLACE
        log("⚙️ Otimizando tipos...")
        
        # Categorias
        cat_cols = ["Praca", "Emissora", "Anunciante", "Anuncio", "Tipo", "DayPart"]
        for col in cat_cols:
            if col in df.columns:
                df[col] = df[col].astype("category")

        # Numéricos
        num_cols = ["Volume de Insercoes", "Duracao"]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype("int32")

        # Datas
        ultima = "N/A"
        if "Data" in df.columns:
            # Converte data direto
            df["Data_Dt"] = pd.to_datetime(df["Data"], dayfirst=True, errors="coerce")
            
            # Tenta pegar max data
            try:
                m = df["Data_Dt"].max()
                if pd.notna(m): ultima = m.strftime("%d/%m/%Y")
            except: pass
            
            # Remove a coluna de texto original para liberar RAM
            df.drop(columns=["Data"], inplace=True) 

        if ultima == "N/A":
             ts = os.path.getmtime(PATH_CROWLEY)
             ultima = datetime.fromtimestamp(ts).strftime("%d/%m/%Y")

        log(f"✅ Base Carregada! Linhas: {len(df)}")
        return df, ultima

    except Exception as e:
        log(f"❌ Erro Crítico Memória/Leitura: {e}")
        # Limpa para não deixar arquivo corrompido
        if os.path.exists(PATH_CROWLEY): os.remove(PATH_CROWLEY)
        return None, "Erro Leitura"

