# utils/loaders.py
import os
import gc
import pandas as pd
import streamlit as st
import pyarrow.parquet as pq
from datetime import datetime, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from .format import normalize_dataframe

# Cria a pasta data se não existir
DATA_FOLDER = "data"
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

# Caminhos persistentes
PATH_CROWLEY = os.path.join(DATA_FOLDER, "crowley.parquet")
PATH_VENDAS = os.path.join(DATA_FOLDER, "vendas.parquet")

# --- CONEXÃO DRIVE ---
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

def get_drive_metadata(service, file_id):
    """Retorna timestamp de modificação do arquivo no Drive (aware datetime)."""
    try:
        meta = service.files().get(fileId=file_id, fields="modifiedTime").execute()
        dt_str = meta.get("modifiedTime")
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except Exception:
        try:
            return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except:
            return None

def download_file_persistent(service, file_id, local_path):
    """
    Baixa o arquivo para a pasta data/ com Logs de Data/Hora.
    Usa troca atômica (.tmp -> final) para garantir substituição limpa.
    """
    try:
        # 1. Verifica metadados do Drive
        drive_dt = get_drive_metadata(service, file_id)
        
        # 2. Verifica arquivo local
        if os.path.exists(local_path) and drive_dt:
            local_ts = os.path.getmtime(local_path)
            local_dt = datetime.fromtimestamp(local_ts, tz=timezone.utc)
            
            # Se o arquivo local for mais novo ou igual ao do Drive, NÃO baixa
            if local_dt >= drive_dt:
                # print(f"Arquivo local {local_path} já está atualizado.") # Silencioso se não precisar atualizar
                return True

        # 3. Download Atômico
        timestamp_log = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print(f"[{timestamp_log}] 📥 Iniciando download da atualização: {local_path}...")
        
        # Baixa para um arquivo temporário primeiro (.tmp)
        temp_download_path = local_path + ".tmp"
        
        with open(temp_download_path, "wb") as f:
            request = service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        
        # 4. Substituição (Remove o velho e coloca o novo)
        if os.path.exists(local_path):
            try:
                os.remove(local_path) # Tenta limpar o antigo explicitamente
            except OSError:
                pass # Se estiver bloqueado (Windows), o replace tenta forçar
        
        os.replace(temp_download_path, local_path)
        
        timestamp_end = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print(f"[{timestamp_end}] ✅ Atualização concluída e substituída: {local_path}")
        
        return True
    except Exception as e:
        timestamp_err = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print(f"[{timestamp_err}] ❌ Erro download persistente: {e}")
        # Limpa o temporário se deu erro
        if os.path.exists(local_path + ".tmp"):
            os.remove(local_path + ".tmp")
        return False

# --- CARREGAMENTO VENDAS (CACHE RESOURCE) ---
@st.cache_resource(ttl=3600, show_spinner="Atualizando Vendas...")
def fetch_from_drive():
    gc.collect()
    service = get_drive_service()
    if not service: return None, None

    file_id = st.secrets["drive_files"]["faturamento_xlsx"]
    
    # Atualiza pasta data/ (Sobrescreve se necessário)
    if not download_file_persistent(service, file_id, PATH_VENDAS):
        if not os.path.exists(PATH_VENDAS):
            return None, "Erro Download"

    try:
        try:
            df_raw = pd.read_parquet(PATH_VENDAS, memory_map=True)
        except:
            df_raw = pd.read_excel(PATH_VENDAS, engine="openpyxl")

        df = normalize_dataframe(df_raw)
        del df_raw
        gc.collect()

        ultima_atualizacao = "N/A"
        if "data_ref" in df.columns and pd.api.types.is_datetime64_any_dtype(df["data_ref"]):
            max_date = df["data_ref"].max()
            if pd.notna(max_date): ultima_atualizacao = max_date.strftime("%m/%Y")
        
        if ultima_atualizacao == "N/A":
            ts = os.path.getmtime(PATH_VENDAS)
            ultima_atualizacao = datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M")

        return df, ultima_atualizacao

    except Exception as e:
        st.error(f"Erro ao ler Vendas: {e}")
        return None, None

def load_main_base():
    if "uploaded_dataframe" in st.session_state and st.session_state.uploaded_dataframe is not None:
        return st.session_state.uploaded_dataframe, st.session_state.get("uploaded_timestamp", "Upload Manual")
    return fetch_from_drive()


# --- CARREGAMENTO CROWLEY (CACHE RESOURCE + MEMORY MAP) ---
@st.cache_resource(ttl=3600, show_spinner="Atualizando Crowley...")
def load_crowley_base():
    # Limpeza de memória antes de começar
    gc.collect()
    
    service = get_drive_service()
    if not service: return None, "Erro Conexão"

    file_id = st.secrets["drive_files"]["crowley_parquet"]
    
    # Atualiza pasta data/ (Sobrescreve se necessário)
    success = download_file_persistent(service, file_id, PATH_CROWLEY)
    if not success and not os.path.exists(PATH_CROWLEY):
        return None, "Erro Download"

    try:
        # LEITURA COM MEMORY MAP (Economia de RAM)
        df = pd.read_parquet(PATH_CROWLEY, memory_map=True)
        
        # OTIMIZAÇÃO: Categorias
        cols_cat = ["Praca", "Emissora", "Anunciante", "Anuncio", "Tipo", "DayPart"]
        for col in cols_cat:
            if col in df.columns: df[col] = df[col].astype("category")

        # OTIMIZAÇÃO: Numéricos (Limpeza de Vazios + Downcast)
        cols_num = ["Volume de Insercoes", "Duracao"]
        for col in cols_num:
            if col in df.columns:
                # fillna(0) corrige o erro do PyArrow com células vazias
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype("int32")

        # DATA
        ultima_atualizacao = "N/A"
        if "Data" in df.columns:
            df["Data_Dt"] = pd.to_datetime(df["Data"], dayfirst=True, errors="coerce")
            df.drop(columns=["Data"], inplace=True)
            try:
                max_ts = df["Data_Dt"].max()
                if pd.notna(max_ts): ultima_atualizacao = max_ts.strftime("%d/%m/%Y")
            except: pass

        if ultima_atualizacao == "N/A":
            ts = os.path.getmtime(PATH_CROWLEY)
            ultima_atualizacao = datetime.fromtimestamp(ts).strftime("%d/%m/%Y")

        return df, ultima_atualizacao

    except Exception as e:
        st.error(f"Erro Crítico Crowley: {e}")
        # Se corrompeu, apaga para forçar download na próxima
        if os.path.exists(PATH_CROWLEY): os.remove(PATH_CROWLEY)
        return None, "Erro Leitura"
        
    finally:
        gc.collect()