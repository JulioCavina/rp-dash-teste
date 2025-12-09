# utils/loaders.py
import os
import gc
import tempfile
import pandas as pd
import streamlit as st
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from .format import normalize_dataframe

# --- FUNÇÕES AUXILIARES DE CONEXÃO ---

def get_drive_service():
    """Autentica e retorna o serviço do Google Drive."""
    # Verificação de segurança para não crashar se secrets não existirem
    if "gcp_service_account" not in st.secrets or "drive_files" not in st.secrets:
        st.error("❌ Erro: Segredos de configuração (Secrets) não encontrados.")
        return None

    try:
        service_account_info = dict(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        st.error(f"Erro na autenticação do Drive: {e}")
        return None

def download_to_temp_file(service, file_id, suffix):
    """
    Baixa um arquivo do Drive para uma pasta temporária no disco.
    Retorna o caminho do arquivo.
    """
    try:
        # Delete=False é necessário para Windows/alguns Linux para permitir reabertura
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
            request = service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(tmp_file, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            tmp_file.flush()
            return tmp_file.name
    except Exception as e:
        print(f"Erro no download temporário: {e}")
        return None

def get_file_modified_time(service, file_id):
    """Obtém a data de modificação do arquivo no Drive."""
    try:
        file_metadata = service.files().get(fileId=file_id, fields="modifiedTime").execute()
        mod_time_str = file_metadata.get("modifiedTime")
        if mod_time_str:
            mod_dt = datetime.strptime(mod_time_str[:19], "%Y-%m-%dT%H:%M:%S")
            return mod_dt.strftime("%d/%m/%Y %H:%M")
    except:
        pass
    return datetime.now().strftime("%d/%m/%Y %H:%M")

# --- CARREGAMENTO BASE DE VENDAS (FATURAMENTO) ---

# MUDANÇA 1: cache_resource gasta menos RAM pois não serializa o objeto
@st.cache_resource(ttl=3600, show_spinner="Carregando do Data Lake (Vendas)...")
def fetch_from_drive():
    # Limpeza preventiva antes de começar
    gc.collect()
    
    service = get_drive_service()
    if not service: return None, None

    file_id = st.secrets["drive_files"]["faturamento_xlsx"]
    temp_path = None

    try:
        temp_path = download_to_temp_file(service, file_id, suffix=".parquet")
        if not temp_path: return None, "Erro Download"

        try:
            df_raw = pd.read_parquet(temp_path)
        except Exception:
            try:
                df_raw = pd.read_excel(temp_path, engine="openpyxl")
            except Exception as e:
                st.error(f"Erro ao ler Vendas: {e}")
                return None, None
        
        df = normalize_dataframe(df_raw)
        
        # Libera memória imediatamente
        del df_raw
        gc.collect()

        if df.empty: return None, "Dados Vazios"

        ultima_atualizacao = "N/A"
        if "data_ref" in df.columns and pd.api.types.is_datetime64_any_dtype(df["data_ref"]):
            max_date = df["data_ref"].max()
            if pd.notna(max_date): ultima_atualizacao = max_date.strftime("%m/%Y")
        
        if ultima_atualizacao == "N/A":
            ultima_atualizacao = get_file_modified_time(service, file_id)
            
        return df, ultima_atualizacao

    except Exception as e:
        print(f"Erro Drive Vendas: {e}")
        return None, None
    
    finally:
        if temp_path and os.path.exists(temp_path):
            try: os.remove(temp_path)
            except: pass
        gc.collect()

def load_main_base():
    if "uploaded_dataframe" in st.session_state and st.session_state.uploaded_dataframe is not None:
        return st.session_state.uploaded_dataframe, st.session_state.get("uploaded_timestamp", "Upload Manual")
    return fetch_from_drive()

# --- CARREGAMENTO BASE CROWLEY (PARQUET - ULTRA OTIMIZADO) ---

# MUDANÇA 1: cache_resource é vital aqui para evitar duplicar a base na memória
@st.cache_resource(ttl=3600, show_spinner="Acessando base Crowley (Otimizado)...")
def load_crowley_base():
    # Limpeza preventiva: tenta liberar espaço da versão antiga se possível
    gc.collect()

    service = get_drive_service()
    if not service: return None, "Erro Conexão"

    file_id = st.secrets["drive_files"]["crowley_parquet"]
    temp_path = None

    try:
        temp_path = download_to_temp_file(service, file_id, suffix=".parquet")
        if not temp_path: return None, "Erro Download"

        df = pd.read_parquet(temp_path)
        
        # OTIMIZAÇÃO: Converter Strings para Categorias
        cols_para_categoria = ["Praca", "Emissora", "Anunciante", "Anuncio", "Tipo", "DayPart"]
        for col in cols_para_categoria:
            if col in df.columns:
                df[col] = df[col].astype("category")

        # OTIMIZAÇÃO: Downcast Numérico
        cols_numericas = ["Volume de Insercoes", "Duracao"]
        for col in cols_numericas:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], downcast="integer")

        # DATA: Datetime
        ultima_atualizacao = "N/A"
        if "Data" in df.columns:
            # Converte e sobrescreve
            df["Data_Dt"] = pd.to_datetime(df["Data"], dayfirst=True, errors="coerce")
            
            # Remove a original string para liberar memória
            df.drop(columns=["Data"], inplace=True)
            
            try:
                max_ts = df["Data_Dt"].max()
                if pd.notna(max_ts):
                    ultima_atualizacao = max_ts.strftime("%d/%m/%Y")
            except:
                pass

        if ultima_atualizacao == "N/A":
            ultima_atualizacao = get_file_modified_time(service, file_id)

        return df, ultima_atualizacao

    except Exception as e:
        st.error(f"Erro ao processar Crowley: {e}")
        return None, "Erro Leitura"
        
    finally:
        # Garante que o arquivo temporário no disco seja deletado
        if temp_path and os.path.exists(temp_path):
            try: os.remove(temp_path)
            except: pass
        # Faxina final na memória
        gc.collect()