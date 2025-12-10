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

# Caminhos
PATH_VENDAS = os.path.join(DATA_FOLDER, "vendas.parquet")

# Separamos: RAW (o que baixa do Google) e OPT (o que o Streamlit lê)
PATH_CROWLEY_RAW = os.path.join(DATA_FOLDER, "crowley_raw.parquet")
PATH_CROWLEY_OPT = os.path.join(DATA_FOLDER, "crowley_opt.parquet")

def log(msg):
    """Função auxiliar para logs visíveis no Streamlit Cloud"""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")

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
    try:
        meta = service.files().get(fileId=file_id, fields="modifiedTime").execute()
        dt_str = meta.get("modifiedTime")
        # Tenta formatos comuns do Drive
        try:
            return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        except:
            return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception as e:
        log(f"⚠️ Aviso: Não foi possível ler metadados do Drive: {e}")
        return None

# --- PROCESSAMENTO PESADO (ETL) ---
def optimize_crowley(raw_path, opt_path):
    """
    Lê o arquivo RAW, aplica tipagem agressiva e salva o OPT.
    """
    try:
        log("⚙️ INÍCIO ETL: Otimizando base Crowley...")
        start_time = time.time()
        
        # Lê o bruto
        df = pd.read_parquet(raw_path)
        
        # 1. OTIMIZAÇÃO: Categorias
        cols_cat = ["Praca", "Emissora", "Anunciante", "Anuncio", "Tipo", "DayPart"]
        for col in cols_cat:
            if col in df.columns: 
                df[col] = df[col].astype("category")

        # 2. OTIMIZAÇÃO: Numéricos (Downcast int32)
        cols_num = ["Volume de Insercoes", "Duracao"]
        for col in cols_num:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype("int32")

        # 3. DATA
        if "Data" in df.columns:
            df["Data_Dt"] = pd.to_datetime(df["Data"], dayfirst=True, errors="coerce")
            df.drop(columns=["Data"], inplace=True)
            
        # Salva o arquivo FINAL OTIMIZADO
        df.to_parquet(opt_path, index=False)
        
        elapsed = time.time() - start_time
        log(f"✅ FIM ETL: Base salva em {opt_path} ({elapsed:.2f}s)")
        
        # Limpa memória
        del df
        gc.collect()
        return True
    except Exception as e:
        log(f"❌ ERRO CRÍTICO ETL: {e}")
        return False

# --- DOWNLOADER INTELIGENTE ---
def download_and_process(service, file_id, path_final, path_raw=None, is_crowley=False):
    """
    Lógica estrita:
    1. Verifica data.
    2. Se desatualizado: DELETA arquivos locais -> BAIXA -> PROCESSA.
    """
    try:
        # 1. Checagem de versão
        drive_dt = get_drive_metadata(service, file_id)
        
        # Se is_crowley, verificamos a data do arquivo OTIMIZADO (que é o que usamos)
        check_path = path_final
        
        if os.path.exists(check_path) and drive_dt:
            local_ts = os.path.getmtime(check_path)
            local_dt = datetime.fromtimestamp(local_ts, tz=timezone.utc)
            
            if local_dt >= drive_dt:
                log(f"⏭️ Arquivo atualizado. Pulando download. (Drive: {drive_dt} | Local: {local_dt})")
                return False # Sem alterações

        # 2. LIMPEZA PRÉ-DOWNLOAD (Liberar disco e RAM)
        log("🧹 Limpando arquivos antigos e memória...")
        gc.collect()
        
        if os.path.exists(path_final):
            os.remove(path_final)
            log(f"🗑️ Deletado antigo: {path_final}")
            
        if path_raw and os.path.exists(path_raw):
            os.remove(path_raw)
            log(f"🗑️ Deletado rascunho: {path_raw}")

        # 3. DOWNLOAD
        target_download_path = path_raw if is_crowley else path_final
        log(f"📥 Iniciando Download para: {target_download_path}")
        
        with open(target_download_path, "wb") as f:
            request = service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        
        log("✅ Download concluído.")

        # 4. PÓS-PROCESSAMENTO (Só Crowley)
        if is_crowley:
            success = optimize_crowley(path_raw, path_final)
            
            # Remove o RAW para economizar espaço
            if os.path.exists(path_raw):
                os.remove(path_raw)
                log("🗑️ Arquivo RAW removido após otimização.")
            
            if not success:
                st.error("Falha no processamento da base.")
                return False
        
        return True

    except Exception as e:
        log(f"❌ ERRO DOWNLOAD: {e}")
        return False

# --- LOADERS (CACHE 3 MINUTOS) ---

# Trocado TTL para 180s (3 min) para testes
@st.cache_resource(ttl=180, show_spinner="Atualizando Vendas...")
def fetch_from_drive():
    log("🔄 Cache Vendas expirado ou ausente. Iniciando refresh...")
    gc.collect()
    service = get_drive_service()
    if not service: return None, None

    file_id = st.secrets["drive_files"]["faturamento_xlsx"]
    
    # Baixa Vendas (Direto para o final, sem otimização pesada)
    download_and_process(service, file_id, path_final=PATH_VENDAS, is_crowley=False)
    
    try:
        # Tenta ler
        try:
            df_raw = pd.read_parquet(PATH_VENDAS, memory_map=True)
        except:
            df_raw = pd.read_excel(PATH_VENDAS, engine="openpyxl")

        df = normalize_dataframe(df_raw)
        
        # Limpeza
        del df_raw
        gc.collect()

        # Data Ref
        ultima_atualizacao = "N/A"
        if "data_ref" in df.columns and pd.api.types.is_datetime64_any_dtype(df["data_ref"]):
            max_date = df["data_ref"].max()
            if pd.notna(max_date): ultima_atualizacao = max_date.strftime("%m/%Y")
        
        if ultima_atualizacao == "N/A" and os.path.exists(PATH_VENDAS):
            ts = os.path.getmtime(PATH_VENDAS)
            ultima_atualizacao = datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M")

        log("✅ Vendas carregado na memória.")
        return df, ultima_atualizacao

    except Exception as e:
        log(f"❌ Erro leitura Vendas: {e}")
        return None, None

def load_main_base():
    if "uploaded_dataframe" in st.session_state and st.session_state.uploaded_dataframe is not None:
        return st.session_state.uploaded_dataframe, st.session_state.get("uploaded_timestamp", "Upload Manual")
    return fetch_from_drive()

# Trocado TTL para 180s (3 min) para testes
@st.cache_resource(ttl=180, show_spinner="Atualizando Crowley...")
def load_crowley_base():
    log("🔄 Cache Crowley expirado. Iniciando rotina...")
    
    # 1. Limpeza Radical
    gc.collect()
    
    service = get_drive_service()
    if not service: return None, "Erro Conexão"

    file_id = st.secrets["drive_files"]["crowley_parquet"]
    
    # 2. Baixa e Otimiza (Se necessário)
    # Aqui ele deleta o antigo antes de baixar o novo
    download_and_process(
        service, 
        file_id, 
        path_final=PATH_CROWLEY_OPT, 
        path_raw=PATH_CROWLEY_RAW, 
        is_crowley=True
    )

    # 3. Leitura Leve (Memory Map)
    try:
        if not os.path.exists(PATH_CROWLEY_OPT):
            log("⚠️ Arquivo otimizado não encontrado.")
            return None, "Erro: Arquivo Inexistente"

        log("📖 Lendo arquivo otimizado com Memory Map...")
        # AQUI É O SEGREDO: Não fazemos astype/transformações. Lemos o que está no disco.
        df = pd.read_parquet(PATH_CROWLEY_OPT, memory_map=True)
        
        # Data
        ultima_atualizacao = "N/A"
        try:
            if "Data_Dt" in df.columns:
                 max_ts = df["Data_Dt"].max()
                 if pd.notna(max_ts): ultima_atualizacao = max_ts.strftime("%d/%m/%Y")
        except: pass

        if ultima_atualizacao == "N/A":
            ts = os.path.getmtime(PATH_CROWLEY_OPT)
            ultima_atualizacao = datetime.fromtimestamp(ts).strftime("%d/%m/%Y")

        log(f"✅ Crowley carregado com sucesso! ({len(df)} linhas)")
        return df, ultima_atualizacao

    except Exception as e:
        log(f"❌ Erro Leitura Final Crowley: {e}")
        # Se o arquivo estiver corrompido, apaga para tentar de novo na próxima
        if os.path.exists(PATH_CROWLEY_OPT): 
            os.remove(PATH_CROWLEY_OPT)
        return None, "Erro Leitura"
