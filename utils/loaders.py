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
    """Logs visíveis no console do Streamlit Cloud"""
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
        try:
            return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        except:
            return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception as e:
        log(f"⚠️ Aviso: Falha ao ler metadados: {e}")
        return None

# --- ETL ESTRATÉGICO (Baixo Consumo) ---
def optimize_crowley(raw_path, opt_path):
    """
    Lê o arquivo RAW e salva o OPTIMIZED.
    TRUQUE: Lê apenas as colunas estritamente necessárias para economizar RAM no processo.
    """
    try:
        log("⚙️ INÍCIO ETL: Convertendo base...")
        
        # Defina aqui apenas as colunas que seu dashboard REALMENTE usa.
        # Se carregar colunas inúteis, a RAM explode.
        cols_to_load = [
            "Data", "Praca", "Emissora", "Anunciante", "Anuncio", 
            "Tipo", "DayPart", "Volume de Insercoes", "Duracao"
        ]
        
        # Tenta ler apenas colunas existentes (fallback se a coluna não existir)
        try:
            # Pega o schema para validar colunas antes de ler
            schema = pq.read_schema(raw_path)
            actual_cols = [c for c in cols_to_load if c in schema.names]
            df = pd.read_parquet(raw_path, columns=actual_cols)
        except:
            # Fallback: lê tudo se der erro no schema
            df = pd.read_parquet(raw_path)
        
        # 1. OTIMIZAÇÃO: Categorias
        cols_cat = ["Praca", "Emissora", "Anunciante", "Anuncio", "Tipo", "DayPart"]
        for col in cols_cat:
            if col in df.columns: 
                df[col] = df[col].astype("category")

        # 2. OTIMIZAÇÃO: Numéricos
        cols_num = ["Volume de Insercoes", "Duracao"]
        for col in cols_num:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype("int32")

        # 3. DATA
        if "Data" in df.columns:
            df["Data_Dt"] = pd.to_datetime(df["Data"], dayfirst=True, errors="coerce")
            df.drop(columns=["Data"], inplace=True)
            
        # Salva
        df.to_parquet(opt_path, index=False)
        log(f"✅ FIM ETL: Base otimizada salva em {opt_path}")
        
        del df, schema
        gc.collect()
        return True
    except Exception as e:
        log(f"❌ ERRO CRÍTICO ETL: {e}")
        return False

# --- "NUCLEAR" CLEAN & DOWNLOAD ---
def nuclear_download_sequence(service, file_id, path_final, path_raw=None, is_crowley=False):
    """
    Sequência Estrita:
    1. Verifica se precisa atualizar.
    2. SE SIM: DELETA TUDO (Arquivos finais e temporários).
    3. LIMPA RAM.
    4. SÓ ENTÃO inicia o download.
    """
    try:
        # 1. Checagem de versão
        drive_dt = get_drive_metadata(service, file_id)
        check_path = path_final
        
        if os.path.exists(check_path) and drive_dt:
            local_ts = os.path.getmtime(check_path)
            local_dt = datetime.fromtimestamp(local_ts, tz=timezone.utc)
            
            # Se local for mais novo, retorna False (Não faz nada)
            if local_dt >= drive_dt:
                log(f"⏭️ Base atualizada. Mantendo cache.")
                return False 

        # --- AQUI COMEÇA O PROCESSO DESTRUTIVO ---
        log("🔥 ATUALIZAÇÃO DETECTADA: Iniciando limpeza total...")
        
        # Força limpeza de RAM
        gc.collect()
        
        # REMOVE ARQUIVO FINAL (Otimizado/Vendas)
        if os.path.exists(path_final):
            try:
                os.remove(path_final)
                log(f"🗑️ Deletado do disco: {path_final}")
            except Exception as e:
                log(f"⚠️ Erro ao deletar {path_final}: {e}")

        # REMOVE ARQUIVO RAW (Se houver resquício)
        if path_raw and os.path.exists(path_raw):
            try:
                os.remove(path_raw)
                log(f"🗑️ Deletado RAW antigo: {path_raw}")
            except: pass

        # Espera 1s para o OS liberar handles de arquivo
        time.sleep(1)
        gc.collect()

        # --- DOWNLOAD ---
        target_download_path = path_raw if is_crowley else path_final
        log(f"📥 Iniciando Download Limpo para: {target_download_path}")
        
        with open(target_download_path, "wb") as f:
            request = service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        
        log("✅ Download concluído.")

        # --- PÓS-PROCESSAMENTO ---
        if is_crowley:
            success = optimize_crowley(path_raw, path_final)
            
            # Deleta o RAW imediatamente após o uso
            if os.path.exists(path_raw):
                os.remove(path_raw)
                log("🗑️ Arquivo RAW removido para liberar espaço.")
            
            if not success:
                st.error("Falha Crítica no Processamento.")
                return False
        
        return True

    except Exception as e:
        log(f"❌ ERRO DOWNLOAD NUCLEAR: {e}")
        return False

# --- LOADERS (TTL 3 MINUTOS) ---

@st.cache_resource(ttl=180, show_spinner="Atualizando Vendas...")
def fetch_from_drive():
    log("🔄 Cache Vendas expirado. Verificando...")
    gc.collect()
    service = get_drive_service()
    if not service: return None, None

    file_id = st.secrets["drive_files"]["faturamento_xlsx"]
    
    # Processo Nuclear
    nuclear_download_sequence(service, file_id, path_final=PATH_VENDAS, is_crowley=False)
    
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
        
        if ultima_atualizacao == "N/A" and os.path.exists(PATH_VENDAS):
            ts = os.path.getmtime(PATH_VENDAS)
            ultima_atualizacao = datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M")

        return df, ultima_atualizacao

    except Exception as e:
        log(f"❌ Erro leitura Vendas: {e}")
        return None, None

def load_main_base():
    if "uploaded_dataframe" in st.session_state and st.session_state.uploaded_dataframe is not None:
        return st.session_state.uploaded_dataframe, st.session_state.get("uploaded_timestamp", "Upload Manual")
    return fetch_from_drive()


@st.cache_resource(ttl=180, show_spinner="Atualizando Crowley...")
def load_crowley_base():
    log("🔄 TIMER 3 MIN: Iniciando rotina Crowley...")
    
    # 1. Limpeza Radical da Memória
    gc.collect()
    
    service = get_drive_service()
    if not service: return None, "Erro Conexão"

    file_id = st.secrets["drive_files"]["crowley_parquet"]
    
    # 2. Chama a Sequência Nuclear
    # (Deleta cache disco -> Limpa RAM -> Baixa -> Processa)
    nuclear_download_sequence(
        service, 
        file_id, 
        path_final=PATH_CROWLEY_OPT, 
        path_raw=PATH_CROWLEY_RAW, 
        is_crowley=True
    )

    # 3. Leitura Leve (Otimizada)
    try:
        if not os.path.exists(PATH_CROWLEY_OPT):
            log("⚠️ Arquivo otimizado não encontrado (Download falhou?).")
            return None, "Erro: Arquivo Inexistente"

        log("📖 Lendo arquivo otimizado (Memory Map)...")
        
        # AQUI É O PONTO CRÍTICO:
        # Se memory_map=True e o arquivo foi recém criado, o OS gerencia a RAM.
        df = pd.read_parquet(PATH_CROWLEY_OPT, memory_map=True)
        
        # Extração de Data Segura (sem astype)
        ultima_atualizacao = "N/A"
        try:
            if "Data_Dt" in df.columns:
                 max_ts = df["Data_Dt"].max()
                 if pd.notna(max_ts): ultima_atualizacao = max_ts.strftime("%d/%m/%Y")
        except: pass

        if ultima_atualizacao == "N/A":
            ts = os.path.getmtime(PATH_CROWLEY_OPT)
            ultima_atualizacao = datetime.fromtimestamp(ts).strftime("%d/%m/%Y")

        log(f"✅ Crowley carregado! ({len(df)} linhas)")
        return df, ultima_atualizacao

    except Exception as e:
        log(f"❌ Erro Leitura Final Crowley: {e}")
        # Se falhar na leitura final, limpa para não deixar lixo
        if os.path.exists(PATH_CROWLEY_OPT): 
            os.remove(PATH_CROWLEY_OPT)
        return None, "Erro Leitura"
