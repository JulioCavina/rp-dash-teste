# utils/filters.py
import streamlit as st
import pandas as pd
import json 
from datetime import datetime 

def aplicar_filtros(df, cookies):
    """
    Aplica filtros interativos no TOPO da página (Main Area).
    Retorna os dados filtrados e as flags de configuração (Rótulos e Totalizador).
    """

    # ==================== NORMALIZAÇÃO ====================
    df.columns = df.columns.str.strip().str.lower()

    if "mes" not in df.columns: 
        possiveis = ["mês", "month", "mês referência", "mes_ref", "data", "date"]
        for c in possiveis:
            if c in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[c]):
                    df["mes"] = df[c].dt.month
                else:
                    df["mes"] = pd.to_numeric(df[c], errors="coerce")
                break
        else:
            df["mes"] = 1

    if "ano" not in df.columns:
        possiveis_ano = ["ano_ref", "ano referência", "year", "data", "date"]
        for c in possiveis_ano:
            if c in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[c]):
                    df["ano"] = df[c].dt.year
                else:
                    df["ano"] = pd.to_numeric(df[c], errors="coerce")
                break
        else:
            df["ano"] = 2024

    for col in ["emissora", "executivo", "cliente"]:
        if col not in df.columns:
            df[col] = ""

    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").fillna(0).astype(int)
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce").fillna(0).astype(int)


    # ==================== DADOS BASE PARA FILTROS ====================
    anos_disponiveis = sorted(df["ano"].dropna().unique())
    emisoras = sorted(df["emissora"].dropna().unique())
    execs = sorted(df["executivo"].dropna().unique())
    clientes = sorted(df["cliente"].dropna().unique())
    
    mes_map = {
        1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
        7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
    }
    mes_map_inverso = {v: k for k, v in mes_map.items()}
    
    meses_disponiveis_num = sorted(df[df["mes"].between(1, 12)]["mes"].dropna().unique())
    meses_disponiveis_nomes = [mes_map.get(m, m) for m in meses_disponiveis_num]


    # ==================== LÓGICA DE PERSISTÊNCIA (SESSION STATE) ====================
    
    if anos_disponiveis:
        default_ini = min(anos_disponiveis)
        default_fim = max(anos_disponiveis)
    else:
        default_ini = 2024
        default_fim = 2025
    
    if "filtro_ano_ini" not in st.session_state:
        st.session_state["filtro_ano_ini"] = default_ini
    if "filtro_ano_fim" not in st.session_state:
        st.session_state["filtro_ano_fim"] = default_fim

    if "filtro_emis" not in st.session_state:
        st.session_state["filtro_emis"] = emisoras

    if "filtro_execs" not in st.session_state:
        st.session_state["filtro_execs"] = execs

    if "filtro_clientes" not in st.session_state:
        st.session_state["filtro_clientes"] = []

    if "filtro_meses_lista" not in st.session_state:
        st.session_state["filtro_meses_lista"] = meses_disponiveis_nomes
    
    if "filtro_show_labels" not in st.session_state:
        st.session_state["filtro_show_labels"] = True 

    # NOVO: Estado do Totalizador
    if "filtro_show_total" not in st.session_state:
        st.session_state["filtro_show_total"] = True

    # --- CALLBACKS ---
    def reset_filtros_callback():
        st.session_state["filtro_ano_ini"] = default_ini
        st.session_state["filtro_ano_fim"] = default_fim
        st.session_state["filtro_emis"] = emisoras
        st.session_state["filtro_execs"] = execs
        st.session_state["filtro_clientes"] = []
        st.session_state["filtro_meses_lista"] = meses_disponiveis_nomes
        st.session_state["filtro_show_labels"] = True
        st.session_state["filtro_show_total"] = True # Reset Totalizador
        
        if cookies.get("app_filters"):
            del cookies["app_filters"] 
            cookies.save()

    def set_ytd_callback():
        hoje = datetime.now()
        mes_atual = hoje.month
        meses_ytd_num = list(range(1, mes_atual + 1))
        meses_ytd_nomes = [mes_map.get(m) for m in meses_ytd_num if m in mes_map]
        st.session_state["filtro_meses_lista"] = meses_ytd_nomes
        
    # ==================== WIDGETS NO TOPO (EXPANDER WIDE) ====================
    
    with st.expander("Filtros Globais (Clique para expandir)", expanded=False):
        
        # --- LINHA 1: PERÍODO E CATEGORIAS MACRO ---
        c1, c2, c3, c4 = st.columns([1, 1, 2, 2])
        
        with c1:
            st.selectbox("Ano De:", anos_disponiveis, key="filtro_ano_ini")
        with c2:
            st.selectbox("Ano Até:", anos_disponiveis, key="filtro_ano_fim")
        with c3:
            st.multiselect("Emissoras:", emisoras, key="filtro_emis")
        with c4:
            st.multiselect("Executivos:", execs, key="filtro_execs")

        # --- LINHA 2: MESES E CLIENTES ---
        c5, c6 = st.columns([2, 4])
        
        with c5:
            st.multiselect("Meses:", meses_disponiveis_nomes, key="filtro_meses_lista")
        with c6:
            st.multiselect("Clientes:", clientes, key="filtro_clientes", help="Digite para buscar clientes específicos")

        st.markdown("---")

        # --- LINHA 3: AÇÕES (Atualizada com Totalizador) ---
        st.markdown("**Controles & Ações**")
        
        # Ajuste de colunas para caber 4 botões + espaço
        c7, c8, c9, c10, c11 = st.columns([1.2, 1.2, 1.5, 0.8, 0.8])
        
        # Botão Rótulos
        with c7:
            is_active_lbl = st.session_state["filtro_show_labels"]
            if is_active_lbl:
                btn_type_lbl = "primary"
                btn_text_lbl = "Rótulos: Ativo"
            else:
                btn_type_lbl = "secondary"
                btn_text_lbl = "Rótulos: Inativo"
            
            if st.button(btn_text_lbl, type=btn_type_lbl, key="btn_toggle_labels", help="Ativar/Desativar Rótulo de Dados", use_container_width=True):
                st.session_state["filtro_show_labels"] = not is_active_lbl
                st.rerun()

        # Botão Totalizador (Novo)
        with c8:
            is_active_tot = st.session_state["filtro_show_total"]
            if is_active_tot:
                btn_type_tot = "primary"
                btn_text_tot = "Totalizador: Ativo"
            else:
                btn_type_tot = "secondary"
                btn_text_tot = "Totalizador: Inativo"
            
            if st.button(btn_text_tot, type=btn_type_tot, key="btn_toggle_total", help="Ativar/Desativar linha Totalizadora", use_container_width=True):
                st.session_state["filtro_show_total"] = not is_active_tot
                st.rerun()

        # Botões de Ação (YTD e Limpar)
        with c10:
            st.button("YTD", type="secondary", help="Selecionar de Jan até Hoje", use_container_width=True, on_click=set_ytd_callback)
        
        with c11:
            st.button("Limpar", type="secondary", help="Resetar todos os filtros", use_container_width=True, on_click=reset_filtros_callback)


    # ==================== APLICA FILTROS (BACKEND) ====================
    ano_ini_sel = st.session_state["filtro_ano_ini"]
    ano_fim_sel = st.session_state["filtro_ano_fim"]
    
    ano_1 = min(ano_ini_sel, ano_fim_sel)
    ano_2 = max(ano_ini_sel, ano_fim_sel)
    anos_sel = list(range(ano_1, ano_2 + 1)) 
    
    emis_sel = st.session_state["filtro_emis"]
    exec_sel = st.session_state["filtro_execs"]
    cli_sel = st.session_state["filtro_clientes"]
    
    meses_sel_nomes = st.session_state["filtro_meses_lista"]
    meses_sel_num = [mes_map_inverso.get(m, -1) for m in meses_sel_nomes]
    
    mes_ini = min(meses_sel_num) if meses_sel_num else 1
    mes_fim = max(meses_sel_num) if meses_sel_num else 12
    
    # Flags de visualização
    show_labels = st.session_state["filtro_show_labels"]
    show_total = st.session_state["filtro_show_total"]
    
    df_filtrado = df[
        (df["ano"].between(ano_1, ano_2)) &
        (df["emissora"].isin(emis_sel)) &
        (df["executivo"].isin(exec_sel)) &
        (df["mes"].isin(meses_sel_num))
    ]

    if cli_sel:
        df_filtrado = df_filtrado[df_filtrado["cliente"].isin(cli_sel)]
    
    # Salva os filtros no Cookie (silencioso)
    try:
        current_filters = {
            "filtro_ano_ini": int(st.session_state["filtro_ano_ini"]),
            "filtro_ano_fim": int(st.session_state["filtro_ano_fim"]),
            "filtro_emis": st.session_state["filtro_emis"],
            "filtro_execs": st.session_state["filtro_execs"],
            "filtro_clientes": st.session_state["filtro_clientes"],
            "filtro_meses_lista": st.session_state["filtro_meses_lista"],
            "filtro_show_labels": st.session_state["filtro_show_labels"], 
            "filtro_show_total": st.session_state["filtro_show_total"], # Salva no cookie
        }
        cookies["app_filters"] = json.dumps(current_filters)
        cookies.save()
    except Exception:
        pass

    # Retorna também o show_total
    return df_filtrado, anos_sel, emis_sel, exec_sel, cli_sel, mes_ini, mes_fim, show_labels, show_total