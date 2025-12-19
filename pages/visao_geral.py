# pages/visao_geral.py

import streamlit as st
import plotly.express as px
from utils.format import brl, PALETTE
import pandas as pd
import plotly.graph_objects as go 
from plotly.subplots import make_subplots
import numpy as np
from utils.export import create_zip_package 

# ==================== MAPA DE CORES ====================
COLOR_MAP = {
    "Novabrasil": "#6fa8dc",   
    "Difusora": "#9f86c0",     
    "Thathi Tv": "#93c47d",    
    "Th+ Prime": "#76a5af",    
    "novabrasil": "#6fa8dc",
    "difusora": "#9f86c0",
    "thathi tv": "#93c47d",
    "th+ prime": "#76a5af"
}

# ==================== ESTILO CSS (CENTRALIZAÇÃO E ALINHAMENTO) ====================
ST_METRIC_CENTER = """
<style>
/* Container principal do Metric: Flexbox vertical centralizado */
[data-testid="stMetric"] {
    display: flex;
    flex-direction: column;
    align-items: center; 
    justify-content: center; 
    text-align: center;
    width: 100%;
    margin: auto;
}

/* Rótulo (Título do Card) */
[data-testid="stMetricLabel"] {
    justify-content: center;
    width: 100%;
    margin-bottom: 0px !important; 
}

/* Valor (Número Grande) */
[data-testid="stMetricValue"] {
    justify-content: center;
    width: 100%;
}

/* Delta (Variação ou Texto abaixo) */
[data-testid="stMetricDelta"] {
    justify-content: center;
    width: 100%;
    margin-top: 0px !important; 
}
</style>
"""

def format_pt_br_abrev(val):
    if pd.isna(val): return "R$ 0"
    sign = "-" if val < 0 else ""
    val_abs = abs(val)
    if val_abs == 0: return "R$ 0"
    if val_abs >= 1_000_000: return f"{sign}R$ {val_abs/1_000_000:,.1f} Mi".replace(",", "X").replace(".", ",").replace("X", ".")
    if val_abs >= 1_000: return f"{sign}R$ {val_abs/1_000:,.0f} mil".replace(",", "X").replace(".", ",").replace("X", ".")
    return brl(val)

def format_int(val):
    if pd.isna(val) or val == 0: return "-"
    return f"{int(val):,}".replace(",", ".")

def get_pretty_ticks(max_val, num_ticks=5):
    if max_val <= 0: return [0], ["R$ 0"], 100 
    ideal_interval = max_val / num_ticks
    magnitude = 10**np.floor(np.log10(ideal_interval)) if ideal_interval > 0 else 1
    residual = ideal_interval / magnitude
    if residual < 1.5: nice_interval = 1 * magnitude
    elif residual < 3: nice_interval = 2 * magnitude
    elif residual < 7: nice_interval = 5 * magnitude
    else: nice_interval = 10 * magnitude
    max_y_rounded = np.ceil(max_val / nice_interval) * nice_interval
    tick_values = np.arange(0, max_y_rounded + nice_interval, nice_interval)
    tick_texts = [format_pt_br_abrev(v) for v in tick_values]
    y_axis_cap = max_y_rounded * 1.05
    return tick_values, tick_texts, y_axis_cap

def get_top_client_info(df_base):
    if df_base.empty:
        return "—", 0.0, "—"
    top_series = df_base.groupby("cliente")["faturamento"].sum().sort_values(ascending=False)
    if top_series.empty:
        return "—", 0.0, "—"
    nome_full = top_series.index[0]
    valor = top_series.iloc[0]
    nome_display = nome_full[:18] + "..." if len(nome_full) > 18 else nome_full
    return nome_full, valor, nome_display

# ==================== FUNÇÃO AUXILIAR DE TABELA (COM ESTILO DE TOTAL) ====================
def display_styled_table(df):
    if df.empty: return

    def highlight_total_row(row):
        # Verifica se a primeira coluna (Mês/Ano) é "Total"
        if row.iloc[0] == "Total": 
            return ['background-color: #e6f3ff; font-weight: bold; color: #003366'] * len(row)
        return [''] * len(row)

    st.dataframe(
        df.style.apply(highlight_total_row, axis=1), 
        width="stretch", 
        hide_index=True
    )

def render(df, mes_ini, mes_fim, show_labels, show_total, ultima_atualizacao=None):
    # Aplica CSS para centralizar os cards e aproximar título/valor
    st.markdown(ST_METRIC_CENTER, unsafe_allow_html=True)

    # Título Centralizado
    st.markdown("<h2 style='text-align: center; color: #003366;'>Visão Geral</h2>", unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom: 20px;'></div>", unsafe_allow_html=True)

    evol_raw = pd.DataFrame()
    base_emis_raw = pd.DataFrame()
    base_exec_raw = pd.DataFrame()
    
    # Dicionário para armazenar figuras das roscas para exportação
    figs_share_dict = {}
    
    # ==================== PREPARAÇÃO DE DADOS ====================
    df = df.rename(columns={c: c.lower() for c in df.columns})

    if "emissora" in df.columns:
        df["emissora"] = df["emissora"].astype(str).str.strip().str.title()
        df["emissora"] = df["emissora"].replace({
            "Thathi": "Thathi Tv",
            "Th+": "Th+ Prime" 
        })

    if "insercoes" not in df.columns:
        df["insercoes"] = 0.0

    if "meslabel" not in df.columns:
        if "ano" in df.columns and "mes" in df.columns:
            df["meslabel"] = pd.to_datetime(dict(
                year=df["ano"].astype(int),
                month=df["mes"].astype(int),
                day=1
            )).dt.strftime("%b/%y")
        else:
            df["meslabel"] = ""

    anos = sorted(df["ano"].dropna().unique())
    if not anos:
        st.info("Sem anos válidos na base.")
        return
    if len(anos) >= 2:
        ano_base, ano_comp = anos[-2], anos[-1]
    else:
        ano_base = ano_comp = anos[-1]

    ano_base_str = str(ano_base)[-2:]
    ano_comp_str = str(ano_comp)[-2:]
    
    base_periodo = df[df["mes"].between(mes_ini, mes_fim)]
    baseA = base_periodo[base_periodo["ano"] == ano_base]
    baseB = base_periodo[base_periodo["ano"] == ano_comp]

    # ==================== KPI LINHA 1: TOTAIS (MACRO) ====================
    totalA = float(baseA["faturamento"].sum()) if not baseA.empty else 0.0
    totalB = float(baseB["faturamento"].sum()) if not baseB.empty else 0.0
    delta_abs = totalB - totalA
    delta_pct = (delta_abs / totalA * 100) if totalA > 0.0 else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(f"Total {ano_base}", format_pt_br_abrev(totalA))
    c2.metric(f"Total {ano_comp}", format_pt_br_abrev(totalB))
    c3.metric(f"Δ Absoluto ({ano_comp_str}-{ano_base_str})", format_pt_br_abrev(delta_abs))
    c4.metric(f"Δ % ({ano_comp_str} vs {ano_base_str})", f"{delta_pct:.2f}%" if totalA > 0 else "—")

    # ==================== KPI LINHA 2: TICKET MÉDIO E MAIOR CLIENTE ====================
    cliA = baseA["cliente"].nunique()
    tmA = totalA / cliA if cliA > 0 else 0.0
    
    cliB = baseB["cliente"].nunique()
    tmB = totalB / cliB if cliB > 0 else 0.0

    full_A, val_A, disp_A = get_top_client_info(baseA)
    full_B, val_B, disp_B = get_top_client_info(baseB)

    st.markdown("<div style='height: 25px;'></div>", unsafe_allow_html=True) 
    
    k1, k2, k3, k4 = st.columns(4)
    
    k1.metric(f"Ticket Médio ({ano_base})", format_pt_br_abrev(tmA))
    k2.metric(f"Ticket Médio ({ano_comp})", format_pt_br_abrev(tmB))
    
    k3.metric(
        label=f"Maior Cliente ({ano_base})", 
        value=format_pt_br_abrev(val_A),
        delta=disp_A, 
        delta_color="off",
        help=f"Cliente: {full_A}"
    )
    
    k4.metric(
        label=f"Maior Cliente ({ano_comp})", 
        value=format_pt_br_abrev(val_B),
        delta=disp_B, 
        delta_color="off",
        help=f"Cliente: {full_B}"
    )

    st.divider()

    # ==================== 1. TABELA DE EVOLUÇÃO MENSAL ====================
    st.markdown("<p class='custom-chart-title'>1. Evolução Mensal de Faturamento e Inserções</p>", unsafe_allow_html=True)
    
    evol_raw = base_periodo.groupby(["ano", "meslabel", "mes"], as_index=False)[["faturamento", "insercoes"]].sum().sort_values(["ano", "mes"])
    
    if not evol_raw.empty:
        # Prepara Tabela para Visualização
        evol_display = evol_raw[["meslabel", "faturamento", "insercoes"]].copy()
        evol_display.columns = ["Mês/Ano", "Faturamento", "Inserções"]
        
        # --- LÓGICA DO TOTALIZADOR ---
        if show_total:
            total_fat = evol_display["Faturamento"].sum()
            total_ins = evol_display["Inserções"].sum()
            
            # Cria a linha de total
            row_total = pd.DataFrame([{
                "Mês/Ano": "Total",
                "Faturamento": total_fat,
                "Inserções": total_ins
            }])
            
            # Concatena
            evol_display = pd.concat([evol_display, row_total], ignore_index=True)
        
        # Formatação (após somar para não quebrar o cálculo)
        evol_display["Faturamento"] = evol_display["Faturamento"].apply(brl)
        evol_display["Inserções"] = evol_display["Inserções"].apply(format_int)
        
        # Exibe com estilo
        display_styled_table(evol_display)
    else:
        st.info("Sem dados para o período selecionado.")

    st.divider()

    # ==================== GRÁFICO 2: FATURAMENTO POR EMISSORA ====================
    st.markdown("<p class='custom-chart-title'>2. Faturamento por Emissora (Ano a Ano)</p>", unsafe_allow_html=True)
    
    base_emis_raw = base_periodo.groupby(["emissora", "ano"], as_index=False)["faturamento"].sum()
    fig_emis = None

    if not base_emis_raw.empty:
        base_emis_raw = base_emis_raw.sort_values(["emissora", "ano"])
        base_emis_raw["label_x"] = base_emis_raw["emissora"] + " " + base_emis_raw["ano"].astype(str)
        
        fig_emis = px.bar(
            base_emis_raw, 
            x="label_x", 
            y="faturamento", 
            color="emissora", 
            color_discrete_map=COLOR_MAP,
            labels={"label_x": "Emissora / Ano", "faturamento": "Faturamento"}
        )
        
        max_y_emis = base_emis_raw['faturamento'].max()
        tick_vals_e, tick_txt_e, y_cap_e = get_pretty_ticks(max_y_emis)
        
        fig_emis.update_layout(
            height=400, xaxis_title=None, yaxis_title=None, 
            template="plotly_white", showlegend=True, legend_title="Emissora",
            bargap=0.2,
            dragmode=False,
            xaxis=dict(fixedrange=True),
            yaxis=dict(fixedrange=True)
        )
        fig_emis.update_traces(width=0.5) 

        fig_emis.update_yaxes(tickvals=tick_vals_e, ticktext=tick_txt_e, range=[0, y_cap_e])
        
        if show_labels:
            fig_emis.update_traces(text=base_emis_raw['faturamento'].apply(format_pt_br_abrev), textposition='outside')
            
        st.plotly_chart(fig_emis, width="stretch", config={'displayModeBar': False})
    else:
        st.info("Sem dados.")

    st.markdown("<div style='margin-bottom: 30px;'></div>", unsafe_allow_html=True)

    # ==================== GRÁFICO 3: SHARE DE MERCADO ====================
    st.markdown("<p class='custom-chart-title'>3. Share Faturamento (%)</p>", unsafe_allow_html=True)
    
    anos_presentes = sorted(base_periodo["ano"].dropna().unique())
    if anos_presentes:
        cols_share = st.columns(len(anos_presentes))
        
        for idx, ano_share in enumerate(anos_presentes):
            df_share_ano = base_periodo[base_periodo["ano"] == ano_share].groupby("emissora", as_index=False)["faturamento"].sum()
            
            if not df_share_ano.empty:
                fig_share = px.pie(
                    df_share_ano, 
                    values="faturamento", 
                    names="emissora",
                    color="emissora",
                    color_discrete_map=COLOR_MAP,
                    hole=0.6 
                )
                fig_share.update_traces(textposition='inside', textinfo='percent+label')
                
                fig_share.add_annotation(
                    text=f"<b>{ano_share}</b>", 
                    x=0.5, y=0.5, 
                    showarrow=False, 
                    font_size=20,
                    xanchor='center',
                    yanchor='middle'
                )

                fig_share.update_layout(
                    height=300, 
                    showlegend=False, 
                    margin=dict(l=10, r=10, t=10, b=10),
                    dragmode=False
                )
                
                figs_share_dict[f"3. Share de Faturamento (Gráfico {ano_share})"] = fig_share
                
                with cols_share[idx]:
                    st.plotly_chart(fig_share, width="stretch", config={'displayModeBar': False})
            else:
                with cols_share[idx]:
                    st.info(f"Sem dados para {ano_share}")
    else:
        st.info("Sem dados para gerar gráfico de share.")

    st.markdown("<div style='margin-bottom: 30px;'></div>", unsafe_allow_html=True)

    # ==================== GRÁFICO 4: FATURAMENTO POR EXECUTIVO ====================
    st.markdown("<p class='custom-chart-title'>4. Faturamento por Executivo (Ano a Ano)</p>", unsafe_allow_html=True)
    
    base_exec_raw = base_periodo.groupby(["executivo", "ano"], as_index=False)["faturamento"].sum()
    fig_exec = None 

    if not base_exec_raw.empty:
        rank_exec = base_exec_raw.groupby("executivo")["faturamento"].sum().sort_values(ascending=False).index.tolist()
        base_exec_raw["executivo"] = pd.Categorical(base_exec_raw["executivo"], categories=rank_exec, ordered=True)
        base_exec_raw = base_exec_raw.sort_values(["executivo", "ano"])
        base_exec_raw["label_x"] = base_exec_raw["executivo"].astype(str) + " " + base_exec_raw["ano"].astype(str)
        
        fig_exec = px.bar(
            base_exec_raw, 
            x="label_x", 
            y="faturamento", 
            color="executivo",
            color_discrete_sequence=px.colors.qualitative.Bold 
        )
        
        max_y_ex = base_exec_raw['faturamento'].max()
        tick_vals_x, tick_txt_x, y_cap_x = get_pretty_ticks(max_y_ex)
        
        fig_exec.update_layout(
            height=450, xaxis_title=None, yaxis_title=None, 
            template="plotly_white", showlegend=False,
            bargap=0.2,
            dragmode=False,
            xaxis=dict(fixedrange=True),
            yaxis=dict(fixedrange=True)
        )
        fig_exec.update_traces(width=0.5)

        fig_exec.update_yaxes(tickvals=tick_vals_x, ticktext=tick_txt_x, range=[0, y_cap_x])
        
        if show_labels:
            fig_exec.update_traces(text=base_exec_raw['faturamento'].apply(format_pt_br_abrev), textposition='outside')
            
        st.plotly_chart(fig_exec, width="stretch", config={'displayModeBar': False})
    else:
        st.info("Sem dados.")

    # ==================== SEÇÃO DE EXPORTAÇÃO (CENTRALIZADA) ====================
    st.divider()
    def get_filter_string():
        f = st.session_state 
        ano_ini = f.get("filtro_ano_ini", "N/A")
        ano_fim = f.get("filtro_ano_fim", "N/A")
        emis = ", ".join(f.get("filtro_emis", ["Todas"]))
        execs = ", ".join(f.get("filtro_execs", ["Todos"]))
        meses = ", ".join(f.get("filtro_meses_lista", ["Todos"]))
        clientes = ", ".join(f.get("filtro_clientes", ["Todos"])) if f.get("filtro_clientes") else "Todos"
        return (f"Período (Ano): {ano_ini} a {ano_fim} | Meses: {meses} | "
                f"Emissoras: {emis} | Executivos: {execs} | Clientes: {clientes}")

    # Lógica de Centralização do Botão
    c_left, c_btn, c_right = st.columns([3, 2, 3])
    with c_btn:
        if st.button("Exportar Dados da Página", type="secondary", use_container_width=True):
            st.session_state.show_visao_geral_export = True
    
    if ultima_atualizacao:
        st.markdown(f"<div style='text-align: center; color: grey; font-size: 0.8rem; margin-top: 5px;'>Última atualização da base de dados: {ultima_atualizacao}</div>", unsafe_allow_html=True)

    if st.session_state.get("show_visao_geral_export", False):
        @st.dialog("Opções de Exportação - Visão Geral")
        def export_dialog():
            final_ordered_options = {}

            # 1. Evolução (AGORA TABELA)
            if not evol_raw.empty:
                df_evol_exp = evol_raw[["ano", "meslabel", "mes", "faturamento", "insercoes"]].copy()
                df_evol_exp.columns = ["Ano", "Mês", "Mês ID", "Faturamento", "Inserções"]
                final_ordered_options["1. Evolução Mensal de Faturamento e Inserções (Dados)"] = {'df': df_evol_exp}

            # 2. Emissora (CORRIGIDO: Remover label_x)
            if not base_emis_raw.empty:
                df_emis_exp = base_emis_raw[["emissora", "ano", "faturamento"]].copy()
                df_emis_exp.columns = ["Emissora", "Ano", "Faturamento"]
                final_ordered_options["2. Faturamento por Emissora (Dados)"] = {'df': df_emis_exp}
                final_ordered_options["2. Faturamento por Emissora (Gráfico)"] = {'fig': fig_emis if not base_emis_raw.empty else None}

            # 3. Share (CORRIGIDO: Remover label_x, usar estrutura limpa)
            if not base_emis_raw.empty:
                df_share_exp = base_emis_raw[["emissora", "ano", "faturamento"]].copy()
                df_share_exp.columns = ["Emissora", "Ano", "Faturamento"]
                final_ordered_options["3. Share de Faturamento (Dados)"] = {'df': df_share_exp}
                for name, fig in figs_share_dict.items():
                    final_ordered_options[name] = {'fig': fig}

            # 4. Executivo (CORRIGIDO: Remover label_x)
            if not base_exec_raw.empty:
                df_exec_exp = base_exec_raw[["executivo", "ano", "faturamento"]].copy()
                df_exec_exp.columns = ["Executivo", "Ano", "Faturamento"]
                final_ordered_options["4. Faturamento por Executivo (Dados)"] = {'df': df_exec_exp}
                final_ordered_options["4. Faturamento por Executivo (Gráfico)"] = {'fig': fig_exec if not base_exec_raw.empty else None}

            # Filtra apenas o que tem conteúdo válido
            available_options = [k for k, v in final_ordered_options.items() if (v.get('df') is not None and not v['df'].empty) or (v.get('fig') is not None)]
            
            if not available_options:
                st.warning("Nenhuma tabela ou gráfico com dados foi gerado.")
                if st.button("Fechar", type="secondary"):
                    st.session_state.show_visao_geral_export = False
                    st.rerun()
                return

            selected_names = st.multiselect("Selecione os itens para exportar:", options=available_options, default=available_options)
            
            tables_to_export = {name: final_ordered_options[name] for name in selected_names}

            if not tables_to_export:
                st.error("Selecione pelo menos um item.")
                return

            try:
                filtro_str = get_filter_string()
                nome_interno_excel = "Dashboard_Visao_Geral.xlsx"
                zip_data = create_zip_package(tables_to_export, filtro_str, excel_filename=nome_interno_excel) 
                
                st.download_button(
                    label="Clique para baixar", 
                    data=zip_data, 
                    file_name="Dashboard_VisaoGeral.zip", 
                    mime="application/zip", 
                    on_click=lambda: st.session_state.update(show_visao_geral_export=False), 
                    type="secondary"
                )
            except Exception as e:
                st.error(f"Erro ao gerar ZIP: {e}")

            if st.button("Cancelar", key="cancel_export", type="secondary"):
                st.session_state.show_visao_geral_export = False
                st.rerun()
        export_dialog()