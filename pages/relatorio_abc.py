# pages/relatorio_abc.py

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from utils.format import brl, PALETTE
from utils.export import create_zip_package 

# ==================== ESTILO CSS LOCAL (PÁGINA ABC) ====================
# Ajustes específicos para esta página:
# 1. Centralização dos Cards (KPIs).
# 2. Ajuste dos botões de filtro no mobile (lado a lado e fonte menor).
ST_PAGE_STYLES = """
<style>
/* Centraliza Cards de Métricas (KPIs) */
[data-testid="stMetric"] {
    display: flex;
    flex-direction: column;
    align-items: center; 
    justify-content: center; 
    text-align: center;
    width: 100%;
    margin: auto;
}
[data-testid="stMetricLabel"], [data-testid="stMetricValue"], [data-testid="stMetricDelta"] {
    justify-content: center;
    width: 100%;
}

/* MOBILE: Botões de Filtro (Faturamento/Inserções) Lado a Lado */
@media only screen and (max-width: 768px) {
    /* Força fonte menor nos botões desta página para caberem lado a lado */
    div.row-widget.stButton > button {
        font-size: 0.75rem !important;
        padding: 0.25rem 0.5rem !important;
        line-height: 1.2 !important;
        min-height: 0px !important; 
        height: auto !important;
    }
}
</style>
"""

def format_int(val):
    """Formata inteiros com separador de milhar."""
    if pd.isna(val) or val == 0: return "-"
    return f"{int(val):,}".replace(",", ".")

def render(df, mes_ini, mes_fim, show_labels, show_total, ultima_atualizacao=None):
    # INJEÇÃO DO CSS LOCAL
    st.markdown(ST_PAGE_STYLES, unsafe_allow_html=True)

    # ==================== TÍTULO CENTRALIZADO ====================
    st.markdown("<h2 style='text-align: center; color: #003366; width: 100%;'>Relatório ABC (Pareto)</h2>", unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom: 20px;'></div>", unsafe_allow_html=True)

    # Legenda explicativa
    st.markdown("""
    <div style='font-size: 0.9rem; color: #555; margin-bottom: 10px; text-align: center;'>
    <b>Classificação:</b> 
    <span style='color:#FFD700; font-weight:bold; text-shadow: 1px 1px 1px #999;'>Classe A</span> (até 80%) • 
    <span style='color:#A9A9A9; font-weight:bold; text-shadow: 1px 1px 1px #ccc;'>Classe B</span> (próximos 15%) • 
    <span style='color:#A0522D; font-weight:bold;'>Classe C</span> (últimos 5%)
    </div>
    """, unsafe_allow_html=True)

    # Inicializa variáveis
    fig_pie = None 

    # Normalização
    df = df.rename(columns={c: c.lower() for c in df.columns})
    
    if "cliente" not in df.columns or "faturamento" not in df.columns:
        st.error("Colunas obrigatórias ausentes.")
        return
    if "insercoes" not in df.columns:
        df["insercoes"] = 0.0

    # Filtros
    base_periodo = df[df["mes"].between(mes_ini, mes_fim)]
    
    if base_periodo.empty:
        st.info("Sem dados para o período selecionado.")
        return

    # ==================== SELETOR DE MÉTRICA (CENTRALIZADO) ====================
    if "abc_metric" not in st.session_state:
        st.session_state.abc_metric = "Faturamento"
    
    criterio = st.session_state.abc_metric
    
    # Layout responsivo: 
    # Usamos colunas vazias nas laterais para centralizar no Desktop.
    # No Mobile, o CSS injetado reduz a fonte para caber.
    # [1, 2, 1] centraliza bem no desktop. No mobile o Streamlit tende a empilhar se faltar espaço,
    # mas o CSS de fonte menor ajuda a manter lado a lado se a tela permitir.
    _, col_sel, _ = st.columns([1, 2, 1])
    
    with col_sel:
        # Colunas internas para os botões ficarem lado a lado
        b1, b2 = st.columns(2)
        type_fat = "primary" if criterio == "Faturamento" else "secondary"
        type_ins = "primary" if criterio == "Inserções" else "secondary"
        
        # Labels simplificadas para garantir encaixe no mobile
        if b1.button("Por Faturamento (R$)", type=type_fat, use_container_width=True):
            st.session_state.abc_metric = "Faturamento"
            st.rerun()
            
        if b2.button("Por Inserções (Qtd)", type=type_ins, use_container_width=True):
            st.session_state.abc_metric = "Inserções"
            st.rerun()

    st.divider()

    # ==================== CÁLCULO DO ABC ====================
    df_abc = base_periodo.groupby("cliente", as_index=False).agg(
        faturamento=("faturamento", "sum"),
        insercoes=("insercoes", "sum")
    )
    
    target_col = "faturamento" if criterio == "Faturamento" else "insercoes"
    df_abc = df_abc.sort_values(target_col, ascending=False).reset_index(drop=True)
    
    total_target = df_abc[target_col].sum()
    df_abc["share"] = (df_abc[target_col] / total_target) if total_target > 0 else 0
    df_abc["acumulado"] = df_abc["share"].cumsum()
    
    def definir_classe(acum):
        if acum <= 0.80: return "A"
        elif acum <= 0.95: return "B"
        return "C"
    
    df_abc["classe"] = df_abc["acumulado"].apply(definir_classe)
    
    df_abc["custo_medio"] = np.where(
        df_abc["insercoes"] > 0, 
        df_abc["faturamento"] / df_abc["insercoes"], 
        np.nan
    )

    # ==================== KPIs DO TOPO ====================
    resumo_classes = df_abc.groupby("classe").agg(
        Qtd_Clientes=("cliente", "count"),
        Total_Faturamento=("faturamento", "sum"),
        Total_Insercoes=("insercoes", "sum")
    ).reindex(["A", "B", "C"]).fillna(0)
    
    c1, c2, c3 = st.columns(3)
    
    def get_kpi_display(row):
        if criterio == "Faturamento":
            return brl(row["Total_Faturamento"])
        else:
            return f"{int(row['Total_Insercoes']):,}".replace(",", ".") + " ins."

    qtd_a = int(resumo_classes.loc["A", "Qtd_Clientes"])
    val_a = get_kpi_display(resumo_classes.loc["A"])
    c1.metric("Classe A (Vitais)", f"{qtd_a} Clientes", val_a, border=True)
    
    qtd_b = int(resumo_classes.loc["B", "Qtd_Clientes"])
    val_b = get_kpi_display(resumo_classes.loc["B"])
    c2.metric("Classe B (Intermediários)", f"{qtd_b} Clientes", val_b, border=True)
    
    qtd_c = int(resumo_classes.loc["C", "Qtd_Clientes"])
    val_c = get_kpi_display(resumo_classes.loc["C"])
    c3.metric("Classe C (Cauda Longa)", f"{qtd_c} Clientes", val_c, border=True)

    st.divider()

    # ==================== GRÁFICO E TABELA ====================
    col_graf, col_tab = st.columns([1, 2])
    
    with col_graf:
        st.markdown("<p class='custom-chart-title'>1. Distribuição da Carteira (Clientes)</p>", unsafe_allow_html=True)
        
        abc_colors = {'A': '#FFD700', 'B': '#C0C0C0', 'C': '#A0522D'}

        fig_pie = px.pie(
            resumo_classes.reset_index(), 
            values='Qtd_Clientes', 
            names='classe', 
            color='classe',
            color_discrete_map=abc_colors,
            category_orders={"classe": ["A", "B", "C"]},
            hole=0.4
        )
        fig_pie.update_traces(textinfo='value')
        fig_pie.update_layout(height=350, margin=dict(t=20, b=20, l=20, r=20))
        st.plotly_chart(fig_pie, width="stretch")

    with col_tab:
        st.markdown("<p class='custom-chart-title'>2. Detalhamento dos Clientes</p>", unsafe_allow_html=True)
        
        df_display = df_abc.copy()
        df_display["share_fmt"] = (df_display["share"] * 100).apply(lambda x: f"{x:.2f}%")
        df_display["acum_fmt"] = (df_display["acumulado"] * 100).apply(lambda x: f"{x:.2f}%")
        df_display["faturamento_fmt"] = df_display["faturamento"].apply(brl)
        df_display["insercoes_fmt"] = df_display["insercoes"].apply(format_int)
        df_display["custo_fmt"] = df_display["custo_medio"].apply(lambda x: brl(x) if pd.notna(x) else "-")
        
        cols_order = ["classe", "cliente", "faturamento_fmt", "insercoes_fmt", "custo_fmt", "share_fmt", "acum_fmt"]
        df_display = df_display[cols_order]
        df_display.columns = ["Classe", "Cliente", "Faturamento", "Inserções", "Custo Médio", "Share %", "% Acumulado"]
        
        df_display.index = range(1, len(df_display) + 1)
        df_display.index.name = "RNK"
        
        st.dataframe(
            df_display, 
            height=350, 
            width="stretch",
            column_config={
                "Custo Médio": st.column_config.Column(
                    label="CMU ℹ️",
                    help="Custo Médio Unitário"
                )
            }
        )
        
    # ==================== EXPORTAÇÃO (CENTRALIZADA) ====================
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
                f"Emissoras: {emis} | Executivos: {execs} | Clientes: {clientes} | "
                f"Critério ABC: {criterio}")

    # Lógica de Centralização do Botão
    # Usamos colunas [4, 2, 4] para espremer o botão no meio sem esticá-lo (use_container_width=False)
    # Ajuste os ratios se achar que o botão está muito apertado ou largo
    c_left, c_btn, c_right = st.columns([3, 2, 3])
    
    with c_btn:
        # Botão sem emoji, centralizado pela coluna
        if st.button("Exportar Dados da Página", type="secondary", use_container_width=True):
            st.session_state.show_abc_export = True
    
    if ultima_atualizacao:
        # Texto centralizado sem emoji
        st.markdown(f"<div style='text-align: center; color: grey; font-size: 0.8rem; margin-top: 5px;'>Última atualização da base de dados: {ultima_atualizacao}</div>", unsafe_allow_html=True)

    if st.session_state.get("show_abc_export", False):
        @st.dialog("Opções de Exportação - Relatório ABC")
        def export_dialog():
            df_dist_exp = resumo_classes.reset_index().rename(columns={"classe": "Classe", "Qtd_Clientes": "Qtd Clientes"})
            
            if criterio == "Faturamento":
                df_dist_exp = df_dist_exp[["Classe", "Qtd Clientes", "Total_Faturamento"]]
                df_dist_exp = df_dist_exp.rename(columns={"Total_Faturamento": "Faturamento Total"})
            else:
                df_dist_exp = df_dist_exp[["Classe", "Qtd Clientes", "Total_Insercoes"]]
                df_dist_exp = df_dist_exp.rename(columns={"Total_Insercoes": "Inserções Totais"})
            
            df_det_exp = df_abc.copy()
            df_det_exp.index = range(1, len(df_det_exp) + 1)
            df_det_exp = df_det_exp.reset_index()
            
            df_det_exp = df_det_exp.rename(columns={
                "index": "RNK",
                "classe": "Classe",
                "cliente": "Cliente",
                "faturamento": "Faturamento",
                "insercoes": "Inserções",
                "custo_medio": "Custo Médio Unitário",
                "share": "Share %",
                "acumulado": "% Acumulado"
            })
            
            cols_export_order = ["RNK", "Classe", "Cliente", "Faturamento", "Inserções", "Custo Médio Unitário", "Share %", "% Acumulado"]
            df_det_exp = df_det_exp[cols_export_order]

            table_options = {
                "1. Distribuição da Carteira (Dados)": {'df': df_dist_exp},
                "1. Distribuição da Carteira (Gráfico)": {'fig': fig_pie}, 
                "2. Detalhamento dos Clientes (Dados)": {'df': df_det_exp}
            }
            
            available_options = [name for name, data in table_options.items() if (data.get('df') is not None and not data['df'].empty) or (data.get('fig') is not None)]
            
            if not available_options:
                st.warning("Sem dados para exportar.")
                if st.button("Fechar", type="secondary"):
                    st.session_state.show_abc_export = False
                    st.rerun()
                return

            selected_names = st.multiselect("Selecione os itens para exportar:", options=available_options, default=available_options)
            tables_to_export = {name: table_options[name] for name in selected_names}
            
            if not tables_to_export:
                st.error("Selecione pelo menos um item.")
                return

            try:
                filtro_str = get_filter_string()
                nome_interno_excel = "Dashboard_Relatorio_ABC.xlsx"
                zip_data = create_zip_package(tables_to_export, filtro_str, excel_filename=nome_interno_excel)
                
                st.download_button(
                    label="Clique para baixar", 
                    data=zip_data, 
                    file_name=f"Dashboard_Relatorio_ABC.zip", 
                    mime="application/zip", 
                    on_click=lambda: st.session_state.update(show_abc_export=False), 
                    type="secondary"
                )
            except Exception as e:
                st.error(f"Erro ao gerar ZIP: {e}")

            if st.button("Cancelar", key="cancel_export", type="secondary"):
                st.session_state.show_abc_export = False
                st.rerun()
        export_dialog()