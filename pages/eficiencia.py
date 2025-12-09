# pages/eficiencia.py

import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
from utils.format import brl, PALETTE
from utils.export import create_zip_package 

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
</style>
"""

def format_int(val):
    if pd.isna(val) or val == 0: return "-"
    return f"{int(val):,}".replace(",", ".")

# ==================== FUNÇÃO AUXILIAR DE ESTILO ====================
def display_styled_table(df, highlight_total=True, column_config=None):
    """
    Renderiza o dataframe aplicando estilo de destaque (Totalizador) na última linha.
    """
    if df.empty: return

    def highlight_total_row(row):
        # Verifica se deve destacar E se é a última linha (assumindo Totalizador no fim)
        if highlight_total and row.name == (len(df) - 1): 
            return ['background-color: #e6f3ff; font-weight: bold; color: #003366'] * len(row)
        return [''] * len(row)

    st.dataframe(
        df.style.apply(highlight_total_row, axis=1), 
        width="stretch", 
        hide_index=True,
        column_config=column_config if column_config else {"#": st.column_config.TextColumn("#", width="small")}
    )

def render(df, mes_ini, mes_fim, show_labels, show_total, ultima_atualizacao=None):
    # Aplica CSS para centralizar os cards
    st.markdown(ST_METRIC_CENTER, unsafe_allow_html=True)
    
    st.markdown("<h2 style='text-align: center; color: #003366;'>Eficiência & KPIs Avançados</h2>", unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom: 20px;'></div>", unsafe_allow_html=True)

    # Normalização e Filtros
    df = df.rename(columns={c: c.lower() for c in df.columns})
    
    if "faturamento" not in df.columns or "cliente" not in df.columns:
        st.error("Colunas obrigatórias ausentes.")
        return
    
    if "insercoes" not in df.columns:
        df["insercoes"] = 0.0

    # Definição dos Anos para lógica de colunas
    anos_global = sorted(df["ano"].dropna().unique())
    if len(anos_global) >= 2:
        ano_base, ano_comp = anos_global[-2], anos_global[-1]
    elif len(anos_global) == 1:
        ano_base = ano_comp = anos_global[0]
    else:
        ano_base = ano_comp = 2024 # Fallback

    # Filtra período
    base_periodo = df[df["mes"].between(mes_ini, mes_fim)]
    
    # Filtra apenas quem tem faturamento > 0
    base_analise = base_periodo[base_periodo["faturamento"] > 0].copy()

    if base_analise.empty:
        st.info("Sem dados financeiros para o período.")
        return

    # ==================== CÁLCULOS DE KPI (MACRO - CONSOLIDADO) ====================
    total_fat = base_analise["faturamento"].sum()
    total_ins = base_analise["insercoes"].sum()
    total_cli = base_analise["cliente"].nunique()
    
    # Yield Global (Preço por 1 Inserção)
    custo_medio_global = (total_fat / total_ins) if total_ins > 0 else 0
    
    # Média de Inserções por Cliente (Substituindo o CPM)
    media_ins_cli = (total_ins / total_cli) if total_cli > 0 else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Yield Médio (R$ / Inserção)", brl(custo_medio_global), help="Valor médio pago por uma única inserção.")
    col2.metric("Volume Médio (Ins. / Cliente)", f"{int(media_ins_cli)}", help="Média de inserções veiculadas por cada cliente.")
    col3.metric("Volume Total Entregue", f"{int(total_ins):,}".replace(",", "."))

    st.divider()

    # ==================== 1. MATRIZ DE EFICIÊNCIA (COM FILTRO DE ANO) ====================
    st.subheader("1. Matriz de Eficiência (Preço vs. Volume)")

    # Seletor de Ano
    anos_disponiveis = sorted(base_analise["ano"].dropna().unique())
    opcoes_ano = ["Consolidado (Seleção Atual)"] + anos_disponiveis
    
    # Default: Último ano da lista (index -1 de anos_disponiveis, mas ajustado para lista completa)
    default_idx = len(opcoes_ano) - 1 
    
    col_sel, _ = st.columns([1, 2])
    ano_sel = col_sel.selectbox("Selecione o Ano:", opcoes_ano, index=default_idx)

    # Filtragem Local
    if ano_sel == "Consolidado (Seleção Atual)":
        df_matriz = base_analise.copy()
        titulo_matriz = "Consolidado"
    else:
        df_matriz = base_analise[base_analise["ano"] == ano_sel].copy()
        titulo_matriz = str(ano_sel)

    # Agrupa dados para o Gráfico
    scatter_data = df_matriz.groupby(["cliente", "emissora"], as_index=False).agg(
        Faturamento=("faturamento", "sum"),
        Insercoes=("insercoes", "sum")
    )
    
    # Calcula custo médio
    scatter_data["Custo_Medio"] = scatter_data["Faturamento"] / scatter_data["Insercoes"].replace(0, 1)
    # Filtra zeros
    scatter_data = scatter_data[scatter_data["Insercoes"] > 0]

    # Cores
    color_map = {
        "Novabrasil": "#007dc3", 
        "Difusora": "#ef4444", 
    }

    if not scatter_data.empty:
        fig_scatter = px.scatter(
            scatter_data,
            x="Insercoes",
            y="Custo_Medio",
            size="Faturamento",
            color="emissora",
            hover_name="cliente",
            log_x=False, 
            template="plotly_white",
            labels={
                "Insercoes": "Volume de Inserções (Qtd)",
                "Custo_Medio": "Preço Médio Pago (R$)",
                "emissora": "Emissora",
                "Faturamento": "Investimento Total"
            },
            color_discrete_map=color_map, 
            color_discrete_sequence=PALETTE 
        )
        
        # Linhas médias dinâmicas
        avg_x = scatter_data["Insercoes"].median()
        avg_y = scatter_data["Custo_Medio"].median()
        
        fig_scatter.add_hline(y=avg_y, line_dash="dot", annotation_text="Preço Médio", annotation_position="bottom right")
        fig_scatter.add_vline(x=avg_x, line_dash="dot", annotation_text="Vol. Médio", annotation_position="top right")

        # BLOQUEIO DE INTERAÇÃO (Zoom/Pan fixos)
        fig_scatter.update_layout(
            height=500,
            dragmode=False, # Desabilita ferramenta de seleção/arrasto
            xaxis=dict(fixedrange=True), # Trava eixo X
            yaxis=dict(fixedrange=True)  # Trava eixo Y
        )
        
        # config={'displayModeBar': False} remove a barra de ferramentas do Plotly
        st.plotly_chart(fig_scatter, width="stretch", config={'displayModeBar': False})
    else:
        st.warning(f"Sem dados de inserções para o ano {titulo_matriz}.")

    # ==================== TABELAS DE EXPORTAÇÃO (INICIALIZAÇÃO) ====================
    df_matriz_export = pd.DataFrame()
    
    # ==================== TABELA DETALHADA (AFETADA PELO FILTRO) ====================
    with st.expander(f"Ver dados detalhados da Matriz ({titulo_matriz})", expanded=True):
        if not scatter_data.empty:
            df_table = scatter_data.copy()
            
            # Formatação
            df_table["Faturamento_fmt"] = df_table["Faturamento"].apply(brl)
            df_table["Custo_Medio_fmt"] = df_table["Custo_Medio"].apply(brl)
            df_table["Insercoes_fmt"] = df_table["Insercoes"].apply(format_int)
            
            # Seleção e Ordenação (Inserções ANTES de Faturamento)
            df_table = df_table[["cliente", "emissora", "Insercoes_fmt", "Faturamento_fmt", "Custo_Medio_fmt"]]
            
            # Renomeia para UI
            df_table.columns = ["Cliente", "Emissora", "Inserções", "Faturamento Total", "CMU"]
            
            # Ordena por Cliente
            df_table = df_table.sort_values("Cliente", ascending=True).reset_index(drop=True)
            
            # Display com Tooltip
            display_styled_table(
                df_table, 
                highlight_total=False,
                column_config={
                    "CMU": st.column_config.Column(
                        label="CMU ℹ️",
                        help="Custo Médio Unitário (Preço Médio Pago por Inserção)"
                    )
                }
            )

            # Preparação para Exportação (Cópia fiel + Renomeação solicitada)
            df_matriz_export = scatter_data.copy()
            # Reordena colunas RAW: Cliente, Emissora, Inserções, Faturamento, Custo
            df_matriz_export = df_matriz_export[["cliente", "emissora", "Insercoes", "Faturamento", "Custo_Medio"]]
            
            # Renomeia para Excel
            df_matriz_export.columns = ["Cliente", "Emissora", "Inserções", "Faturamento Total", "Custo Médio Unitário"]
            df_matriz_export = df_matriz_export.sort_values("Cliente")
            
        else:
            st.info("Sem dados para exibir na tabela.")

    st.divider()

    # ==================== 2. RESUMO POR EMISSORA (COM DIVISÃO ANUAL) ====================
    st.subheader("2. Resumo de Eficiência por Emissora (Comparativo Anual)")
    
    # Pivotagem para separar por ano
    grp_ano = base_periodo.groupby(["emissora", "ano"]).agg(
        Faturamento=("faturamento", "sum"),
        Insercoes=("insercoes", "sum")
    ).unstack(fill_value=0)
    
    # Flatten nas colunas (Fat 2024, Fat 2025, etc.)
    grp_ano.columns = [f"{col[0]}_{col[1]}" for col in grp_ano.columns]
    grp_ano = grp_ano.reset_index()
    
    # Garante colunas dos anos base e comp se não existirem
    # CORREÇÃO: Evitar duplicidade se anos forem iguais
    anos_check = sorted(list(set([ano_base, ano_comp])))
    
    for ano in anos_check:
        if f"Faturamento_{ano}" not in grp_ano.columns: grp_ano[f"Faturamento_{ano}"] = 0.0
        if f"Insercoes_{ano}" not in grp_ano.columns: grp_ano[f"Insercoes_{ano}"] = 0.0

    # Calcula Yield Anual
    for ano in anos_check:
        grp_ano[f"Yield_{ano}"] = np.where(grp_ano[f"Insercoes_{ano}"] > 0, grp_ano[f"Faturamento_{ano}"] / grp_ano[f"Insercoes_{ano}"], 0.0)

    # Ordena pelo Yield do último ano (ou ano base se for o único)
    sort_year = ano_comp if f"Yield_{ano_comp}" in grp_ano.columns else ano_base
    if f"Yield_{sort_year}" in grp_ano.columns:
        grp_ano = grp_ano.sort_values(f"Yield_{sort_year}", ascending=False)

    # Totalizador (Condicionado ao botão)
    if not grp_ano.empty and show_total:
        total_row = {"emissora": "Totalizador"}
        
        # Soma para Fat e Ins, Média para Yield (recalculada)
        for ano in anos_check:
            sum_fat = grp_ano[f"Faturamento_{ano}"].sum()
            sum_ins = grp_ano[f"Insercoes_{ano}"].sum()
            avg_yld = sum_fat / sum_ins if sum_ins > 0 else 0
            
            total_row[f"Faturamento_{ano}"] = sum_fat
            total_row[f"Insercoes_{ano}"] = sum_ins
            total_row[f"Yield_{ano}"] = avg_yld
            
        grp_ano = pd.concat([grp_ano, pd.DataFrame([total_row])], ignore_index=True)

    # Display Formatado
    tb_display = grp_ano.copy()
    
    # Dicionário de Renomeação
    cols_rename = {"emissora": "Emissora"}
    for ano in anos_check:
        cols_rename[f"Insercoes_{ano}"] = f"Inserções ({ano})"
        cols_rename[f"Faturamento_{ano}"] = f"Faturamento ({ano})"
        cols_rename[f"Yield_{ano}"] = f"Yield Médio ({ano})"
        
    tb_display = tb_display.rename(columns=cols_rename)
    
    # Ordenação das colunas - LÓGICA ANTI-CRASH (Se anos iguais, mostra só 1 kit de colunas)
    if ano_base == ano_comp:
         cols_order = [
            "Emissora", 
            f"Inserções ({ano_base})",
            f"Faturamento ({ano_base})",
            f"Yield Médio ({ano_base})"
        ]
    else:
        cols_order = [
            "Emissora", 
            f"Inserções ({ano_base})", f"Inserções ({ano_comp})",
            f"Faturamento ({ano_base})", f"Faturamento ({ano_comp})",
            f"Yield Médio ({ano_base})", f"Yield Médio ({ano_comp})"
        ]
        
    # Filtra apenas colunas que existem (segurança extra)
    cols_order = [c for c in cols_order if c in tb_display.columns]
    tb_display = tb_display[cols_order]
    
    # Formatação
    for col in tb_display.columns:
        if "Faturamento" in col or "Yield" in col:
            tb_display[col] = tb_display[col].apply(brl)
        elif "Inserções" in col:
            tb_display[col] = tb_display[col].apply(format_int)
    
    # Passamos o show_total como highlight_total
    display_styled_table(tb_display, highlight_total=show_total)

    # ==================== EXPORTAÇÃO ====================
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

    if st.button("📥 Exportar Dados da Página", type="secondary"):
        st.session_state.show_efi_export = True
    
    if ultima_atualizacao:
        st.caption(f"📅 Última atualização da base de dados: {ultima_atualizacao}")

    if st.session_state.get("show_efi_export", False):
        @st.dialog("Opções de Exportação - Eficiência")
        def export_dialog():
            table_options = {
                "1. Matriz de Eficiência (Preço vs. Volume) (Dados)": {'df': df_matriz_export},
                "1. Matriz de Eficiência (Preço vs. Volume) (Gráfico)": {'fig': fig_scatter if not scatter_data.empty else None},
                "2. Resumo de Eficiência por Emissora (Comparativo Anual) (Dados)": {'df': tb_display} # Usa tb_display pq já está com nomes bonitos e ordem correta
            }
            
            available_options = [name for name, data in table_options.items() if (data.get('df') is not None and not data['df'].empty) or (data.get('fig') is not None)]
            
            if not available_options:
                st.warning("Sem dados para exportar.")
                if st.button("Fechar", type="secondary"):
                    st.session_state.show_efi_export = False
                    st.rerun()
                return

            st.write("Selecione os itens para exportar:")
            selected_names = st.multiselect("Itens", options=available_options, default=available_options)
            tables_to_export = {name: table_options[name] for name in selected_names}
            
            if not tables_to_export:
                st.error("Selecione pelo menos um item.")
                return

            try:
                filtro_str = get_filter_string()
                
                # Nomes corretos para ZIP e Excel Interno
                nome_interno_excel = "Dashboard_Eficiencia.xlsx"
                zip_filename = "Dashboard_Eficiencia.zip"
                
                zip_data = create_zip_package(tables_to_export, filtro_str, excel_filename=nome_interno_excel)
                
                st.download_button(
                    label="Clique para baixar", 
                    data=zip_data, 
                    file_name=zip_filename, 
                    mime="application/zip", 
                    on_click=lambda: st.session_state.update(show_efi_export=False), 
                    type="secondary"
                )
            except Exception as e:
                st.error(f"Erro ao gerar ZIP: {e}")

            if st.button("Cancelar", key="cancel_export", type="secondary"):
                st.session_state.show_efi_export = False
                st.rerun()
        export_dialog()