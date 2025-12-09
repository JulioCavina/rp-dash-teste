# pages/top10.py

import streamlit as st
import plotly.express as px
from utils.format import brl, PALETTE
from utils.export import create_zip_package 
import pandas as pd
import plotly.graph_objects as go
import numpy as np

def format_pt_br_abrev(val):
    if pd.isna(val): return "R$ 0" 
    sign = "-" if val < 0 else ""
    val_abs = abs(val)
    if val_abs == 0: return "R$ 0"
    if val_abs >= 1_000_000: return f"{sign}R$ {val_abs/1_000_000:,.1f} Mi".replace(",", "X").replace(".", ",").replace("X", ".")
    if val_abs >= 1_000: return f"{sign}R$ {val_abs/1_000:,.0f} mil".replace(",", "X").replace(".", ",").replace("X", ".")
    return brl(val)

def format_int_abrev(val):
    if pd.isna(val) or val == 0: return "0"
    if val >= 1000: return f"{val/1000:,.1f}k".replace(".", ",")
    return f"{int(val)}"

def get_pretty_ticks(max_val, num_ticks=5, is_currency=True):
    if max_val <= 0: 
        return [0], ["R$ 0"] if is_currency else ["0"], 100 
    
    ideal_interval = max_val / num_ticks
    magnitude = 10**np.floor(np.log10(ideal_interval)) if ideal_interval > 0 else 1
    residual = ideal_interval / magnitude
    
    if residual < 1.5: nice_interval = 1 * magnitude
    elif residual < 3: nice_interval = 2 * magnitude
    elif residual < 7: nice_interval = 5 * magnitude
    else: nice_interval = 10 * magnitude
    
    max_y_rounded = np.ceil(max_val / nice_interval) * nice_interval
    tick_values = np.arange(0, max_y_rounded + nice_interval, nice_interval)
    
    if is_currency:
        tick_texts = [format_pt_br_abrev(v) for v in tick_values]
    else:
        tick_texts = [format_int_abrev(v) for v in tick_values]
        
    y_axis_cap = max_y_rounded * 1.05
    return tick_values, tick_texts, y_axis_cap

def format_int(val):
    """Formata inteiros com separador de milhar."""
    if pd.isna(val) or val == 0: return "-"
    return f"{int(val):,}".replace(",", ".")

# ==================== FUNÇÃO AUXILIAR DE ESTILO ====================
def display_styled_table(df):
    """
    Renderiza o dataframe aplicando estilo de destaque (Totalizador) na última linha.
    """
    if df.empty: return

    def highlight_total_row(row):
        if row.name == (len(df) - 1): # Última linha (Totalizador)
            return ['background-color: #e6f3ff; font-weight: bold; color: #003366'] * len(row)
        return [''] * len(row)

    st.dataframe(
        df.style.apply(highlight_total_row, axis=1), 
        width="stretch", 
        hide_index=True,
        column_config={"#": st.column_config.TextColumn("#", width="small")}
    )

def render(df, mes_ini, mes_fim, show_labels, show_total, ultima_atualizacao=None):
    # ==================== TÍTULO CENTRALIZADO ====================
    st.markdown("<h2 style='text-align: center; color: #003366;'>Top 10 Maiores Anunciantes</h2>", unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom: 20px;'></div>", unsafe_allow_html=True)

    top10_raw = pd.DataFrame()
    fig = go.Figure() 
    top10_raw_export = pd.DataFrame()

    df = df.rename(columns={c: c.lower() for c in df.columns})
    if "emissora" not in df.columns or "ano" not in df.columns:
        st.error("Colunas 'Emissora' e/ou 'Ano' ausentes.")
        return
    
    # Garante Inserções
    if "insercoes" not in df.columns:
        df["insercoes"] = 0.0

    # Filtra período (Mês)
    base_periodo = df[df["mes"].between(mes_ini, mes_fim)]
    
    # Listas para os seletores
    emis_list = sorted(base_periodo["emissora"].dropna().unique())
    anos_list = sorted(base_periodo["ano"].dropna().unique())

    if not emis_list or not anos_list:
        st.info("Sem dados para selecionar emissora/ano.")
        return

    # ==================== FILTROS ====================
    # Inicializa estado do botão se não existir
    if "top10_metric" not in st.session_state:
        st.session_state.top10_metric = "Faturamento"
    
    criterio = st.session_state.top10_metric

    col1, col2, col3 = st.columns([1.5, 1, 2.5])
    
    # Opção de Consolidado para Emissora
    opcoes_emissora = ["Consolidado (Seleção Atual)"] + emis_list
    
    # Opção de Consolidado para Ano
    opcoes_ano = ["Consolidado (Seleção Atual)"] + anos_list
    
    emis_sel = col1.selectbox("Emissora / Visão", opcoes_emissora)
    
    # Default: Último ano da lista (que é o último item de opcoes_ano)
    default_ano_idx = len(opcoes_ano) - 1
    ano_sel = col2.selectbox("Ano", opcoes_ano, index=default_ano_idx)
    
    # --- BOTÕES ESTILIZADOS ---
    with col3:
        st.markdown('<p style="font-size:0.85rem; font-weight:600; margin-bottom: 0px;">Classificar por:</p>', unsafe_allow_html=True)
        b1, b2, b3 = st.columns(3)
        
        type_fat = "primary" if criterio == "Faturamento" else "secondary"
        type_ins = "primary" if criterio == "Inserções" else "secondary"
        type_efc = "primary" if criterio == "Eficiência" else "secondary"
        
        if b1.button("Faturamento", type=type_fat, use_container_width=True):
            st.session_state.top10_metric = "Faturamento"
            st.rerun()
            
        if b2.button("Inserções", type=type_ins, use_container_width=True):
            st.session_state.top10_metric = "Inserções"
            st.rerun()

        if b3.button("Eficiência", type=type_efc, help="Menor Custo Unitário", use_container_width=True):
            st.session_state.top10_metric = "Eficiência"
            st.rerun()

    # ==================== LÓGICA DE FILTRAGEM ====================
    # 1. Filtro de Emissora
    if emis_sel == "Consolidado (Seleção Atual)":
        base = base_periodo.copy()
        cor_grafico = PALETTE[3] # Azul Escuro
    else:
        base = base_periodo[base_periodo["emissora"] == emis_sel].copy()
        cor_grafico = PALETTE[0] # Azul Claro

    # 2. Filtro de Ano
    if ano_sel != "Consolidado (Seleção Atual)":
        base = base[base["ano"] == ano_sel]

    # ==================== PROCESSAMENTO ====================
    # Agrupa por cliente somando métricas
    top10_raw = base.groupby("cliente", as_index=False).agg(
        faturamento=("faturamento", "sum"),
        insercoes=("insercoes", "sum")
    )
    
    # Calcula Custo Unitário
    top10_raw["custo_unitario"] = np.where(
        top10_raw["insercoes"] > 0, 
        top10_raw["faturamento"] / top10_raw["insercoes"], 
        np.nan
    )

    # Ordena pelo critério selecionado
    if criterio == "Faturamento":
        col_sort = "faturamento"
        ascending = False
    elif criterio == "Inserções":
        col_sort = "insercoes"
        ascending = False
    else: # Eficiência
        col_sort = "custo_unitario"
        ascending = True 
        top10_raw = top10_raw[top10_raw["insercoes"] > 0]

    # Pega Top 10
    top10_raw = top10_raw.sort_values(col_sort, ascending=ascending).head(10)

    if not top10_raw.empty:
        # Tabela com Totalizador para exportação
        top10_with_total = top10_raw.copy()
        
        # Lógica Totalizador
        if show_total:
            tot_fat = top10_with_total["faturamento"].sum()
            tot_ins = top10_with_total["insercoes"].sum()
            tot_custo = tot_fat / tot_ins if tot_ins > 0 else np.nan

            total_row = {
                "cliente": "Totalizador", 
                "faturamento": tot_fat,
                "insercoes": tot_ins,
                "custo_unitario": tot_custo
            }
            top10_with_total = pd.concat([top10_with_total, pd.DataFrame([total_row])], ignore_index=True)
        
        if show_total:
             top10_with_total.insert(0, "#", list(range(1, len(top10_raw) + 1)) + ["Total"])
        else:
             top10_with_total.insert(0, "#", list(range(1, len(top10_raw) + 1)))
        
        top10_raw_export = top10_with_total.copy()

        # Display Tabela
        top10_display = top10_with_total.copy()
        top10_display['#'] = top10_display['#'].astype(str)
        top10_display["faturamento_fmt"] = top10_display["faturamento"].apply(brl)
        top10_display["insercoes_fmt"] = top10_display["insercoes"].apply(format_int)
        top10_display["custo_fmt"] = top10_display["custo_unitario"].apply(brl)
        
        tabela = top10_display[["#", "cliente", "faturamento_fmt", "insercoes_fmt", "custo_fmt"]].rename(columns={
            "cliente": "Cliente", 
            "faturamento_fmt": "Faturamento",
            "insercoes_fmt": "Inserções",
            "custo_fmt": "Custo Médio"
        })
        
        display_styled_table(tabela)

        # Display Gráfico
        is_currency = (criterio == "Faturamento" or criterio == "Eficiência")
        
        if criterio == "Faturamento":
            y_col, y_label = "faturamento", "Faturamento (R$)"
        elif criterio == "Inserções":
            y_col, y_label = "insercoes", "Inserções (Qtd)"
        else:
            y_col, y_label = "custo_unitario", "Custo Unitário (R$)"
        
        if criterio == "Eficiência":
            cor_grafico_final = "#16a34a" # Verde
        else:
            cor_grafico_final = cor_grafico

        fig = px.bar(
            top10_raw.head(10), 
            x="cliente", 
            y=y_col, 
            color_discrete_sequence=[cor_grafico_final], 
            labels={"cliente": "Cliente", y_col: y_label}
        )
        
        max_y = top10_raw.head(10)[y_col].max()
        tick_values, tick_texts, y_axis_cap = get_pretty_ticks(max_y, is_currency=is_currency)
        
        fig.update_layout(height=400, showlegend=False, template="plotly_white")
        fig.update_yaxes(tickvals=tick_values, ticktext=tick_texts, range=[0, y_axis_cap], title=y_label)
        
        # --- TRAVA DE INTERAÇÃO ---
        fig.update_xaxes(fixedrange=True)
        fig.update_yaxes(fixedrange=True)
        
        if show_labels:
            format_func = format_pt_br_abrev if is_currency else format_int_abrev
            fig.update_traces(text=top10_raw.head(10)[y_col].apply(format_func), textposition='outside')
        
        st.plotly_chart(fig, width="stretch", config={'displayModeBar': False}) 
    else: 
        st.info("Sem dados para essa seleção (ou valores zerados).")

    st.divider()
    
    # Exportação
    def get_filter_string():
        f = st.session_state 
        ano_ini = f.get("filtro_ano_ini", "N/A")
        ano_fim = f.get("filtro_ano_fim", "N/A")
        emis = ", ".join(f.get("filtro_emis", ["Todas"]))
        execs = ", ".join(f.get("filtro_execs", ["Todos"]))
        meses = ", ".join(f.get("filtro_meses_lista", ["Todos"]))
        clientes = ", ".join(f.get("filtro_clientes", ["Todos"])) if f.get("filtro_clientes") else "Todos"
        return (f"Período (Ano): {ano_ini} a {ano_fim} | Meses: {meses} | Emissoras: {emis} | Executivos: {execs} | Clientes: {clientes}")

    if st.button("📥 Exportar Dados da Página", type="secondary"):
        st.session_state.show_top10_export = True
    
    if ultima_atualizacao:
        st.caption(f"📅 Última atualização da base de dados: {ultima_atualizacao}")

    if st.session_state.get("show_top10_export", False):
        @st.dialog("Opções de Exportação - Top 10")
        def export_dialog():
            nome_arq = "Global" if emis_sel.startswith("Consolidado") else emis_sel
            
            # Tratamento para nome do arquivo
            ano_arq = "Consolidado" if str(ano_sel).startswith("Consolidado") else str(ano_sel)
            criterio_arq = criterio.replace(" ", "_")
            
            # Prepara DF para exportação
            df_exp = top10_raw_export.rename(columns={
                "cliente": "Cliente", 
                "faturamento": "Faturamento",
                "insercoes": "Inserções",
                "custo_unitario": "Custo Médio"
            }) if not top10_raw_export.empty else None

            all_options = {
                "Top 10 Maiores Anunciantes (Dados)": {'df': df_exp}, 
                "Top 10 Maiores Anunciantes (Gráfico)": {'fig': fig}
            }
            available_options = [name for name, data in all_options.items() if (data.get('df') is not None and not data['df'].empty) or (data.get('fig') is not None and data['fig'].data)]
            
            if not available_options:
                st.warning("Nenhuma tabela com dados foi gerada.")
                if st.button("Fechar", type="secondary"):
                    st.session_state.show_top10_export = False
                    st.rerun()
                return

            st.write("Selecione os itens para exportar:")
            selected_names = st.multiselect("Itens", options=available_options, default=available_options)
            tables_to_export = {name: all_options[name] for name in selected_names}
            
            if not tables_to_export:
                st.error("Selecione pelo menos um item.")
                return

            try:
                filtro_str = get_filter_string()
                filtro_str += f" | Visão Top 10: {emis_sel} | Critério: {criterio} | Ano Base: {ano_sel}"
                
                # NOME DO ARQUIVO EXCEL INTERNO
                nome_interno_excel = "Dashboard_Top10.xlsx"
                zip_filename = f"Dashboard_Top10.zip"
                
                zip_data = create_zip_package(tables_to_export, filtro_str, excel_filename=nome_interno_excel)
                
                st.download_button(
                    label="Clique para baixar", 
                    data=zip_data, 
                    file_name=zip_filename, 
                    mime="application/zip", 
                    on_click=lambda: st.session_state.update(show_top10_export=False), 
                    type="secondary"
                )
            except Exception as e:
                st.error(f"Erro ao gerar ZIP: {e}")

            if st.button("Cancelar", key="cancel_export", type="secondary"):
                st.session_state.show_top10_export = False
                st.rerun()
        export_dialog()