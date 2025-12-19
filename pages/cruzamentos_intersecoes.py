# pages/cruzamentos_intersecoes.py

import streamlit as st
import pandas as pd
import numpy as np
from utils.format import brl, PALETTE
import plotly.graph_objects as go
import plotly.express as px
from itertools import combinations
from utils.export import create_zip_package 

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
        # Tenta identificar pela coluna
        is_total = False
        if "Emissora" in df.columns and row["Emissora"] == "Totalizador": is_total = True
        elif "Cliente" in df.columns and row["Cliente"] == "Totalizador": is_total = True
        
        if is_total:
            return ['background-color: #e6f3ff; font-weight: bold; color: #003366'] * len(row)
        return [''] * len(row)

    st.dataframe(
        df.style.apply(highlight_total_row, axis=1), 
        width='stretch', 
        hide_index=True, 
        column_config={"#": st.column_config.TextColumn("#", width="small")}
    )

def render(df, mes_ini, mes_fim, show_labels, show_total, ultima_atualizacao=None):
    def format_pt_br_abrev(val):
        if pd.isna(val) or val == 0: return brl(0) 
        if val >= 1_000_000: return f"R$ {val/1_000_000:,.1f} Mi"
        if val >= 1_000: return f"R$ {val/1_000:,.0f} mil"
        return brl(val)

    # ==================== TÍTULO CENTRALIZADO ====================
    st.markdown("<h2 style='text-align: center; color: #003366;'>Cruzamentos & Interseções entre Emissoras</h2>", unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom: 20px;'></div>", unsafe_allow_html=True)

    # Inicialização para exportação
    df_excl_raw = pd.DataFrame()
    df_comp_raw = pd.DataFrame()
    df_ausentes_raw = pd.DataFrame()
    top_shared_raw = pd.DataFrame()
    mat_raw = pd.DataFrame()
    pivot_cost_display = pd.DataFrame() 
    fig_mat = go.Figure() 

    df = df.rename(columns={c: c.lower() for c in df.columns})

    if "cliente" not in df.columns or "emissora" not in df.columns or "faturamento" not in df.columns:
        st.error("Colunas obrigatórias 'Cliente', 'Emissora' e 'Faturamento' ausentes.")
        return
    
    if "insercoes" not in df.columns:
        df["insercoes"] = 0.0

    base_periodo = df[df["mes"].between(mes_ini, mes_fim)]

    if base_periodo.empty:
        st.info("Sem dados para o período selecionado.")
        return

    # Agrupamento Base
    agg = base_periodo.groupby(["cliente", "emissora"], as_index=False).agg(
        faturamento=("faturamento", "sum"),
        insercoes=("insercoes", "sum")
    )
    agg["presenca"] = np.where(agg["faturamento"] > 0, 1, 0)

    # Pivôs para cálculos
    pres_pivot = agg.pivot_table(index="cliente", columns="emissora", values="presenca", fill_value=0)
    # Garante que todas as emissoras do dataframe filtrado apareçam nas colunas
    emissoras = sorted(agg["emissora"].unique())
    
    # Contagem de Emissoras por Cliente
    emis_count = pres_pivot.sum(axis=1)
    
    # Lista de todos os clientes únicos no período (Mercado Total Filtrado)
    todos_clientes = set(agg["cliente"].unique())
  
    # ==================== CÁLCULOS GERAIS ====================
    exclusivos_mask = emis_count == 1
    compartilhados_mask = emis_count >= 2

    excl_info, comp_info, ausentes_info = [], [], []
    fat_total_geral = agg["faturamento"].sum() 

    for emis in emissoras:
        # --- 1. EXCLUSIVOS ---
        cli_excl = pres_pivot.loc[exclusivos_mask & (pres_pivot[emis] == 1)].index
        dados_excl = agg[(agg["cliente"].isin(cli_excl)) & (agg["emissora"] == emis)]
        
        fat_excl = dados_excl["faturamento"].sum()
        ins_excl = dados_excl["insercoes"].sum()
        
        # --- 2. COMPARTILHADOS ---
        cli_comp = pres_pivot.loc[compartilhados_mask & (pres_pivot[emis] == 1)].index
        dados_comp = agg[(agg["cliente"].isin(cli_comp)) & (agg["emissora"] == emis)]
        
        fat_comp = dados_comp["faturamento"].sum()
        ins_comp = dados_comp["insercoes"].sum()
        
        # --- 3. AUSENTES (NOVO) ---
        clientes_emis = set(agg[agg["emissora"] == emis]["cliente"].unique())
        lista_ausentes = list(todos_clientes - clientes_emis)
        dados_ausentes = agg[agg["cliente"].isin(lista_ausentes)]
        
        fat_ausente = dados_ausentes["faturamento"].sum()
        ins_ausente = dados_ausentes["insercoes"].sum()
        qtd_ausentes = len(lista_ausentes)

        dados_total = agg[agg["emissora"] == emis]
        fat_total_emis = dados_total["faturamento"].sum()
        
        # Appends
        excl_info.append({
            "Emissora": emis, 
            "Clientes Exclusivos": len(cli_excl),
            "Faturamento Exclusivo": fat_excl, 
            "Inserções Exclusivas": ins_excl,
            "% Faturamento": (fat_excl / fat_total_emis * 100) if fat_total_emis > 0 else 0
        })
        comp_info.append({
            "Emissora": emis, 
            "Clientes Compartilhados": len(cli_comp),
            "Faturamento Compartilhado": fat_comp, 
            "Inserções Compartilhadas": ins_comp,
            "% Faturamento": (fat_comp / fat_total_emis * 100) if fat_total_emis > 0 else 0
        })
        ausentes_info.append({
            "Emissora": emis,
            "Clientes Ausentes": qtd_ausentes,
            "Faturamento Perdido (Oportunidade)": fat_ausente,
            "Inserções Perdidas": ins_ausente,
            "% Share Perdido": (fat_ausente / fat_total_geral * 100) if fat_total_geral > 0 else 0
        })

    # ==================== 1. EXCLUSIVOS ====================
    st.subheader("1. Clientes Exclusivos por Emissora")
    df_excl_raw = pd.DataFrame(excl_info) 
    if not df_excl_raw.empty:
        df_excl_raw = df_excl_raw.sort_values("Faturamento Exclusivo", ascending=False).reset_index(drop=True)
        
        # Lógica Totalizador
        if show_total:
            total_row = {
                "Emissora": "Totalizador", 
                "Clientes Exclusivos": df_excl_raw["Clientes Exclusivos"].sum(), 
                "Faturamento Exclusivo": df_excl_raw["Faturamento Exclusivo"].sum(), 
                "Inserções Exclusivas": df_excl_raw["Inserções Exclusivas"].sum(),
                "% Faturamento": (df_excl_raw["Faturamento Exclusivo"].sum() / fat_total_geral * 100) if fat_total_geral > 0 else np.nan
            }
            df_excl_raw = pd.concat([df_excl_raw, pd.DataFrame([total_row])], ignore_index=True)
        
        if show_total:
             df_excl_raw.insert(0, "#", list(range(1, len(df_excl_raw))) + ["Total"])
        else:
             df_excl_raw.insert(0, "#", list(range(1, len(df_excl_raw) + 1)))
        
        df_excl_display = df_excl_raw.copy()
        df_excl_display['#'] = df_excl_display['#'].astype(str)
        df_excl_display["Faturamento Exclusivo"] = df_excl_display["Faturamento Exclusivo"].apply(brl)
        df_excl_display["Inserções Exclusivas"] = df_excl_display["Inserções Exclusivas"].apply(format_int)
        df_excl_display["% Faturamento"] = df_excl_display["% Faturamento"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "—")
        
        display_styled_table(df_excl_display)
    else: st.info("Nenhum cliente exclusivo encontrado.")
    st.divider()

    # ==================== 2. COMPARTILHADOS ====================
    st.subheader("2. Clientes Compartilhados por Emissora")
    df_comp_raw = pd.DataFrame(comp_info) 
    if not df_comp_raw.empty:
        df_comp_raw = df_comp_raw.sort_values("Faturamento Compartilhado", ascending=False).reset_index(drop=True)
        
        # Lógica Totalizador
        if show_total:
            total_row = {
                "Emissora": "Totalizador", 
                "Clientes Compartilhados": df_comp_raw["Clientes Compartilhados"].sum(), 
                "Faturamento Compartilhado": df_comp_raw["Faturamento Compartilhado"].sum(), 
                "Inserções Compartilhadas": df_comp_raw["Inserções Compartilhadas"].sum(),
                "% Faturamento": (df_comp_raw["Faturamento Compartilhado"].sum() / fat_total_geral * 100) if fat_total_geral > 0 else np.nan
            }
            df_comp_raw = pd.concat([df_comp_raw, pd.DataFrame([total_row])], ignore_index=True)
        
        if show_total:
             df_comp_raw.insert(0, "#", list(range(1, len(df_comp_raw))) + ["Total"])
        else:
             df_comp_raw.insert(0, "#", list(range(1, len(df_comp_raw) + 1)))
        
        df_comp_display = df_comp_raw.copy()
        df_comp_display['#'] = df_comp_display['#'].astype(str)
        df_comp_display["Faturamento Compartilhado"] = df_comp_display["Faturamento Compartilhado"].apply(brl)
        df_comp_display["Inserções Compartilhadas"] = df_comp_display["Inserções Compartilhadas"].apply(format_int)
        df_comp_display["% Faturamento"] = df_comp_display["% Faturamento"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "—")
        
        display_styled_table(df_comp_display)
    else: st.info("Nenhum cliente compartilhado encontrado.")
    st.divider()

    # ==================== 3. AUSENTES (NOVO) ====================
    st.subheader("3. Clientes Ausentes por Emissora (Oportunidade)")
    df_ausentes_raw = pd.DataFrame(ausentes_info)
    
    if not df_ausentes_raw.empty:
        df_ausentes_raw = df_ausentes_raw.sort_values("Faturamento Perdido (Oportunidade)", ascending=False).reset_index(drop=True)
        
        # Lógica Totalizador
        if show_total:
            total_row = {
                "Emissora": "Totalizador",
                "Clientes Ausentes": df_ausentes_raw["Clientes Ausentes"].sum(),
                "Faturamento Perdido (Oportunidade)": df_ausentes_raw["Faturamento Perdido (Oportunidade)"].sum(),
                "Inserções Perdidas": df_ausentes_raw["Inserções Perdidas"].sum(),
                "% Share Perdido": np.nan 
            }
            df_ausentes_raw = pd.concat([df_ausentes_raw, pd.DataFrame([total_row])], ignore_index=True)
        
        if show_total:
             df_ausentes_raw.insert(0, "#", list(range(1, len(df_ausentes_raw))) + ["Total"])
        else:
             df_ausentes_raw.insert(0, "#", list(range(1, len(df_ausentes_raw) + 1)))
        
        df_ausentes_display = df_ausentes_raw.copy()
        df_ausentes_display['#'] = df_ausentes_display['#'].astype(str)
        df_ausentes_display["Faturamento Perdido (Oportunidade)"] = df_ausentes_display["Faturamento Perdido (Oportunidade)"].apply(brl)
        df_ausentes_display["Inserções Perdidas"] = df_ausentes_display["Inserções Perdidas"].apply(format_int)
        df_ausentes_display["% Share Perdido"] = df_ausentes_display["% Share Perdido"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "—")
        
        display_styled_table(df_ausentes_display)
    else:
        st.success("Incrível! Todas as emissoras atendem a todos os clientes do filtro (Nenhum ausente).")
    
    st.divider()

    # ==================== 4. TOP CLIENTES COMPARTILHADOS ====================
    st.subheader("4. Top clientes compartilhados (2+ emissoras)")
    if compartilhados_mask.any():
        share_clients_idx = pres_pivot[compartilhados_mask].index
        
        custom_order = ["Difusora", "Novabrasil", "Th+ Prime", "Thathi Tv"]
        order_map = {name.lower(): i for i, name in enumerate(custom_order)}

        def get_emissoras_str(row):
            emis_ativas = row.index[row == 1].tolist()
            emis_ativas.sort(key=lambda x: (order_map.get(x.lower(), 999), x))
            return ", ".join(emis_ativas)

        df_emis_list = pres_pivot.loc[share_clients_idx].apply(get_emissoras_str, axis=1)
        
        top_shared_raw = (base_periodo[base_periodo["cliente"].isin(share_clients_idx)]
                          .groupby("cliente", as_index=False)
                          .agg(faturamento=("faturamento", "sum"), insercoes=("insercoes", "sum"))
                          .sort_values("faturamento", ascending=False)
                          .head(20))
        
        top_shared_raw["emissoras_compartilhadas"] = top_shared_raw["cliente"].map(df_emis_list)

        if not top_shared_raw.empty and show_total:
            top_shared_raw = pd.concat([
                top_shared_raw, 
                pd.DataFrame([{
                    "cliente": "Totalizador", 
                    "faturamento": top_shared_raw["faturamento"].sum(),
                    "insercoes": top_shared_raw["insercoes"].sum(),
                    "emissoras_compartilhadas": "" 
                }])
            ], ignore_index=True)
        
        if show_total:
             top_shared_raw.insert(0, "#", list(range(1, len(top_shared_raw))) + ["Total"])
        else:
             top_shared_raw.insert(0, "#", list(range(1, len(top_shared_raw) + 1)))
        
        top_shared_disp = top_shared_raw.copy().rename(columns={
            "cliente": "Cliente", 
            "faturamento": "Faturamento",
            "insercoes": "Inserções",
            "emissoras_compartilhadas": "Emissoras Compartilhadas"
        })
        
        top_shared_disp['#'] = top_shared_disp['#'].astype(str)
        top_shared_disp["Faturamento"] = top_shared_disp["Faturamento"].apply(brl)
        top_shared_disp["Inserções"] = top_shared_disp["Inserções"].apply(format_int)
        
        display_styled_table(top_shared_disp)
    else: st.info("Não há clientes compartilhados com os filtros atuais.")
    st.divider()

    # ==================== 5. MATRIZ DE INTERSEÇÃO ====================
    if "cruzamentos_metric" not in st.session_state: st.session_state.cruzamentos_metric = "Clientes"
    metric = st.session_state.cruzamentos_metric
    
    btn_label_clientes = "Clientes em comum"
    btn_label_fat = "Faturamento em comum (R$)"
    btn_label_ins = "Inserções em comum (Qtd)"
    
    metric_label = metric 
    if metric == "Clientes": metric_label = btn_label_clientes
    elif metric == "Faturamento": metric_label = btn_label_fat
    else: metric_label = btn_label_ins
    
    st.subheader(f"5. Interseções entre emissoras (matriz) - {metric_label}")
    emis_list = sorted(list(pres_pivot.columns))
    
    if len(emis_list) < 2:
        st.info("Requer pelo menos 2 emissoras para cruzamento.")
    else:
        col1, col2, col3 = st.columns([1, 1, 1]) 
        
        btn_type_clientes = "primary" if metric == "Clientes" else "secondary"
        btn_type_fat = "primary" if metric == "Faturamento" else "secondary"
        btn_type_ins = "primary" if metric == "Insercoes" else "secondary"
        
        with col1:
            if st.button(btn_label_clientes, type=btn_type_clientes, use_container_width=True):
                st.session_state.cruzamentos_metric = "Clientes"
                st.rerun() 
        with col2:
            if st.button(btn_label_fat, type=btn_type_fat, use_container_width=True):
                st.session_state.cruzamentos_metric = "Faturamento"
                st.rerun() 
        with col3:
            if st.button(btn_label_ins, type=btn_type_ins, use_container_width=True):
                st.session_state.cruzamentos_metric = "Insercoes"
                st.rerun() 

        mat_raw = pd.DataFrame(0.0, index=emis_list, columns=emis_list)
        z_text = None 
        text_colors_2d = [] 

        if metric == "Clientes":
            for a, b in combinations(emis_list, 2):
                comuns = ((pres_pivot[a] == 1) & (pres_pivot[b] == 1)).sum()
                mat_raw.loc[a, b] = comuns
                mat_raw.loc[b, a] = comuns
            for e in emis_list: mat_raw.loc[e, e] = (pres_pivot[e] == 1).sum()
            z = mat_raw.values
            hover = "<b>%{y} x %{x}</b><br>Clientes: %{z}<extra></extra>"
            z_text = z.astype(int).astype(str) 
            max_val = np.nanmax(z) if z.size > 0 else 0
            text_colors_2d = [['white' if v > max_val * 0.4 else 'black' for v in row] for row in z]
            
        elif metric == "Faturamento": 
            val_pivot = agg.pivot_table(index="cliente", columns="emissora", values="faturamento", fill_value=0.0) 
            for a, b in combinations(emis_list, 2):
                menor = np.minimum(val_pivot[a], val_pivot[b])
                vlr = menor[menor > 0].sum()
                mat_raw.loc[a, b] = vlr
                mat_raw.loc[b, a] = vlr
            for e in emis_list: mat_raw.loc[e, e] = val_pivot[e].sum()
            z = mat_raw.values
            hover = "<b>%{y} x %{x}</b><br>Valor: R$ %{z:,.2f}<extra></extra>"
            z_text = [[format_pt_br_abrev(v) for v in row] for row in z]
            max_val = np.nanmax(z) if z.size > 0 else 0
            text_colors_2d = [['white' if v > max_val * 0.4 else 'black' for v in row] for row in z]
            
        else: 
            ins_pivot = agg.pivot_table(index="cliente", columns="emissora", values="insercoes", fill_value=0.0)
            for a, b in combinations(emis_list, 2):
                menor = np.minimum(ins_pivot[a], ins_pivot[b])
                vlr = menor[menor > 0].sum()
                mat_raw.loc[a, b] = vlr
                mat_raw.loc[b, a] = vlr
            for e in emis_list: mat_raw.loc[e, e] = ins_pivot[e].sum()
            z = mat_raw.values
            hover = "<b>%{y} x %{x}</b><br>Inserções: %{z:,.0f}<extra></extra>"
            z_text = [[format_int(v) for v in row] for row in z]
            max_val = np.nanmax(z) if z.size > 0 else 0
            text_colors_2d = [['white' if v > max_val * 0.4 else 'black' for v in row] for row in z]

        fig_mat = go.Figure(data=go.Heatmap(z=z, x=mat_raw.columns, y=mat_raw.index, colorscale="Blues", hovertemplate=hover, showscale=True))
        if show_labels and z_text is not None:
            for i, row in enumerate(z):
                for j, val in enumerate(row):
                    fig_mat.add_annotation(x=mat_raw.columns[j], y=mat_raw.index[i], text=z_text[i][j], showarrow=False, font=dict(color=text_colors_2d[i][j]))

        fig_mat.update_layout(height=420, template="plotly_white", margin=dict(l=0, r=10, t=10, b=0))
        
        # --- TRAVA DE INTERAÇÃO (HEATMAP) ---
        fig_mat.update_xaxes(fixedrange=True)
        fig_mat.update_yaxes(fixedrange=True)
        
        st.plotly_chart(fig_mat, width="stretch", config={'displayModeBar': False})
        
    st.divider()

    # ==================== 6. COMPARATIVO CUSTO UNITÁRIO ====================
    st.subheader("6. Comparativo de Custo Médio Unitário (Clientes Compartilhados)")
    
    if compartilhados_mask.any():
        share_clients_idx = pres_pivot[compartilhados_mask].index
        
        df_cost = agg[agg["cliente"].isin(share_clients_idx)].copy()
        
        # Custo Unitário
        df_cost["custo_unit"] = np.where(
            df_cost["insercoes"] > 0,
            df_cost["faturamento"] / df_cost["insercoes"],
            df_cost["faturamento"] 
        )
        
        pivot_cost = df_cost.pivot_table(
            index="cliente", 
            columns="emissora", 
            values="custo_unit"
        )
        
        pivot_cost = pivot_cost.reindex(columns=emissoras)
        
        # Ordenação
        client_ranking = base_periodo.groupby("cliente")["faturamento"].sum()
        pivot_cost["_sort_val"] = pivot_cost.index.map(client_ranking)
        pivot_cost = pivot_cost.sort_values("_sort_val", ascending=False).drop(columns="_sort_val")
        
        # --- CÁLCULO DA LINHA TOTALIZADORA (MÉDIA) ---
        df_final = pivot_cost.reset_index()
        
        if show_total:
            mean_values = pivot_cost.mean(numeric_only=True)
            total_row_data = {"cliente": "Totalizador"}
            for col in pivot_cost.columns:
                total_row_data[col] = mean_values[col]
            
            total_df = pd.DataFrame([total_row_data])
            df_final = pd.concat([df_final, total_df], ignore_index=True)
        
        # --- FORMATAÇÃO ---
        pivot_cost_display = df_final.copy()
        pivot_cost_display = pivot_cost_display.rename(columns={"cliente": "Cliente"})
        
        cols_to_fmt = [c for c in pivot_cost_display.columns if c != "Cliente"]
        for col in cols_to_fmt:
            pivot_cost_display[col] = pivot_cost_display[col].apply(lambda x: brl(x) if pd.notna(x) else "-")
            
        display_styled_table(pivot_cost_display)
        
    else:
        st.info("Não há dados suficientes para comparação de custos (sem clientes compartilhados).")

    st.divider()

    # ==================== EXPORTAÇÃO (CENTRALIZADA) ====================
    # Lógica de Centralização do Botão (Padronizada)
    c_left, c_btn, c_right = st.columns([3, 2, 3])
    with c_btn:
        if st.button("Exportar Dados da Página", type="secondary", use_container_width=True):
            st.session_state.show_cruzamentos_export = True
    
    if ultima_atualizacao:
        st.markdown(f"<div style='text-align: center; color: grey; font-size: 0.8rem; margin-top: 5px;'>Última atualização da base de dados: {ultima_atualizacao}</div>", unsafe_allow_html=True)

    def get_filter_string():
        f = st.session_state 
        ano_ini = f.get("filtro_ano_ini", "N/A")
        ano_fim = f.get("filtro_ano_fim", "N/A")
        emis = ", ".join(f.get("filtro_emis", ["Todas"]))
        execs = ", ".join(f.get("filtro_execs", ["Todos"]))
        meses = ", ".join(f.get("filtro_meses_lista", ["Todos"]))
        clientes = ", ".join(f.get("filtro_clientes", ["Todos"])) if f.get("filtro_clientes") else "Todos"
        return (f"Período (Ano): {ano_ini} a {ano_fim} | Meses: {meses} | Emissoras: {emis} | Executivos: {execs} | Clientes: {clientes}")

    if st.session_state.get("show_cruzamentos_export", False):
        @st.dialog("Opções de Exportação - Cruzamentos")
        def export_dialog():
            # Títulos padronizados para Exportação
            table_options = {
                "1. Clientes Exclusivos por Emissora (Dados)": {'df': df_excl_raw},
                "2. Clientes Compartilhados por Emissora (Dados)": {'df': df_comp_raw},
                "3. Clientes Ausentes por Emissora (Oportunidade) (Dados)": {'df': df_ausentes_raw}, 
                "4. Top clientes compartilhados (2+ emissoras) (Dados)": {'df': top_shared_raw},
                f"5. Interseções entre emissoras - {metric_label} (Dados)": {'df': mat_raw.reset_index().rename(columns={'index':'Emissora'})},
                f"5. Interseções entre emissoras - {metric_label} (Gráfico)": {'fig': fig_mat},
                "6. Comparativo de Custo Médio Unitário (Clientes Compartilhados) (Dados)": {'df': pivot_cost_display.reset_index()} 
            }
            
            available_options = [name for name, data in table_options.items() if (data.get('df') is not None and not data['df'].empty) or (data.get('fig') is not None and data['fig'].data)]
            
            if not available_options:
                st.warning("Nenhuma tabela com dados foi gerada.")
                if st.button("Fechar", type="secondary"):
                    st.session_state.show_cruzamentos_export = False
                    st.rerun()
                return

            selected_names = st.multiselect("Selecione os itens para exportar:", options=available_options, default=available_options)
            tables_to_export = {name: table_options[name] for name in selected_names}
            
            if not tables_to_export:
                st.error("Selecione pelo menos um item.")
                return

            try:
                filtro_str = get_filter_string()
                
                # Nomes ATUALIZADOS para ZIP e Excel Interno
                nome_interno_excel = "Dashboard_Cruzamentos_Intersecoes.xlsx"
                zip_filename = "Dashboard_Cruzamentos_Intersecoes.zip"
                
                zip_data = create_zip_package(tables_to_export, filtro_str, excel_filename=nome_interno_excel)
                
                st.download_button(
                    label="Clique para baixar", 
                    data=zip_data, 
                    file_name=zip_filename, 
                    mime="application/zip", 
                    on_click=lambda: st.session_state.update(show_cruzamentos_export=False), 
                    type="secondary"
                )
            except Exception as e:
                st.error(f"Erro ao gerar ZIP: {e}")

            if st.button("Cancelar", key="cancel_export", type="secondary"):
                st.session_state.show_cruzamentos_export = False
                st.rerun()
        export_dialog()