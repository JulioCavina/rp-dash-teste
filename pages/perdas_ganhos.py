# pages/perdas_ganhos.py

import streamlit as st
from utils.format import brl
import pandas as pd
import numpy as np
from utils.export import create_zip_package 

# ==================== ESTILO CSS (CENTRALIZAÇÃO E ALINHAMENTO) ====================
# ATUALIZADO: CSS reforçado para corrigir alinhamento de títulos no mobile
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

/* Rótulo (Título do Card) - Força centralização do texto */
[data-testid="stMetricLabel"] {
    justify-content: center;
    width: 100%;
    margin-bottom: 0px !important; 
    text-align: center !important;
}

/* Garante que o texto dentro do label (p ou div) também centralize */
[data-testid="stMetricLabel"] > div, [data-testid="stMetricLabel"] p {
    text-align: center !important;
    width: 100%;
    justify-content: center;
    display: flex;
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

def color_delta(val):
    """Colore valores positivos de verde e negativos de vermelho."""
    if pd.isna(val) or val == "" or val == "-": return ""
    try:
        if isinstance(val, (int, float)):
            v = float(val)
        else:
            v = float(str(val).replace("%", "").replace("+", "").replace(",", "."))
            
        if v > 0: return "color: #16a34a; font-weight: 600;" 
        if v < 0: return "color: #dc2626; font-weight: 600;" 
    except (ValueError, TypeError): return ""
    return ""

def format_currency(val):
    """Formata moeda de forma abreviada ou completa dependendo do tamanho."""
    if pd.isna(val): return "R$ 0,00"
    val_abs = abs(val)
    sign = "-" if val < 0 else ""
    if val_abs >= 1_000_000:
        return f"{sign}R$ {val_abs/1_000_000:,.1f} Mi".replace(",", "X").replace(".", ",").replace("X", ".")
    return brl(val)

def format_int(val):
    """Formata inteiros com separador de milhar."""
    if pd.isna(val) or val == 0: return "-"
    return f"{int(val):,}".replace(",", ".")

def format_percent_col(val):
    """Converte valor numérico para string formatada ou hífen se nulo."""
    if pd.isna(val): return "-"
    return f"{val:+.2f}%"

# ==================== FUNÇÃO AUXILIAR DE ESTILO ====================
def display_styled_table(df, format_dict=None, color_cols=None):
    """
    Renderiza o dataframe aplicando estilo de destaque (Totalizador) na última linha.
    """
    if df.empty:
        return

    # CORREÇÃO: Removemos a lógica de fallback (row.name == len-1).
    # Agora só destaca se o nome for explicitamente "Totalizador".
    def highlight_total_row(row):
        is_total = False
        if "Cliente" in df.columns and row["Cliente"] == "Totalizador": is_total = True
        elif "Emissora" in df.columns and row["Emissora"] == "Totalizador": is_total = True
        
        if is_total:
            return ['background-color: #e6f3ff; font-weight: bold; color: #003366'] * len(row)
        return [''] * len(row)

    styler = df.style.apply(highlight_total_row, axis=1)

    # Aplica cores condicionais (verde/vermelho) se houver colunas de variação
    if color_cols:
        existing_cols = [c for c in color_cols if c in df.columns]
        if existing_cols:
            styler = styler.map(color_delta, subset=existing_cols)
    
    # Aplica formatação de dicionário se fornecido
    if format_dict:
        styler = styler.format(format_dict)

    st.dataframe(
        styler, 
        width="stretch", 
        hide_index=True, 
        column_config={"#": st.column_config.TextColumn("#", width="small")}
    )

# ==================== RENDERIZAÇÃO DA PÁGINA ====================
def render(df, mes_ini, mes_fim, show_labels, show_total, ultima_atualizacao=None):
    # Aplica CSS para centralizar os cards
    st.markdown(ST_METRIC_CENTER, unsafe_allow_html=True)

    # Inicialização de DFs para exportação
    df_perdas_raw = pd.DataFrame()
    df_ganhos_raw = pd.DataFrame()
    var_cli_raw = pd.DataFrame()
    var_emis_raw = pd.DataFrame()
    
    # Normalização básica
    df = df.rename(columns={c: c.lower() for c in df.columns})
    
    # Garante coluna insercoes
    if "insercoes" not in df.columns:
        df["insercoes"] = 0.0
    
    # ==================== LÓGICA DE ANOS (AUTOMÁTICA) ====================
    anos = sorted(df["ano"].dropna().unique())
    
    if not anos:
        st.info("Sem anos válidos na base.")
        return
    
    if len(anos) >= 2:
        ano_base, ano_comp = anos[-2], anos[-1]
    else:
        ano_base = ano_comp = anos[-1]

    # ==================== TÍTULO CENTRALIZADO ====================
    st.markdown(
        f"<h2 style='text-align: center; color: #003366;'>Perdas & Ganhos ({ano_base} vs {ano_comp})</h2>", 
        unsafe_allow_html=True
    )
    st.markdown("<div style='margin-bottom: 20px;'></div>", unsafe_allow_html=True)

    if "cliente" not in df.columns or "faturamento" not in df.columns:
        st.error("Colunas obrigatórias 'Cliente' e/ou 'Faturamento' ausentes.")
        return

    # Filtra período (Meses) e separa as bases
    base_periodo = df[df["mes"].between(mes_ini, mes_fim)]
    baseA = base_periodo[base_periodo["ano"] == ano_base]
    baseB = base_periodo[base_periodo["ano"] == ano_comp]

    # ==================== CÁLCULOS DE CHURN E NOVOS NEGÓCIOS ====================
    cliA = set(baseA["cliente"].unique())
    cliB = set(baseB["cliente"].unique())
    
    # Listas de Clientes
    lista_perdas = sorted(list(cliA - cliB))
    lista_ganhos = sorted(list(cliB - cliA))

    # Valores Perdidos (Saíram em A)
    dados_perdas = baseA[baseA["cliente"].isin(lista_perdas)]
    val_perdas = dados_perdas["faturamento"].sum()
    ins_perdas = dados_perdas["insercoes"].sum()
    
    # Valores Ganhos (Entraram em B)
    dados_ganhos = baseB[baseB["cliente"].isin(lista_ganhos)]
    val_ganhos = dados_ganhos["faturamento"].sum()
    ins_ganhos = dados_ganhos["insercoes"].sum()

    # Cálculo do Custo Unitário Médio (Yield)
    custo_medio_perdas = (val_perdas / ins_perdas) if ins_perdas > 0 else 0.0
    custo_medio_ganhos = (val_ganhos / ins_ganhos) if ins_ganhos > 0 else 0.0
    
    # Deltas (Saldos)
    saldo_financeiro = val_ganhos - val_perdas
    saldo_clientes = len(lista_ganhos) - len(lista_perdas)
    saldo_insercoes = ins_ganhos - ins_perdas
    saldo_custo = custo_medio_ganhos - custo_medio_perdas

    # ==================== CARDS DE SALDO LÍQUIDO (CENTRALIZADOS) ====================
    col_s1, col_s2, col_s3, col_s4 = st.columns(4)
    
    col_s1.metric(
        "Saldo Líquido (R$)", 
        format_currency(saldo_financeiro), 
        delta=f"Novos: {format_currency(val_ganhos)} | Perdidos: {format_currency(val_perdas)}",
        delta_color="normal" 
    )
    col_s2.metric(
        "Saldo de Clientes (Qtd)", 
        f"{saldo_clientes:+}", 
        delta=f"Novos: {len(lista_ganhos)} | Perdidos: {len(lista_perdas)}",
        delta_color="normal"
    )
    col_s3.metric(
        "Saldo Inserções (Qtd)",
        f"{int(saldo_insercoes):+}",
        delta=f"Novas: {int(ins_ganhos)} | Perdidas: {int(ins_perdas)}",
        delta_color="normal"
    )
    col_s4.metric(
        "Custo Médio Unitário (Saldo)",
        f"{saldo_custo:+.2f}".replace(".", ","), 
        delta=f"Novos: {brl(custo_medio_ganhos)} | Perdidos: {brl(custo_medio_perdas)}",
        delta_color="normal"
    )
    
    st.divider()

    # ==================== TABELAS 1 e 2 (LINHAS SEPARADAS) ====================
    
    # --- Tabela Perdas ---
    st.subheader(f"1. Clientes Perdidos (Saíram de {ano_base})")
    if lista_perdas:
        df_perdas_raw = (baseA[baseA["cliente"].isin(lista_perdas)]
                            .groupby("cliente", as_index=False)
                            .agg(faturamento=("faturamento", "sum"), insercoes=("insercoes", "sum"))
                            .sort_values("faturamento", ascending=False)
                            .reset_index(drop=True))
        
        if not df_perdas_raw.empty:
            # Lógica do Totalizador
            if show_total:
                total_row = pd.DataFrame([{
                    "cliente": "Totalizador", 
                    "faturamento": df_perdas_raw["faturamento"].sum(),
                    "insercoes": df_perdas_raw["insercoes"].sum()
                }])
                df_perdas_raw = pd.concat([df_perdas_raw, total_row], ignore_index=True)

        # Adiciona coluna #
        if show_total and not df_perdas_raw.empty:
             numeracao = list(range(1, len(df_perdas_raw))) + ["Total"]
             df_perdas_raw.insert(0, "#", numeracao)
        elif not df_perdas_raw.empty:
             df_perdas_raw.insert(0, "#", list(range(1, len(df_perdas_raw) + 1)))
        
        t_display = df_perdas_raw.copy()
        t_display = t_display.rename(columns={
            "cliente": "Cliente", 
            "faturamento": "Faturamento", 
            "insercoes": "Inserções"
        })
        t_display['#'] = t_display['#'].astype(str)
        t_display["Faturamento"] = t_display["Faturamento"].apply(brl)
        t_display["Inserções"] = t_display["Inserções"].apply(format_int)
        
        display_styled_table(t_display)

    else: 
        st.success("Nenhum cliente perdido neste período!")

    st.markdown("<br>", unsafe_allow_html=True) # Espaçamento

    # --- Tabela Ganhos ---
    st.subheader(f"2. Clientes Novos (Entraram em {ano_comp})")
    if lista_ganhos:
        df_ganhos_raw = (baseB[baseB["cliente"].isin(lista_ganhos)]
                            .groupby("cliente", as_index=False)
                            .agg(faturamento=("faturamento", "sum"), insercoes=("insercoes", "sum"))
                            .sort_values("faturamento", ascending=False)
                            .reset_index(drop=True))
        
        if not df_ganhos_raw.empty:
            # Lógica do Totalizador
            if show_total:
                total_row = pd.DataFrame([{
                    "cliente": "Totalizador", 
                    "faturamento": df_ganhos_raw["faturamento"].sum(),
                    "insercoes": df_ganhos_raw["insercoes"].sum()
                }])
                df_ganhos_raw = pd.concat([df_ganhos_raw, total_row], ignore_index=True)

        if show_total and not df_ganhos_raw.empty:
             numeracao = list(range(1, len(df_ganhos_raw))) + ["Total"]
             df_ganhos_raw.insert(0, "#", numeracao)
        elif not df_ganhos_raw.empty:
             df_ganhos_raw.insert(0, "#", list(range(1, len(df_ganhos_raw) + 1)))
        
        t_display = df_ganhos_raw.copy()
        t_display = t_display.rename(columns={
            "cliente": "Cliente", 
            "faturamento": "Faturamento",
            "insercoes": "Inserções"
        })
        t_display['#'] = t_display['#'].astype(str)
        t_display["Faturamento"] = t_display["Faturamento"].apply(brl)
        t_display["Inserções"] = t_display["Inserções"].apply(format_int)
        
        display_styled_table(t_display)

    else: 
        st.info("Nenhum cliente novo neste período.")

    st.divider()

    # CORREÇÃO CRÍTICA: Função refeita para suportar comparação de mesmo ano (2025 vs 2025)
    def build_variation_table(groupby_col, label_col):
        # Pivot sem preencher nomes de colunas automaticamente ainda
        piv_fat = base_periodo.groupby([groupby_col, "ano"])["faturamento"].sum().unstack(fill_value=0)
        piv_ins = base_periodo.groupby([groupby_col, "ano"])["insercoes"].sum().unstack(fill_value=0)
        
        # Garante alinhamento de índices (caso algum cliente tenha só em um ano e o unstack ignore)
        combined_index = piv_fat.index.union(piv_ins.index)
        
        # Constrói o DataFrame manualmente selecionando as séries. 
        # O uso de .get() evita KeyError se o ano não existir, 
        # e permite chamar o mesmo ano duas vezes (ex: 2025 e 2025) sem erro de duplicação de colunas no concat.
        df_var = pd.DataFrame(index=combined_index)
        
        df_var[f"Fat_{ano_base}"] = piv_fat.get(ano_base, 0.0)
        df_var[f"Fat_{ano_comp}"] = piv_fat.get(ano_comp, 0.0)
        df_var[f"Ins_{ano_base}"] = piv_ins.get(ano_base, 0.0)
        df_var[f"Ins_{ano_comp}"] = piv_ins.get(ano_comp, 0.0)
        
        # Cálculos de Delta
        df_var["# Fat"] = df_var[f"Fat_{ano_comp}"] - df_var[f"Fat_{ano_base}"]
        df_var["Δ%"] = np.where(df_var[f"Fat_{ano_base}"] > 0, (df_var["# Fat"] / df_var[f"Fat_{ano_base}"]) * 100, np.nan)
        df_var["Δ Ins"] = df_var[f"Ins_{ano_comp}"] - df_var[f"Ins_{ano_base}"]
        
        df_var = df_var.reset_index().rename(columns={groupby_col: label_col})
        df_var = df_var.sort_values("# Fat", ascending=True)
        
        return df_var

    # ==================== VARIAÇÕES (COMPARATIVO DE CARTEIRA) ====================
    st.subheader("3. Variações por Cliente (Faturamento e Inserções)")
    
    var_cli_raw = build_variation_table("cliente", "Cliente")

    if not var_cli_raw.empty and show_total:
        total_fat_a = var_cli_raw[f"Fat_{ano_base}"].sum()
        total_fat_b = var_cli_raw[f"Fat_{ano_comp}"].sum()
        total_ins_a = var_cli_raw[f"Ins_{ano_base}"].sum()
        total_ins_b = var_cli_raw[f"Ins_{ano_comp}"].sum()
        
        row_total = pd.DataFrame([{
            "Cliente": "Totalizador", 
            f"Fat_{ano_base}": total_fat_a, 
            f"Fat_{ano_comp}": total_fat_b, 
            "# Fat": total_fat_b - total_fat_a, 
            "Δ%": (total_fat_b - total_fat_a) / total_fat_a * 100 if total_fat_a > 0 else np.nan,
            f"Ins_{ano_base}": total_ins_a,
            f"Ins_{ano_comp}": total_ins_b,
            "Δ Ins": total_ins_b - total_ins_a
        }])
        var_cli_raw = pd.concat([var_cli_raw, row_total], ignore_index=True)
    
    var_cli_disp = var_cli_raw.copy()
    col_map = {
        f"Fat_{ano_base}": f"R$ {ano_base}",
        f"Fat_{ano_comp}": f"R$ {ano_comp}",
        f"Ins_{ano_base}": f"Ins. {ano_base}",
        f"Ins_{ano_comp}": f"Ins. {ano_comp}",
    }
    var_cli_disp = var_cli_disp.rename(columns=col_map)
    var_cli_disp["Δ%"] = var_cli_disp["Δ%"].apply(format_percent_col)

    # Chama função de estilo
    display_styled_table(
        var_cli_disp, 
        format_dict={
            f"R$ {ano_base}": brl, 
            f"R$ {ano_comp}": brl, 
            "# Fat": brl, 
            f"Ins. {ano_base}": format_int,
            f"Ins. {ano_comp}": format_int,
            "Δ Ins": format_int
        },
        color_cols=["# Fat", "Δ%", "Δ Ins"] 
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # ==================== VARIAÇÕES POR EMISSORA ====================
    st.subheader("4. Variações por Emissora (Faturamento e Inserções)")
    
    var_emis_raw = build_variation_table("emissora", "Emissora")
    
    if not var_emis_raw.empty and show_total:
        total_fat_a = var_emis_raw[f"Fat_{ano_base}"].sum()
        total_fat_b = var_emis_raw[f"Fat_{ano_comp}"].sum()
        total_ins_a = var_emis_raw[f"Ins_{ano_base}"].sum()
        total_ins_b = var_emis_raw[f"Ins_{ano_comp}"].sum()
        
        row_total_e = pd.DataFrame([{
            "Emissora": "Totalizador", 
            f"Fat_{ano_base}": total_fat_a, 
            f"Fat_{ano_comp}": total_fat_b, 
            "# Fat": total_fat_b - total_fat_a, 
            "Δ%": (total_fat_b - total_fat_a) / total_fat_a * 100 if total_fat_a > 0 else np.nan,
            f"Ins_{ano_base}": total_ins_a,
            f"Ins_{ano_comp}": total_ins_b,
            "Δ Ins": total_ins_b - total_ins_a
        }])
        var_emis_raw = pd.concat([var_emis_raw, row_total_e], ignore_index=True)
        
    var_emis_disp = var_emis_raw.copy().rename(columns=col_map)
    var_emis_disp["Δ%"] = var_emis_disp["Δ%"].apply(format_percent_col)

    # Chama função de estilo
    display_styled_table(
        var_emis_disp,
        format_dict={
            f"R$ {ano_base}": brl, 
            f"R$ {ano_comp}": brl, 
            "# Fat": brl, 
            f"Ins. {ano_base}": format_int,
            f"Ins. {ano_comp}": format_int,
            "Δ Ins": format_int
        },
        color_cols=["# Fat", "Δ%", "Δ Ins"]
    )
    
    st.divider()

    # ==================== EXPORTAÇÃO (CENTRALIZADA) ====================
    # Lógica de Centralização do Botão (Igual Clientes & Faturamento)
    c_left, c_btn, c_right = st.columns([3, 2, 3])
    with c_btn:
        if st.button("Exportar Dados da Página", type="secondary", use_container_width=True):
            st.session_state.show_perdas_export = True
    
    if ultima_atualizacao:
        st.markdown(f"<div style='text-align: center; color: grey; font-size: 0.8rem; margin-top: 5px;'>Última atualização da base de dados: {ultima_atualizacao}</div>", unsafe_allow_html=True)

    def get_filter_string():
        f = st.session_state 
        return (f"Comparativo: {ano_base} vs {ano_comp} | Meses: {', '.join(f.get('filtro_meses_lista', ['Todos']))}")

    if st.session_state.get("show_perdas_export", False):
        @st.dialog("Opções de Exportação - Perdas & Ganhos")
        def export_dialog():
            df_p_exp = df_perdas_raw.rename(columns={"cliente": "Cliente", "faturamento": "Faturamento", "insercoes": "Inserções"}) if not df_perdas_raw.empty else None
            df_g_exp = df_ganhos_raw.rename(columns={"cliente": "Cliente", "faturamento": "Faturamento", "insercoes": "Inserções"}) if not df_ganhos_raw.empty else None
            df_vc_exp = var_cli_raw if not var_cli_raw.empty else None
            df_ve_exp = var_emis_raw if not var_emis_raw.empty else None

            # Chaves com nomes reais exibidos na tela
            table_options = {
                f"1. Clientes Perdidos (Saíram de {ano_base}) (Dados)": {'df': df_p_exp}, 
                f"2. Clientes Novos (Entraram em {ano_comp}) (Dados)": {'df': df_g_exp}, 
                "3. Variações por Cliente (Faturamento e Inserções) (Dados)": {'df': df_vc_exp}, 
                "4. Variações por Emissora (Faturamento e Inserções) (Dados)": {'df': df_ve_exp}
            }
            available_options = [name for name, data in table_options.items() if data.get('df') is not None and not data['df'].empty]
            
            if not available_options:
                st.warning("Nenhuma tabela com dados foi gerada.")
                if st.button("Fechar", type="secondary"):
                    st.session_state.show_perdas_export = False
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
                # Nome fixo para o Excel interno
                nome_interno_excel = "Dashboard_Perdas_Ganhos.xlsx"
                zip_filename = "Dashboard_Perdas_Ganhos.zip"
                
                zip_data = create_zip_package(tables_to_export, filtro_str, excel_filename=nome_interno_excel)
                
                st.download_button(
                    label="Clique para baixar", 
                    data=zip_data, 
                    file_name=zip_filename, 
                    mime="application/zip", 
                    on_click=lambda: st.session_state.update(show_perdas_export=False), 
                    type="secondary"
                )
            except Exception as e:
                st.error(f"Erro ao gerar ZIP: {e}")

            if st.button("Cancelar", key="cancel_export", type="secondary"):
                st.session_state.show_perdas_export = False
                st.rerun()
        export_dialog()