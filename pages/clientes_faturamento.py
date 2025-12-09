# pages/clientes_faturamento.py

import streamlit as st
import numpy as np
import pandas as pd
from utils.format import brl, PALETTE
from utils.loaders import load_main_base
from utils.export import create_zip_package 

# ==================== FUNÇÕES DE FORMATAÇÃO ====================
def color_delta(val):
    """Colore valores positivos de verde e negativos de vermelho."""
    if pd.isna(val) or val == "" or val == "-": return ""
    try:
        if isinstance(val, (int, float)):
            v = float(val)
        else:
            clean_val = str(val).replace("%", "").replace("+", "").replace(",", ".")
            v = float(clean_val)
        if v > 0: return "color: #16a34a; font-weight: 600;" 
        if v < 0: return "color: #dc2626; font-weight: 600;" 
    except (ValueError, TypeError): 
        return ""
    return ""

def format_percent_col(val):
    if pd.isna(val): return "-"
    return f"{val:+.2f}%"

def format_int(val):
    if isinstance(val, str): return val
    if pd.isna(val) or val == 0: return "-"
    try:
        return f"{int(val):,}".replace(",", ".")
    except (ValueError, TypeError):
        return str(val)

# ==================== FUNÇÃO AUXILIAR DE EXIBIÇÃO (UNIFICADA) ====================
def display_combined_table(df_main, df_total, format_dict=None, color_cols=None, show_total=True, column_config=None):
    """
    Concatena df_main e df_total SE show_total for True.
    Aplica a estilização na última linha apenas se o total estiver presente.
    Aceita column_config para customização de cabeçalhos e tooltips.
    """
    
    # 1. Concatenação (União das tabelas)
    if show_total and not df_total.empty:
        # Garante que as colunas estejam alinhadas
        df_combined = pd.concat([df_main, df_total], ignore_index=True)
    else:
        df_combined = df_main.copy()

    # 2. Definição da função de estilo para a linha de Total
    def highlight_total_row(row):
        # Só destaca se o total estiver ativado e for a última linha
        if show_total and not df_total.empty and row.name == (len(df_combined) - 1):
            return ['background-color: #e6f3ff; font-weight: bold; color: #003366'] * len(row)
        return [''] * len(row)

    # 3. Aplicação dos estilos
    # Primeiro aplicamos o destaque da linha total (se houver)
    styler = df_combined.style.apply(highlight_total_row, axis=1)

    # Depois aplicamos as cores condicionais (verde/vermelho) nas colunas de variação
    if color_cols:
        styler = styler.map(color_delta, subset=[c for c in color_cols if c in df_combined.columns])

    # Se houver formatação numérica específica
    if format_dict:
        styler = styler.format(format_dict)

    # Mescla configurações padrão com as passadas
    final_config = {"#": st.column_config.TextColumn("#", width="small")}
    if column_config:
        final_config.update(column_config)

    # 4. Renderização
    st.dataframe(
        styler,
        hide_index=True, 
        width="stretch", 
        column_config=final_config
    )

    # Retorna o DF combinado para uso na exportação
    return df_combined

# ==================== HELPER PARA TOOLTIPS DE CMU ====================
def get_cmu_config(columns):
    """Gera configuração de colunas para substituir 'Custo Médio Unitário' por 'CMU ℹ️'."""
    config = {}
    for col in columns:
        if "Custo Médio Unitário" in col:
            # Ex: "Custo Médio Unitário (2024)" -> "CMU 2024 ℹ️"
            # Ex: "Custo Médio Unitário" -> "CMU ℹ️"
            label = col.replace("Custo Médio Unitário", "CMU").replace("(", "").replace(")", "").strip() + " ℹ️"
            config[col] = st.column_config.Column(
                label=label,
                help="Custo Médio Unitário"
            )
    return config

# ==================== RENDERIZAÇÃO DA PÁGINA ====================
def render(df, mes_ini, mes_fim, show_labels, show_total, ultima_atualizacao=None):
    st.markdown("<h2 style='text-align: center; color: #003366;'>Clientes & Faturamento</h2>", unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom: 20px;'></div>", unsafe_allow_html=True)

    # Normalização
    df = df.rename(columns={c: c.lower() for c in df.columns})
    if "faturamento" not in df.columns:
        st.error("Coluna 'Faturamento' ausente na base.")
        return
    if "insercoes" not in df.columns:
        df["insercoes"] = 0.0

    # Anos
    anos = sorted(df["ano"].dropna().unique())
    if not anos: st.info("Sem anos válidos."); return
    if len(anos) >= 2: ano_base, ano_comp = anos[-2], anos[-1]
    else: ano_base = ano_comp = anos[-1]

    base_periodo = df[df["mes"].between(mes_ini, mes_fim)]

    # Helper de métricas
    def enrich_with_metrics_split(df_main, group_col):
        piv_ins = base_periodo.groupby([group_col, "ano"])["insercoes"].sum().unstack(fill_value=0)
        piv_fat = base_periodo.groupby([group_col, "ano"])["faturamento"].sum().unstack(fill_value=0)
        
        for ano in [ano_base, ano_comp]:
            if ano not in piv_ins.columns: piv_ins[ano] = 0.0
            if ano not in piv_fat.columns: piv_fat[ano] = 0.0
            
        custo_base = np.where(piv_ins[ano_base] > 0, piv_fat[ano_base] / piv_ins[ano_base], np.nan)
        custo_comp = np.where(piv_ins[ano_comp] > 0, piv_fat[ano_comp] / piv_ins[ano_comp], np.nan)
        
        piv_fat = piv_fat.reset_index()
        piv_ins = piv_ins.reset_index()

        df_metrics = pd.DataFrame({
            group_col: piv_fat[group_col], 
            f"Ins_{ano_base}": piv_ins[ano_base].values,
            f"Ins_{ano_comp}": piv_ins[ano_comp].values,
            f"Custo_{ano_base}": custo_base,
            f"Custo_{ano_comp}": custo_comp
        })
        
        if group_col in df_main.columns:
            df_merged = pd.merge(df_main, df_metrics, on=group_col, how="left")
        else:
            df_merged = df_main # Fallback
            
        return df_merged

    # ==================== 1. CLIENTES POR EMISSORA ====================
    st.subheader("1. Número de Clientes por Emissora (Comparativo)")
    base_clientes_raw = base_periodo.groupby(["emissora", "ano"])["cliente"].nunique().unstack(fill_value=0).reset_index()
    for ano in [ano_base, ano_comp]:
        if ano not in base_clientes_raw.columns: base_clientes_raw[ano] = 0

    base_clientes_raw["Δ"] = base_clientes_raw[ano_comp] - base_clientes_raw[ano_base]
    base_clientes_raw["Δ%"] = np.where(base_clientes_raw[ano_base] > 0, (base_clientes_raw["Δ"] / base_clientes_raw[ano_base]) * 100, np.nan)
    
    # Separa Total
    df_1_main = base_clientes_raw.copy()
    df_1_total = pd.DataFrame()

    if not df_1_main.empty:
        total_A = df_1_main[ano_base].sum()
        total_B = df_1_main[ano_comp].sum()
        total_delta = total_B - total_A
        total_pct = (total_delta / total_A * 100) if total_A > 0 else np.nan
        
        # Cria linha total
        df_1_total = pd.DataFrame([{
            "emissora": "Totalizador", 
            ano_base: total_A, 
            ano_comp: total_B, 
            "Δ": total_delta, 
            "Δ%": total_pct
        }])
    
    # Prepara Visualização
    df_1_main.insert(0, "#", range(1, len(df_1_main) + 1))
    df_1_total.insert(0, "#", ["Total"])
    
    # Renomeia
    rename_1 = {
        "emissora": "Emissora",
        ano_base: f"Clientes {ano_base}",
        ano_comp: f"Clientes {ano_comp}"
    }
    df_1_main = df_1_main.rename(columns=rename_1)
    df_1_total = df_1_total.rename(columns=rename_1)
    
    # Converte colunas para string para o display
    df_1_main.columns = df_1_main.columns.map(str)
    df_1_total.columns = df_1_total.columns.map(str)
    
    # Formatação Específica
    df_1_main["Δ%"] = df_1_main["Δ%"].apply(format_percent_col)
    df_1_total["Δ%"] = df_1_total["Δ%"].apply(format_percent_col)
    df_1_main['#'] = df_1_main['#'].astype(str)

    # EXIBE COMBINADO (COM CONTROLE DO TOTAL)
    export_1 = display_combined_table(
        df_1_main, 
        df_1_total, 
        format_dict=None, 
        color_cols=["Δ", "Δ%"],
        show_total=show_total
    )
    st.divider()

    # ==================== 2. FATURAMENTO POR EMISSORA ====================
    st.subheader("2. Faturamento por Emissora (com Eficiência)")
    base_emissora_raw = base_periodo.groupby(["emissora", "ano"])["faturamento"].sum().unstack(fill_value=0).reset_index()
    for ano in [ano_base, ano_comp]:
        if ano not in base_emissora_raw.columns: base_emissora_raw[ano] = 0.0

    base_emissora_raw["Δ"] = base_emissora_raw[ano_comp] - base_emissora_raw[ano_base]
    base_emissora_raw["Δ%"] = np.where(base_emissora_raw[ano_base] > 0, (base_emissora_raw["Δ"] / base_emissora_raw[ano_base]) * 100, np.nan)
    
    base_emissora_raw = enrich_with_metrics_split(base_emissora_raw, "emissora")

    df_2_main = base_emissora_raw.copy()
    df_2_total = pd.DataFrame()

    if not df_2_main.empty:
        tA = df_2_main[ano_base].sum()
        tB = df_2_main[ano_comp].sum()
        tDelta = tB - tA
        tPct = (tDelta / tA * 100) if tA > 0 else np.nan
        
        tInsA = df_2_main[f"Ins_{ano_base}"].sum()
        tInsB = df_2_main[f"Ins_{ano_comp}"].sum()
        avgCA = tA / tInsA if tInsA > 0 else np.nan
        avgCB = tB / tInsB if tInsB > 0 else np.nan

        df_2_total = pd.DataFrame([{
            "emissora": "Totalizador",
            ano_base: tA, ano_comp: tB, "Δ": tDelta, "Δ%": tPct,
            f"Ins_{ano_base}": tInsA, f"Ins_{ano_comp}": tInsB,
            f"Custo_{ano_base}": avgCA, f"Custo_{ano_comp}": avgCB
        }])

    df_2_main.insert(0, "#", range(1, len(df_2_main) + 1))
    df_2_total.insert(0, "#", ["Total"])

    rename_2 = {
        "emissora": "Emissora",
        ano_base: f"Faturamento {ano_base}", 
        ano_comp: f"Faturamento {ano_comp}",
        f"Ins_{ano_base}": f"Ins. {ano_base}", f"Ins_{ano_comp}": f"Ins. {ano_comp}",
        f"Custo_{ano_base}": f"Custo Médio Unitário ({ano_base})", f"Custo_{ano_comp}": f"Custo Médio Unitário ({ano_comp})"
    }
    
    df_2_main = df_2_main.rename(columns=rename_2)
    df_2_total = df_2_total.rename(columns=rename_2)
    df_2_main.columns = df_2_main.columns.map(str)
    df_2_total.columns = df_2_total.columns.map(str)
    
    # Format
    for d in [df_2_main, df_2_total]:
        if not d.empty:
            d["Δ%"] = d["Δ%"].apply(format_percent_col)
            d[f"Ins. {ano_base}"] = d[f"Ins. {ano_base}"].apply(format_int)
            d[f"Ins. {ano_comp}"] = d[f"Ins. {ano_comp}"].apply(format_int)
            d[f"Custo Médio Unitário ({ano_base})"] = d[f"Custo Médio Unitário ({ano_base})"].apply(brl)
            d[f"Custo Médio Unitário ({ano_comp})"] = d[f"Custo Médio Unitário ({ano_comp})"].apply(brl)
            d['#'] = d['#'].astype(str)

    export_2 = display_combined_table(
        df_2_main, df_2_total,
        format_dict={f"Faturamento {ano_base}": brl, f"Faturamento {ano_comp}": brl, "Δ": brl},
        color_cols=["Δ", "Δ%"],
        show_total=show_total,
        column_config=get_cmu_config(df_2_main.columns)
    )
    st.divider()

    # ==================== 3. FATURAMENTO POR EXECUTIVO ====================
    st.subheader("3. Faturamento por Executivo (com Eficiência)")
    tx_raw = base_periodo.groupby(["executivo", "ano"])["faturamento"].sum().unstack(fill_value=0).reset_index()
    for ano in [ano_base, ano_comp]:
        if ano not in tx_raw.columns: tx_raw[ano] = 0.0
    
    tx_raw["Δ"] = tx_raw[ano_comp] - tx_raw[ano_base]
    tx_raw["Δ%"] = np.where(tx_raw[ano_base] > 0, (tx_raw["Δ"] / tx_raw[ano_base]) * 100, np.nan)
    tx_raw = enrich_with_metrics_split(tx_raw, "executivo")

    df_3_main = tx_raw.copy()
    df_3_total = pd.DataFrame()

    if not df_3_main.empty:
        tA = df_3_main[ano_base].sum()
        tB = df_3_main[ano_comp].sum()
        tDelta = tB - tA
        tPct = (tDelta / tA * 100) if tA > 0 else np.nan
        tInsA = df_3_main[f"Ins_{ano_base}"].sum()
        tInsB = df_3_main[f"Ins_{ano_comp}"].sum()
        avgCA = tA / tInsA if tInsA > 0 else np.nan
        avgCB = tB / tInsB if tInsB > 0 else np.nan

        df_3_total = pd.DataFrame([{
            "executivo": "Totalizador",
            ano_base: tA, ano_comp: tB, "Δ": tDelta, "Δ%": tPct,
            f"Ins_{ano_base}": tInsA, f"Ins_{ano_comp}": tInsB,
            f"Custo_{ano_base}": avgCA, f"Custo_{ano_comp}": avgCB
        }])

    df_3_main.insert(0, "#", range(1, len(df_3_main) + 1))
    df_3_total.insert(0, "#", ["Total"])

    rename_3 = {
        "executivo": "Executivo",
        ano_base: f"Faturamento {ano_base}",
        ano_comp: f"Faturamento {ano_comp}",
        f"Ins_{ano_base}": f"Ins. {ano_base}", f"Ins_{ano_comp}": f"Ins. {ano_comp}",
        f"Custo_{ano_base}": f"Custo Médio Unitário ({ano_base})", f"Custo_{ano_comp}": f"Custo Médio Unitário ({ano_comp})"
    }
    
    df_3_main = df_3_main.rename(columns=rename_3)
    df_3_total = df_3_total.rename(columns=rename_3)
    df_3_main.columns = df_3_main.columns.map(str)
    df_3_total.columns = df_3_total.columns.map(str)

    for d in [df_3_main, df_3_total]:
        if not d.empty:
            d["Δ%"] = d["Δ%"].apply(format_percent_col)
            d[f"Ins. {ano_base}"] = d[f"Ins. {ano_base}"].apply(format_int)
            d[f"Ins. {ano_comp}"] = d[f"Ins. {ano_comp}"].apply(format_int)
            d[f"Custo Médio Unitário ({ano_base})"] = d[f"Custo Médio Unitário ({ano_base})"].apply(brl)
            d[f"Custo Médio Unitário ({ano_comp})"] = d[f"Custo Médio Unitário ({ano_comp})"].apply(brl)
            d['#'] = d['#'].astype(str)

    export_3 = display_combined_table(
        df_3_main, df_3_total,
        format_dict={f"Faturamento {ano_base}": brl, f"Faturamento {ano_comp}": brl, "Δ": brl},
        color_cols=["Δ", "Δ%"],
        show_total=show_total,
        column_config=get_cmu_config(df_3_main.columns)
    )
    st.divider()

    # ==================== 4. MÉDIAS ====================
    st.subheader("4. Médias por Cliente (Investimento e Inserções)")
    t16_raw = base_periodo.groupby("emissora").agg(
        Faturamento=("faturamento", "sum"), Insercoes=("insercoes", "sum"), Clientes=("cliente", "nunique")
    ).reset_index()
    t16_raw["Média Invest./Cliente"] = np.where(t16_raw["Clientes"] == 0, np.nan, t16_raw["Faturamento"] / t16_raw["Clientes"])
    t16_raw["Média Inserções/Cliente"] = np.where(t16_raw["Clientes"] == 0, np.nan, t16_raw["Insercoes"] / t16_raw["Clientes"])

    df_4_main = t16_raw.copy()
    df_4_total = pd.DataFrame()

    if not df_4_main.empty:
        tfat = df_4_main["Faturamento"].sum()
        tins = df_4_main["Insercoes"].sum()
        tcli = base_periodo["cliente"].nunique()
        mfat = tfat/tcli if tcli > 0 else np.nan
        mins = tins/tcli if tcli > 0 else np.nan
        
        df_4_total = pd.DataFrame([{
            "emissora": "Totalizador", "Faturamento": tfat, "Insercoes": tins,
            "Clientes": tcli, "Média Invest./Cliente": mfat, "Média Inserções/Cliente": mins
        }])

    df_4_main.insert(0, "#", range(1, len(df_4_main) + 1))
    df_4_total.insert(0, "#", ["Total"])

    rename_4 = {"emissora": "Emissora", "Insercoes": "Total Inserções"}
    df_4_main = df_4_main.rename(columns=rename_4)
    df_4_total = df_4_total.rename(columns=rename_4)

    for d in [df_4_main, df_4_total]:
        if not d.empty:
            d["Faturamento"] = d["Faturamento"].apply(brl)
            d["Total Inserções"] = d["Total Inserções"].apply(format_int)
            d["Média Invest./Cliente"] = d["Média Invest./Cliente"].apply(brl)
            d["Média Inserções/Cliente"] = d["Média Inserções/Cliente"].apply(lambda x: f"{x:,.1f}" if pd.notna(x) else "-")
            d['#'] = d['#'].astype(str)

    export_4 = display_combined_table(df_4_main, df_4_total, show_total=show_total)
    st.divider()

    # ==================== 5. FATURAMENTO TOTAL ====================
    st.subheader("5. Faturamento por Emissora (Total)")
    t15_simple = base_periodo.groupby("emissora", as_index=False).agg(
        Faturamento=("faturamento", "sum"), Insercoes=("insercoes", "sum")
    ).sort_values("Faturamento", ascending=False)
    t15_simple["Custo Unitário"] = np.where(t15_simple["Insercoes"] > 0, t15_simple["Faturamento"] / t15_simple["Insercoes"], np.nan)

    df_5_main = t15_simple.copy()
    df_5_total = pd.DataFrame()

    if not df_5_main.empty:
        tf = df_5_main["Faturamento"].sum()
        ti = df_5_main["Insercoes"].sum()
        tc = tf/ti if ti > 0 else np.nan
        df_5_total = pd.DataFrame([{"emissora": "Totalizador", "Faturamento": tf, "Insercoes": ti, "Custo Unitário": tc}])

    df_5_main.insert(0, "#", range(1, len(df_5_main)+1))
    df_5_total.insert(0, "#", ["Total"])

    rename_5 = {"emissora": "Emissora", "Insercoes": "Inserções", "Custo Unitário": "Custo Médio Unitário"}
    df_5_main = df_5_main.rename(columns=rename_5)
    df_5_total = df_5_total.rename(columns=rename_5)

    for d in [df_5_main, df_5_total]:
        if not d.empty:
            d["Faturamento"] = d["Faturamento"].apply(brl)
            d["Inserções"] = d["Inserções"].apply(format_int)
            d["Custo Médio Unitário"] = d["Custo Médio Unitário"].apply(brl)
            d['#'] = d['#'].astype(str)

    export_5 = display_combined_table(
        df_5_main, df_5_total, 
        show_total=show_total,
        column_config=get_cmu_config(df_5_main.columns)
    )
    st.divider()

    # ==================== 6. COMPARATIVO MÊS A MÊS ====================
    st.subheader("6. Comparativo mês a mês")
    mes_map = {1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez"}
    base_para_tabela = base_periodo.copy()
    base_para_tabela["mes_nome"] = base_para_tabela["mes"].map(mes_map)
    
    piv_fat = base_para_tabela.groupby(["ano", "mes", "mes_nome"])["faturamento"].sum().reset_index().pivot(index=["mes", "mes_nome"], columns="ano", values="faturamento").fillna(0.0)
    piv_ins = base_para_tabela.groupby(["ano", "mes", "mes_nome"])["insercoes"].sum().reset_index().pivot(index=["mes", "mes_nome"], columns="ano", values="insercoes").fillna(0.0)
    
    if not piv_fat.empty:
        for ano in [ano_base, ano_comp]:
            if ano not in piv_fat.columns: piv_fat[ano] = 0.0
            if ano not in piv_ins.columns: piv_ins[ano] = 0.0
            
        c_base = np.where(piv_ins[ano_base] > 0, piv_fat[ano_base] / piv_ins[ano_base], np.nan)
        c_comp = np.where(piv_ins[ano_comp] > 0, piv_fat[ano_comp] / piv_ins[ano_comp], np.nan)
        
        # Se os anos forem iguais, não duplicamos colunas
        if ano_base == ano_comp:
            t14_final = pd.DataFrame({
                f"Fat. {ano_base}": piv_fat[ano_base],
                f"Ins. {ano_base}": piv_ins[ano_base],
                f"Custo {ano_base}": c_base,
            }, index=piv_fat.index)
        else:
            t14_final = pd.DataFrame({
                f"Fat. {ano_base}": piv_fat[ano_base], f"Fat. {ano_comp}": piv_fat[ano_comp],
                f"Ins. {ano_base}": piv_ins[ano_base], f"Ins. {ano_comp}": piv_ins[ano_comp],
                f"Custo {ano_base}": c_base, f"Custo {ano_comp}": c_comp
            }, index=piv_fat.index)
        
        t14_final = t14_final.sort_index(level="mes")
        
        # Totalizador separado
        total_row_dict = {}
        for col in t14_final.columns:
            if "Custo" not in col: total_row_dict[col] = t14_final[col].sum()
        
        if f"Fat. {ano_base}" in total_row_dict:
            f = total_row_dict[f"Fat. {ano_base}"]
            i = total_row_dict[f"Ins. {ano_base}"]
            total_row_dict[f"Custo {ano_base}"] = f/i if i > 0 else np.nan
             
        if ano_base != ano_comp and f"Fat. {ano_comp}" in total_row_dict:
             f = total_row_dict[f"Fat. {ano_comp}"]
             i = total_row_dict[f"Ins. {ano_comp}"]
             total_row_dict[f"Custo {ano_comp}"] = f/i if i > 0 else np.nan

        df_6_main = t14_final.reset_index(level="mes", drop=True).reset_index()
        df_6_total = pd.DataFrame([total_row_dict])
        
        # Apenas visual
        df_6_main = df_6_main.rename(columns={"mes_nome": "Mês"})
        df_6_total["Mês"] = "Totalizador"

        for d in [df_6_main, df_6_total]:
            d.columns = d.columns.map(str)
            for col in d.columns:
                if "Fat." in col: d[col] = d[col].apply(brl)
                if "Ins." in col: d[col] = d[col].apply(format_int)
                if "Custo" in col:
                    new_name = col.replace("Custo", "Custo Médio Unitário")
                    d.rename(columns={col: new_name}, inplace=True)
                    d[new_name] = d[new_name].apply(brl)

        export_6 = display_combined_table(
            df_6_main, df_6_total, 
            show_total=show_total,
            column_config=get_cmu_config(df_6_main.columns)
        )
    else:
        st.info("Sem dados mensais.")
        export_6 = None
    
    st.divider()

    # ==================== 7. RELAÇÃO DE CLIENTES ====================
    st.subheader(f"7. Relação de Clientes ({ano_base} vs {ano_comp})")
    
    t17_fat = base_periodo.groupby(["cliente", "ano"])["faturamento"].sum().unstack(fill_value=0)
    t17_ins = base_periodo.groupby(["cliente", "ano"])["insercoes"].sum().unstack(fill_value=0)
    
    for ano in [ano_base, ano_comp]:
        if ano not in t17_fat.columns: t17_fat[ano] = 0.0
        if ano not in t17_ins.columns: t17_ins[ano] = 0.0
        
    t17_raw = pd.concat([t17_fat, t17_ins], axis=1)
    if ano_base == ano_comp:
         t17_raw = pd.concat([t17_fat[[ano_base]], t17_ins[[ano_base]]], axis=1)
         t17_raw.columns = [f"Fat_{ano_base}", f"Ins_{ano_base}"]
    else:
         t17_raw.columns = [f"Fat_{ano}" for ano in t17_fat.columns] + [f"Ins_{ano}" for ano in t17_ins.columns]
    
    t17_raw = t17_raw.reset_index()

    cols_fat = [c for c in t17_raw.columns if c.startswith("Fat_")]
    cols_ins = [c for c in t17_raw.columns if c.startswith("Ins_")]
    
    t17_raw["Total Fat"] = t17_raw[cols_fat].sum(axis=1)
    t17_raw["Total Ins"] = t17_raw[cols_ins].sum(axis=1)
    tgf = t17_raw["Total Fat"].sum()
    t17_raw["Share %"] = (t17_raw["Total Fat"] / tgf * 100) if tgf > 0 else 0.0
    
    for cf, ci in zip(cols_fat, cols_ins):
        yr = cf.split("_")[1]
        t17_raw[f"Custo_{yr}"] = np.where(t17_raw[ci] > 0, t17_raw[cf] / t17_raw[ci], np.nan)

    t17_raw = t17_raw.sort_values("Total Fat", ascending=False).reset_index(drop=True)

    df_7_main = t17_raw.copy()
    df_7_total = pd.DataFrame()

    if not df_7_main.empty:
        tot_d = {"cliente": "Totalizador", "Share %": 100.0}
        for c in df_7_main.columns:
            if c not in ["cliente", "Share %"] and not c.startswith("Custo_"):
                tot_d[c] = df_7_main[c].sum()
        
        for cf, ci in zip(cols_fat, cols_ins):
            yr = cf.split("_")[1]
            f, i = tot_d[cf], tot_d[ci]
            tot_d[f"Custo_{yr}"] = f/i if i > 0 else np.nan
            
        df_7_total = pd.DataFrame([tot_d])

    rename_7 = {"cliente": "Cliente", "Total Fat": "Faturamento Total", "Total Ins": "Inserções Total"}
    for c in df_7_main.columns:
        if c.startswith("Fat_"): rename_7[c] = f"Faturamento ({c.split('_')[1]})"
        if c.startswith("Ins_"): rename_7[c] = f"Inserções ({c.split('_')[1]})"
        if c.startswith("Custo_"): rename_7[c] = f"Custo Médio Unitário ({c.split('_')[1]})"

    for d in [df_7_main, df_7_total]:
        if not d.empty:
            d.rename(columns=rename_7, inplace=True)
            for col in d.columns:
                if "Faturamento" in col or "Custo" in col: d[col] = d[col].apply(brl)
                if "Inserções" in col: d[col] = d[col].apply(format_int)
    
    # Ordenação colunas - Agrupar por Tema (Fat 24, Fat 25, Ins 24, Ins 25...)
    final_cols = ["Cliente"]
    years = [ano_base, ano_comp] if ano_base != ano_comp else [ano_base]
    
    # Define a ordem dos temas para comparação
    metrics_order = [
        ("Faturamento", "Faturamento"),
        ("Inserções", "Inserções"),
        ("Custo Médio Unitário", "Custo Médio Unitário")
    ]
    
    for metric_name, prefix in metrics_order:
        for y in years:
            final_cols.append(f"{metric_name} ({y})")
            
    final_cols.extend(["Faturamento Total", "Inserções Total", "Share %"])
    
    # Filtra existentes
    final_cols = [c for c in final_cols if c in df_7_main.columns]
    df_7_main = df_7_main[final_cols]
    df_7_total = df_7_total[final_cols] if not df_7_total.empty else df_7_total

    export_7 = display_combined_table(
        df_7_main, df_7_total,
        format_dict={"Share %": "{:.2f}%"},
        show_total=show_total,
        column_config=get_cmu_config(df_7_main.columns)
    )
    st.divider()

    # ==================== EXPORTAÇÃO ====================
    if st.button("📥 Exportar Dados da Página", type="secondary"):
        st.session_state.show_clientes_export = True
    
    if ultima_atualizacao:
        st.caption(f"📅 Última atualização da base de dados: {ultima_atualizacao}")

    if st.session_state.get("show_clientes_export", False):
        @st.dialog("Opções de Exportação - Clientes & Faturamento")
        def export_dialog():
            
            # Chaves padronizadas com " (Dados)"
            table_options = {
                "1. Número de Clientes por Emissora (Comparativo) (Dados)": {'df': export_1},
                "2. Faturamento por Emissora (com Eficiência) (Dados)": {'df': export_2},
                "3. Faturamento por Executivo (com Eficiência) (Dados)": {'df': export_3},
                "4. Médias por Cliente (Investimento e Inserções) (Dados)": {'df': export_4},
                "5. Faturamento por Emissora (Total) (Dados)": {'df': export_5},
                "6. Comparativo mês a mês (Dados)": {'df': export_6},
                "7. Relação de Clientes Detalhada (Dados)": {'df': export_7},
            }
            
            # Filtra apenas o que existe
            final_options = {k: v for k, v in table_options.items() if v['df'] is not None and not v['df'].empty}

            if not final_options:
                st.warning("Nenhuma tabela com dados foi gerada.")
                if st.button("Fechar", type="secondary"): st.session_state.show_clientes_export = False; st.rerun()
                return

            st.write("Selecione os itens para exportar:")
            selected_names = st.multiselect("Itens", options=final_options.keys(), default=final_options.keys())
            tables_to_export = {name: final_options[name] for name in selected_names}

            if not tables_to_export:
                st.error("Selecione pelo menos um item.")
                return

            try:
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

                filtro_str = get_filter_string()
                zip_data = create_zip_package(tables_to_export, filtro_str, excel_filename="Dashboard_Clientes_Faturamento.xlsx")
                st.download_button("Clique para baixar", data=zip_data, file_name="Dashboard_Clientes_Faturamento.zip", mime="application/zip", on_click=lambda: st.session_state.update(show_clientes_export=False), type="secondary")
            except Exception as e:
                st.error(f"Erro ao gerar ZIP: {e}")

            if st.button("Cancelar", key="cancel_export", type="secondary"):
                st.session_state.show_clientes_export = False
                st.rerun()
        export_dialog()