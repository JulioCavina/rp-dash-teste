# crowley/busca_novos.py
import streamlit as st
import pandas as pd
import json
import io
from datetime import datetime, timedelta, date

def render(df_crowley, cookies, data_atualizacao):
    pd.set_option("styler.render.max_elements", 2_000_000)

    # --- Header e Voltar ---
    if st.button("Voltar", key="btn_voltar_topo"):
        st.query_params["view"] = "menu"
        st.session_state.pop("novos_search_trigger", None)
        st.rerun()

    st.markdown('<div class="page-title-centered">Busca de Novos Anunciantes</div>', unsafe_allow_html=True)
    
    if df_crowley is None or df_crowley.empty:
        st.error("Base de dados não carregada.")
        st.stop()

    if "Data_Dt" not in df_crowley.columns:
        if "Data" in df_crowley.columns:
            df_crowley["Data_Dt"] = pd.to_datetime(df_crowley["Data"], dayfirst=True, errors="coerce")
        else:
            st.error("Coluna de Data não encontrada na base.")
            st.stop()

    # --- CONFIGURAÇÃO DE DATAS LIMITES ---
    min_date_allowed = date(2024, 1, 1)
    
    try:
        max_date_allowed = datetime.strptime(data_atualizacao, "%d/%m/%Y").date()
    except:
        max_date_allowed = datetime.now().date()
        
    tooltip_dates = f"Dados disponíveis para pesquisa:\nDe 01/01/2024 até {data_atualizacao}"

    # --- COOKIES E FILTROS SALVOS ---
    saved_filters = {}
    cookie_val = cookies.get("crowley_filters_novos")
    if cookie_val:
        try:
            saved_filters = json.loads(cookie_val)
        except:
            pass

    def get_date_from_cookie(key, default_date):
        val = saved_filters.get(key)
        if val:
            try:
                d = datetime.strptime(val, "%Y-%m-%d").date()
                if d < min_date_allowed: return min_date_allowed
                if d > max_date_allowed: return max_date_allowed
                return d
            except:
                return default_date
        return default_date

    # Datas Default
    default_ini = max(min_date_allowed, max_date_allowed - timedelta(days=30))
    default_ref_fim = max(min_date_allowed, default_ini - timedelta(days=1))
    default_ref_ini = max(min_date_allowed, default_ref_fim - timedelta(days=30))

    val_dt_ini = get_date_from_cookie("dt_ini", default_ini)
    val_dt_fim = get_date_from_cookie("dt_fim", max_date_allowed)
    val_ref_ini = get_date_from_cookie("ref_ini", default_ref_ini)
    val_ref_fim = get_date_from_cookie("ref_fim", default_ref_fim)
    
    saved_praca = saved_filters.get("praca", None)
    saved_veiculo = saved_filters.get("veiculo", "Consolidado (Todas as emissoras)")
    saved_anunciantes = saved_filters.get("anunciantes", [])

    # --- INTERFACE DE FILTROS ---
    st.markdown("##### Configuração da Análise")
    
    with st.container(border=True):
        
        # 1. Datas
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Período de Análise (Atual)**", help=tooltip_dates)
            col_d1, col_d2 = st.columns(2)
            dt_ini = col_d1.date_input("Início", value=val_dt_ini, min_value=min_date_allowed, max_value=max_date_allowed, format="DD/MM/YYYY")
            dt_fim = col_d2.date_input("Fim", value=val_dt_fim, min_value=min_date_allowed, max_value=max_date_allowed, format="DD/MM/YYYY")
        
        with c2:
            st.markdown("**Período de Referência (Comparação)**", help=tooltip_dates)
            col_d3, col_d4 = st.columns(2)
            ref_ini = col_d3.date_input("Ref. Início", value=val_ref_ini, min_value=min_date_allowed, max_value=max_date_allowed, format="DD/MM/YYYY")
            ref_fim = col_d4.date_input("Ref. Fim", value=val_ref_fim, min_value=min_date_allowed, max_value=max_date_allowed, format="DD/MM/YYYY")

        st.divider()

        # 2. Filtros em Cascata
        c3, c4, c5 = st.columns([1, 1, 2])
        
        lista_pracas = sorted(df_crowley["Praca"].dropna().unique())
        
        def on_praca_change():
            st.session_state["crowley_veiculo_key"] = "Consolidado (Todas as emissoras)"
            st.session_state["crowley_anunc_key"] = []
            st.session_state["novos_search_trigger"] = False

        if "crowley_praca_key" not in st.session_state:
            idx_praca = lista_pracas.index(saved_praca) if saved_praca in lista_pracas else 0
            st.session_state["crowley_praca_key"] = lista_pracas[idx_praca]

        with c3:
            sel_praca = st.selectbox(
                "Praça", 
                options=lista_pracas, 
                key="crowley_praca_key",
                on_change=on_praca_change
            )

        df_praca_filtered = df_crowley[df_crowley["Praca"] == sel_praca]
        
        lista_anunciantes_local = sorted(df_praca_filtered["Anunciante"].dropna().unique())
        raw_veiculos_local = sorted(df_praca_filtered["Emissora"].dropna().unique())
        
        opcao_consolidado = "Consolidado (Todas as emissoras)"
        lista_veiculos_local = [opcao_consolidado] + raw_veiculos_local
        
        if "crowley_veiculo_key" not in st.session_state:
            val_inicial = saved_veiculo if saved_veiculo in lista_veiculos_local else opcao_consolidado
            st.session_state["crowley_veiculo_key"] = val_inicial

        with c4:
            sel_veiculo = st.selectbox(
                "Veículo Base (Protagonista)", 
                options=lista_veiculos_local,
                key="crowley_veiculo_key",
                help="Selecione 'Consolidado' para ver novos em qualquer emissora."
            )
            
        anunciantes_validos = [a for a in saved_anunciantes if a in lista_anunciantes_local]
        if "crowley_anunc_key" not in st.session_state:
            st.session_state["crowley_anunc_key"] = anunciantes_validos

        with c5:
            sel_anunciante = st.multiselect(
                "Filtrar Anunciante (Opcional)", 
                options=lista_anunciantes_local,
                key="crowley_anunc_key",
                placeholder="Todos os anunciantes desta praça"
            )

        st.markdown("<br>", unsafe_allow_html=True)
        submitted = st.button("Pesquisar Novos Anunciantes", type="primary", use_container_width=True)

    # --- LÓGICA DE PROCESSAMENTO ---
    
    if submitted:
        st.session_state["novos_search_trigger"] = True
        
        new_filters = {
            "dt_ini": str(dt_ini), "dt_fim": str(dt_fim),
            "ref_ini": str(ref_ini), "ref_fim": str(ref_fim),
            "praca": sel_praca, "veiculo": sel_veiculo,
            "anunciantes": sel_anunciante
        }
        cookies["crowley_filters_novos"] = json.dumps(new_filters)
        cookies.save()

    if st.session_state.get("novos_search_trigger"):
        
        mask_base = (df_crowley["Praca"] == sel_praca)
        if sel_anunciante:
            mask_base = mask_base & (df_crowley["Anunciante"].isin(sel_anunciante))

        if sel_veiculo != opcao_consolidado:
            mask_base = mask_base & (df_crowley["Emissora"] == sel_veiculo)

        df_base = df_crowley[mask_base]

        ts_ini = pd.Timestamp(dt_ini)
        ts_fim = pd.Timestamp(dt_fim) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        ts_ref_ini = pd.Timestamp(ref_ini)
        ts_ref_fim = pd.Timestamp(ref_fim) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        df_atual = df_base[(df_base["Data_Dt"] >= ts_ini) & (df_base["Data_Dt"] <= ts_fim)]
        df_ref = df_base[(df_base["Data_Dt"] >= ts_ref_ini) & (df_base["Data_Dt"] <= ts_ref_fim)]

        anunciantes_atual = set(df_atual["Anunciante"].unique())
        anunciantes_ref = set(df_ref["Anunciante"].unique())
        novos_anunciantes = anunciantes_atual - anunciantes_ref

        if not novos_anunciantes:
            st.warning(f"Nenhum anunciante novo encontrado na **{sel_praca}** neste período comparativo.")
        else:
            st.success(f"Encontrados **{len(novos_anunciantes)}** novos anunciantes em relação ao período anterior!")
            
            df_resultado = df_atual[df_atual["Anunciante"].isin(novos_anunciantes)].copy()
            
            # --- A) PIVOT TABLE ---
            val_col = "Volume de Insercoes" if "Volume de Insercoes" in df_resultado.columns else "Contagem"
            if val_col == "Contagem": df_resultado["Contagem"] = 1
            agg_func = "sum" if val_col == "Volume de Insercoes" else "count"

            pivot_table = pd.DataFrame()
            try:
                pivot_table = pd.pivot_table(
                    df_resultado,
                    index="Anunciante",
                    columns="Emissora",
                    values=val_col,
                    aggfunc=agg_func,
                    fill_value=0,
                    observed=True 
                )
                pivot_table["TOTAL"] = pivot_table.sum(axis=1)
                pivot_table = pivot_table.sort_values(by="TOTAL", ascending=False)
                
                st.markdown("### Visão Geral por Emissora")
                
                st.dataframe(
                    pivot_table.style.background_gradient(cmap="Blues", subset=["TOTAL"]).format("{:.0f}"),
                    width="stretch", 
                    height=min(450, len(pivot_table) * 35 + 40)
                )

            except Exception as e:
                st.error(f"Erro ao gerar tabela dinâmica: {e}")

            st.markdown("<br>", unsafe_allow_html=True)
            
            # --- B) TABELA DETALHADA ---
            rename_map = {
                "Praca": "Praça", "Anuncio": "Anúncio", "Duracao": "Duração",
                "Emissora": "Veículo", "Volume de Insercoes": "Inserções", "Tipo": "Tipo"
            }
            
            df_detalhe = df_resultado.copy()
            if "Data_Dt" in df_detalhe.columns:
                df_detalhe["Data"] = df_detalhe["Data_Dt"].dt.strftime("%d/%m/%Y")
            
            cols_originais = ["Data", "Anunciante", "Anuncio", "Duracao", "Praca", "Emissora", "Tipo", "Volume de Insercoes"]
            cols_existentes = [c for c in cols_originais if c in df_detalhe.columns]
            
            df_exibicao = df_detalhe[cols_existentes].rename(columns=rename_map)
            df_exibicao.sort_values(by=["Anunciante", "Data"], inplace=True)

            # --- TOTALIZADOR DETALHADA VISUAL (CORRIGIDO) ---
            df_exibicao_display = df_exibicao.copy()
            col_insercoes = "Inserções" if "Inserções" in df_exibicao_display.columns else None
            
            if col_insercoes:
                total_insercoes = df_exibicao_display[col_insercoes].sum()
                
                # CORREÇÃO: Usa "" em vez de None para evitar warnings de concatenação
                new_row = {c: "" for c in df_exibicao_display.columns}
                new_row["Anunciante"] = "TOTAL GERAL"
                new_row[col_insercoes] = total_insercoes
                if "Duração" in df_exibicao_display.columns:
                    new_row["Duração"] = 0 # Garante int se coluna for int

                df_total_row = pd.DataFrame([new_row])
                df_exibicao_display = pd.concat([df_exibicao_display, df_total_row], ignore_index=True)

            with st.expander("Fonte de Dados (Detalhamento)", expanded=False):
                st.dataframe(
                    df_exibicao_display,
                    width="stretch", 
                    hide_index=True
                )

            # --- EXPORTAÇÃO EXCEL ---
            st.markdown("---")
            with st.spinner("Carregando Exportação..."):
                filtros_dict = {
                    "Parâmetro": [
                        "Período de Análise (Início)", "Período de Análise (Fim)",
                        "Período de Referência (Início)", "Período de Referência (Fim)",
                        "Praça Selecionada", "Veículo Base", "Filtro Anunciantes"
                    ],
                    "Valor": [
                        dt_ini.strftime("%d/%m/%Y"), dt_fim.strftime("%d/%m/%Y"),
                        ref_ini.strftime("%d/%m/%Y"), ref_fim.strftime("%d/%m/%Y"),
                        sel_praca, sel_veiculo,
                        ", ".join(sel_anunciante) if sel_anunciante else "Todos"
                    ]
                }
                df_filtros_export = pd.DataFrame(filtros_dict)
                
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    df_filtros_export.to_excel(writer, sheet_name='Filtros', index=False)
                    worksheet_filtros = writer.sheets['Filtros']
                    worksheet_filtros.set_column('A:A', 30); worksheet_filtros.set_column('B:B', 50)
                    
                    if not pivot_table.empty:
                        pivot_export = pivot_table.copy()
                        total_row = pivot_export.sum(numeric_only=True)
                        pivot_export.loc["TOTAL GERAL"] = total_row
                        
                        pivot_export.to_excel(writer, sheet_name='Visão Geral')
                        worksheet_pivot = writer.sheets['Visão Geral']
                        worksheet_pivot.set_column('A:A', 40) 

                    if not df_exibicao.empty:
                        df_exibicao_export = df_exibicao.copy()
                        if col_insercoes:
                            total_ins = df_exibicao_export[col_insercoes].sum()
                            
                            # CORREÇÃO NA EXPORTAÇÃO: Usa "" em vez de None
                            new_row_exp = {c: "" for c in df_exibicao_export.columns}
                            new_row_exp["Anunciante"] = "TOTAL GERAL"
                            new_row_exp[col_insercoes] = total_ins
                            if "Duração" in df_exibicao_export.columns:
                                new_row_exp["Duração"] = 0
                            
                            df_exibicao_export = pd.concat([df_exibicao_export, pd.DataFrame([new_row_exp])], ignore_index=True)

                        df_exibicao_export.to_excel(writer, sheet_name='Detalhamento', index=False)
                        worksheet_detalhe = writer.sheets['Detalhamento']
                        worksheet_detalhe.set_column('A:H', 20)

            c_vazio1, c_vazio2, c_btn, c_vazio3, c_vazio4 = st.columns([1, 1, 1, 1, 1])
            with c_btn:
                st.download_button(
                    label="Exportar Relatório Completo (.xlsx)",
                    data=buffer,
                    file_name=f"Relatorio_Novos_Anunciantes_{sel_praca.replace(' ', '_')}_{datetime.now().strftime('%d%m%Y')}.xlsx",
                    mime="application/vnd.ms-excel",
                    type="secondary", 
                    use_container_width=True
                )
            
            st.markdown(f"""
                <div style="text-align: center; color: #666; font-size: 0.8rem; margin-top: 5px;">
                    Última atualização da base de dados: {data_atualizacao}
                </div>
            """, unsafe_allow_html=True)