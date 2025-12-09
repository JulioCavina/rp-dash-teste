# crowley/busca_novos.py
import streamlit as st
import pandas as pd
import json
import io
from datetime import datetime, timedelta

# RECEBE data_atualizacao COMO ARGUMENTO
def render(df_crowley, cookies, data_atualizacao):
    # Aumenta o limite de células para estilização
    pd.set_option("styler.render.max_elements", 2_000_000)

    # Botão voltar discreto no topo esquerdo
    if st.button("Voltar", key="btn_voltar_topo"):
        st.query_params["view"] = "menu"
        st.session_state.pop("novos_search_trigger", None)
        st.rerun()

    # Título Centralizado e Grande
    st.markdown('<div class="page-title-centered">Busca de Novos Anunciantes</div>', unsafe_allow_html=True)
    
    if df_crowley is None or df_crowley.empty:
        st.error("Base de dados não carregada.")
        st.stop()

    # Verifica colunas necessárias
    if "Data_Dt" not in df_crowley.columns:
        if "Data" in df_crowley.columns:
            df_crowley["Data_Dt"] = pd.to_datetime(df_crowley["Data"], dayfirst=True, errors="coerce")
        else:
            st.error("Coluna de Data não encontrada na base.")
            st.stop()

    # --- PREPARAÇÃO DE FILTROS & COOKIES ---
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
                return datetime.strptime(val, "%Y-%m-%d").date()
            except:
                return default_date
        return default_date

    hoje = datetime.now().date()
    default_ini = hoje - timedelta(days=30)
    default_ref_fim = default_ini - timedelta(days=1)
    default_ref_ini = default_ref_fim - timedelta(days=30)

    val_dt_ini = get_date_from_cookie("dt_ini", default_ini)
    val_dt_fim = get_date_from_cookie("dt_fim", hoje)
    val_ref_ini = get_date_from_cookie("ref_ini", default_ref_ini)
    val_ref_fim = get_date_from_cookie("ref_fim", default_ref_fim)
    
    val_praca = saved_filters.get("praca", None)
    val_veiculo = saved_filters.get("veiculo", "Consolidado (Todas as emissoras)") 
    val_anunciantes = saved_filters.get("anunciantes", [])

    # Listas para Selectbox
    lista_pracas = sorted(df_crowley["Praca"].dropna().unique())
    lista_anunciantes_full = sorted(df_crowley["Anunciante"].dropna().unique())
    
    # Lógica do Veículo: Adiciona "Consolidado" no topo
    raw_veiculos = sorted(df_crowley["Emissora"].dropna().unique())
    opcao_consolidado = "Consolidado (Todas as emissoras)"
    lista_veiculos = [opcao_consolidado] + raw_veiculos
    
    idx_praca = lista_pracas.index(val_praca) if val_praca in lista_pracas else 0
    
    if val_veiculo in lista_veiculos:
        idx_veiculo = lista_veiculos.index(val_veiculo)
    else:
        idx_veiculo = 0

    # --- ÁREA DE FILTROS ---
    with st.form(key="form_novos"):
        st.markdown("##### Configuração da Análise")
        
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Período de Análise (Atual)**")
            col_d1, col_d2 = st.columns(2)
            dt_ini = col_d1.date_input("Início", value=val_dt_ini, format="DD/MM/YYYY")
            dt_fim = col_d2.date_input("Fim", value=val_dt_fim, format="DD/MM/YYYY")
        
        with c2:
            st.markdown("**Período de Referência (Comparação)**")
            col_d3, col_d4 = st.columns(2)
            ref_ini = col_d3.date_input("Ref. Início", value=val_ref_ini, format="DD/MM/YYYY")
            ref_fim = col_d4.date_input("Ref. Fim", value=val_ref_fim, format="DD/MM/YYYY")

        st.divider()

        c3, c4, c5 = st.columns([1, 1, 2])
        with c3:
            sel_praca = st.selectbox("Praça", options=lista_pracas, index=idx_praca)
        with c4:
            sel_veiculo = st.selectbox("Veículo Base (Protagonista)", options=lista_veiculos, index=idx_veiculo, help="Selecione 'Consolidado' para ver novos em qualquer emissora.")
        with c5:
            sel_anunciante = st.multiselect("Filtrar Anunciante (Opcional)", options=lista_anunciantes_full, default=val_anunciantes, placeholder="Todos os anunciantes")

        st.markdown("<br>", unsafe_allow_html=True)
        
        # Botão de pesquisa
        submitted = st.form_submit_button("Pesquisar Novos Anunciantes", type="primary", use_container_width=True)

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

    # --- PROCESSAMENTO ---
    if st.session_state.get("novos_search_trigger"):
        
        # 1. Filtro de Praça e Anunciante (Sempre aplica)
        mask_base = (df_crowley["Praca"] == sel_praca)
        if sel_anunciante:
            mask_base = mask_base & (df_crowley["Anunciante"].isin(sel_anunciante))

        # 2. Filtro de Veículo (Só aplica se NÃO for Consolidado)
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
            
            # --- PREPARAÇÃO DOS DADOS DE EXIBIÇÃO ---
            
            # A) PIVOT TABLE
            if "Volume de Insercoes" in df_resultado.columns:
                val_col = "Volume de Insercoes"
                agg_func = "sum"
            else:
                df_resultado["Contagem"] = 1
                val_col = "Contagem"
                agg_func = "count"

            try:
                # observed=True para silenciar warning
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
                
                # Exibe Pivot
                st.markdown("### Visão Geral por Emissora")
                
                st.dataframe(
                    pivot_table.style.background_gradient(cmap="Blues", subset=["TOTAL"]).format("{:.0f}"),
                    width="stretch", 
                    height=min(400, len(pivot_table) * 35 + 40)
                )

            except Exception as e:
                st.error(f"Erro ao gerar tabela dinâmica: {e}")
                pivot_table = pd.DataFrame() # Fallback

            st.markdown("<br>", unsafe_allow_html=True)
            
            # B) TABELA DETALHADA
            rename_map = {
                "Praca": "Praça",
                "Anuncio": "Anúncio",
                "Duracao": "Duração",
                "Emissora": "Veículo",
                "Volume de Insercoes": "Inserções",
                "Tipo": "Tipo"
            }
            
            df_detalhe = df_resultado.copy()
            if "Data_Dt" in df_detalhe.columns:
                df_detalhe["Data"] = df_detalhe["Data_Dt"].dt.strftime("%d/%m/%Y")
            
            cols_originais = ["Data", "Anunciante", "Anuncio", "Duracao", "Praca", "Emissora", "Tipo", "Volume de Insercoes"]
            cols_existentes = [c for c in cols_originais if c in df_detalhe.columns]
            
            df_exibicao = df_detalhe[cols_existentes].rename(columns=rename_map)

            with st.expander("Fonte de Dados (Detalhamento)", expanded=False):
                st.dataframe(
                    df_exibicao.sort_values(by=["Anunciante", "Data"]),
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
                    worksheet_filtros.set_column('A:A', 30)
                    worksheet_filtros.set_column('B:B', 50)
                    
                    if not pivot_table.empty:
                        pivot_table.to_excel(writer, sheet_name='Visão Geral')
                        worksheet_pivot = writer.sheets['Visão Geral']
                        worksheet_pivot.set_column('A:A', 40) 

                    if not df_exibicao.empty:
                        df_exibicao.to_excel(writer, sheet_name='Detalhamento', index=False)
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
            
            # CORREÇÃO DO RODAPÉ AQUI: Usa data_atualizacao
            st.markdown(f"""
                <div style="text-align: center; color: #666; font-size: 0.8rem; margin-top: 5px;">
                    Última atualização da base de dados: {data_atualizacao}
                </div>
            """, unsafe_allow_html=True)