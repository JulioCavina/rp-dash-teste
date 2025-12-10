# crowley/eca.py
import streamlit as st
import pandas as pd
import json
import io
from datetime import datetime, timedelta, date

def render(df_crowley, cookies, data_atualizacao):
    pd.set_option("styler.render.max_elements", 2_000_000)

    # --- Header e Voltar ---
    if st.button("Voltar", key="btn_voltar_eca"):
        st.query_params["view"] = "menu"
        st.session_state.pop("eca_search_trigger", None)
        st.rerun()

    st.markdown('<div class="page-title-centered">Relatório ECA</div>', unsafe_allow_html=True)
    st.markdown('<p style="text-align: center; color: #666;">Exclusivos • Compartilhados • Ausentes</p>', unsafe_allow_html=True)
    
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

    # --- COOKIES ---
    saved_filters = {}
    cookie_val = cookies.get("crowley_filters_eca")
    if cookie_val:
        try: saved_filters = json.loads(cookie_val)
        except: pass

    def get_date_from_cookie(key, default_date):
        val = saved_filters.get(key)
        if val:
            try:
                d = datetime.strptime(val, "%Y-%m-%d").date()
                if d < min_date_allowed: return min_date_allowed
                if d > max_date_allowed: return max_date_allowed
                return d
            except: return default_date
        return default_date

    # Defaults
    default_ini = max(min_date_allowed, max_date_allowed - timedelta(days=30))
    val_dt_ini = get_date_from_cookie("dt_ini", default_ini)
    val_dt_fim = get_date_from_cookie("dt_fim", max_date_allowed)
    
    saved_praca = saved_filters.get("praca", None)
    saved_veiculo = saved_filters.get("veiculo", None)
    saved_concorrentes = saved_filters.get("concorrentes", [])

    # --- INTERFACE DE FILTROS ---
    st.markdown("##### Configuração da Análise")
    
    with st.container(border=True):
        # Linha 1: Período e Praça
        c1, c2, c3 = st.columns([1, 1, 1.5])
        with c1:
            dt_ini = st.date_input("Início", value=val_dt_ini, min_value=min_date_allowed, max_value=max_date_allowed, format="DD/MM/YYYY", help=tooltip_dates)
        with c2:
            dt_fim = st.date_input("Fim", value=val_dt_fim, min_value=min_date_allowed, max_value=max_date_allowed, format="DD/MM/YYYY")
        
        lista_pracas = sorted(df_crowley["Praca"].dropna().unique())
        
        def on_praca_change():
            st.session_state["eca_veiculo_key"] = None
            st.session_state["eca_concorrentes_key"] = []
            st.session_state["eca_search_trigger"] = False

        if "eca_praca_key" not in st.session_state:
            idx_praca = lista_pracas.index(saved_praca) if saved_praca in lista_pracas else 0
            st.session_state["eca_praca_key"] = lista_pracas[idx_praca]

        with c3:
            sel_praca = st.selectbox("Praça", options=lista_pracas, key="eca_praca_key", on_change=on_praca_change)

        st.divider()

        # Linha 2: Veículo Alvo e Concorrentes
        # Filtragem dinâmica
        df_praca = df_crowley[df_crowley["Praca"] == sel_praca]
        lista_veiculos_local = sorted(df_praca["Emissora"].dropna().unique())
        
        c4, c5 = st.columns([1, 2])
        
        # Veículo Alvo
        if "eca_veiculo_key" not in st.session_state:
            idx_v = lista_veiculos_local.index(saved_veiculo) if saved_veiculo in lista_veiculos_local else 0
            st.session_state["eca_veiculo_key"] = lista_veiculos_local[idx_v] if lista_veiculos_local else None

        with c4:
            sel_veiculo = st.selectbox("Veículo Alvo (Protagonista)", options=lista_veiculos_local, key="eca_veiculo_key", help="A emissora principal para análise.")

        # Concorrentes
        lista_concorrentes = [v for v in lista_veiculos_local if v != sel_veiculo]
        
        valid_concorrentes = [c for c in saved_concorrentes if c in lista_concorrentes]
        if "eca_concorrentes_key" not in st.session_state:
            st.session_state["eca_concorrentes_key"] = valid_concorrentes

        with c5:
            sel_concorrentes = st.multiselect(
                "Comparar com (Concorrência)", 
                options=lista_concorrentes, 
                key="eca_concorrentes_key",
                placeholder="Se vazio, compara com TODOS da praça"
            )

        st.markdown("<br>", unsafe_allow_html=True)
        submitted = st.button("Gerar Relatório ECA", type="primary", use_container_width=True)

    # --- PROCESSAMENTO ---
    if submitted:
        st.session_state["eca_search_trigger"] = True
        # Save Cookies
        new_filters = {
            "dt_ini": str(dt_ini), "dt_fim": str(dt_fim),
            "praca": sel_praca, "veiculo": sel_veiculo,
            "concorrentes": sel_concorrentes
        }
        cookies["crowley_filters_eca"] = json.dumps(new_filters)
        cookies.save()

    if st.session_state.get("eca_search_trigger"):
        
        # 1. Filtro de Tempo e Praça
        ts_ini = pd.Timestamp(dt_ini)
        ts_fim = pd.Timestamp(dt_fim) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        
        df_base = df_praca[
            (df_praca["Data_Dt"] >= ts_ini) & 
            (df_praca["Data_Dt"] <= ts_fim)
        ].copy()

        # 2. Separação Alvo vs Concorrência
        df_target = df_base[df_base["Emissora"] == sel_veiculo]
        
        if sel_concorrentes:
            df_comp = df_base[df_base["Emissora"].isin(sel_concorrentes)]
        else:
            df_comp = df_base[df_base["Emissora"] != sel_veiculo]

        # 3. Conjuntos de Anunciantes
        set_target = set(df_target["Anunciante"].unique())
        set_comp = set(df_comp["Anunciante"].unique())

        # 4. Lógica ECA
        exclusivos = set_target - set_comp
        compartilhados = set_target & set_comp
        ausentes = set_comp - set_target

        # Helper para montar Tabela Resumo (COM TOTALIZADOR)
        def criar_tabela_resumo(df_src, lista_anunciantes):
            if not lista_anunciantes: return pd.DataFrame()
            
            df_final = df_src[df_src["Anunciante"].isin(lista_anunciantes)].copy()
            
            col_val = "Volume de Insercoes" if "Volume de Insercoes" in df_final.columns else "Contagem"
            if col_val == "Contagem": df_final["Contagem"] = 1
            
            pivot = pd.pivot_table(
                df_final, 
                index="Anunciante", 
                columns="Emissora", 
                values=col_val, 
                aggfunc="sum", 
                fill_value=0,
                observed=True
            )
            
            # Ordena por Total Linha
            pivot["TOTAL"] = pivot.sum(axis=1)
            pivot.sort_values("TOTAL", ascending=False, inplace=True)
            
            # === ADICIONA LINHA TOTAL GERAL ===
            total_row = pivot.sum(numeric_only=True)
            pivot.loc["TOTAL GERAL"] = total_row
            
            return pivot

        # --- EXIBIÇÃO POR ABAS ---
        tab1, tab2, tab3 = st.tabs([
            f"Exclusivos ({len(exclusivos)})", 
            f"Compartilhados ({len(compartilhados)})", 
            f"Ausentes ({len(ausentes)})"
        ])

        # --- TAB 1: EXCLUSIVOS ---
        with tab1:
            if not exclusivos:
                st.info("Nenhum cliente exclusivo encontrado no período.")
                df_exclusivos = pd.DataFrame()
            else:
                df_exclusivos = criar_tabela_resumo(df_target, exclusivos)
                st.dataframe(
                    df_exclusivos.style.background_gradient(cmap="Blues", subset=["TOTAL"]).format("{:.0f}"),
                    width="stretch"
                )

        # --- TAB 2: COMPARTILHADOS ---
        with tab2:
            if not compartilhados:
                st.info("Nenhum cliente compartilhado encontrado.")
                df_compartilhados = pd.DataFrame()
            else:
                df_full_shared = pd.concat([
                    df_target[df_target["Anunciante"].isin(compartilhados)],
                    df_comp[df_comp["Anunciante"].isin(compartilhados)]
                ])
                df_compartilhados = criar_tabela_resumo(df_full_shared, compartilhados)
                
                st.dataframe(
                    df_compartilhados.style.background_gradient(cmap="Oranges", subset=["TOTAL"]).format("{:.0f}"),
                    width="stretch"
                )

        # --- TAB 3: AUSENTES ---
        with tab3:
            if not ausentes:
                st.info("Parabéns! Nenhum cliente ausente encontrado (Capture The Flag!).")
                df_ausentes = pd.DataFrame()
            else:
                df_ausentes = criar_tabela_resumo(df_comp, ausentes)
                st.dataframe(
                    df_ausentes.style.background_gradient(cmap="Reds", subset=["TOTAL"]).format("{:.0f}"),
                    width="stretch"
                )

        st.markdown("<br>", unsafe_allow_html=True)

        # --- DETALHAMENTO GERAL ---
        with st.expander("Fonte de Dados Completa (Detalhamento)", expanded=False):
            df_global_view = pd.concat([df_target, df_comp])
            
            rename_map = {
                "Praca": "Praça", "Anuncio": "Anúncio", "Duracao": "Duração",
                "Emissora": "Veículo", "Volume de Insercoes": "Inserções", "Tipo": "Tipo"
            }
            
            df_detalhe = df_global_view.copy()
            if "Data_Dt" in df_detalhe.columns:
                df_detalhe["Data"] = df_detalhe["Data_Dt"].dt.strftime("%d/%m/%Y")
            
            cols_originais = ["Data", "Anunciante", "Anuncio", "Duracao", "Praca", "Emissora", "Tipo", "Volume de Insercoes"]
            cols_existentes = [c for c in cols_originais if c in df_detalhe.columns]
            
            df_exibicao = df_detalhe[cols_existentes].rename(columns=rename_map)
            df_exibicao.sort_values(by=["Anunciante", "Data"], inplace=True)
            
            # === TOTALIZADOR DETALHADA VISUAL ===
            df_exibicao_display = df_exibicao.copy()
            col_insercoes = "Inserções" if "Inserções" in df_exibicao_display.columns else None
            
            if col_insercoes:
                total_insercoes = df_exibicao_display[col_insercoes].sum()
                
                # Usa "" para colunas de texto para evitar Warning de concatenação
                new_row = {c: "" for c in df_exibicao_display.columns}
                new_row["Anunciante"] = "TOTAL GERAL"
                new_row[col_insercoes] = total_insercoes
                if "Duração" in df_exibicao_display.columns: new_row["Duração"] = 0 

                df_total_row = pd.DataFrame([new_row])
                df_exibicao_display = pd.concat([df_exibicao_display, df_total_row], ignore_index=True)

            st.dataframe(df_exibicao_display, width="stretch", hide_index=True)

        # --- EXPORTAÇÃO ---
        st.markdown("---")
        with st.spinner("Carregando Exportação..."):
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                # 1. Filtros
                filtros_dict = {
                    "Parâmetro": ["Início", "Fim", "Praça", "Veículo Alvo", "Concorrentes Selecionados"],
                    "Valor": [
                        dt_ini.strftime("%d/%m/%Y"), dt_fim.strftime("%d/%m/%Y"), 
                        sel_praca, sel_veiculo,
                        ", ".join(sel_concorrentes) if sel_concorrentes else "Todos da Praça"
                    ]
                }
                pd.DataFrame(filtros_dict).to_excel(writer, sheet_name='Filtros', index=False)
                writer.sheets['Filtros'].set_column('A:B', 40)

                # Helper para exportar
                def export_tab(df, sheet_name):
                    if not df.empty:
                        # A linha TOTAL GERAL já está no dataframe vindo da função criar_tabela_resumo
                        # Então basta exportar direto.
                        df.to_excel(writer, sheet_name=sheet_name)
                        writer.sheets[sheet_name].set_column('A:A', 40)

                # 2. Abas ECA
                export_tab(df_exclusivos, 'Exclusivos')
                export_tab(df_compartilhados, 'Compartilhados')
                export_tab(df_ausentes, 'Ausentes')

                # 3. Detalhamento
                if not df_exibicao.empty:
                    df_det_exp = df_exibicao.copy()
                    if col_insercoes:
                        total_ins = df_det_exp[col_insercoes].sum()
                        
                        new_row_exp = {c: "" for c in df_det_exp.columns}
                        new_row_exp["Anunciante"] = "TOTAL GERAL"
                        new_row_exp[col_insercoes] = total_ins
                        if "Duração" in df_det_exp.columns: new_row_exp["Duração"] = 0
                        
                        df_det_exp = pd.concat([df_det_exp, pd.DataFrame([new_row_exp])], ignore_index=True)
                    
                    df_det_exp.to_excel(writer, sheet_name='Detalhamento', index=False)
                    writer.sheets['Detalhamento'].set_column('A:H', 20)

        c_vazio1, c_vazio2, c_btn, c_vazio3, c_vazio4 = st.columns([1, 1, 1, 1, 1])
        with c_btn:
            st.download_button(
                label="Exportar Relatório Completo (.xlsx)",
                data=buffer,
                file_name=f"Relatorio_ECA_{sel_veiculo}_{sel_praca}_{datetime.now().strftime('%d%m%Y')}.xlsx",
                mime="application/vnd.ms-excel",
                type="secondary", 
                use_container_width=True
            )
        
        st.markdown(f"""
            <div style="text-align: center; color: #666; font-size: 0.8rem; margin-top: 5px;">
                Última atualização da base de dados: {data_atualizacao}
            </div>
        """, unsafe_allow_html=True)