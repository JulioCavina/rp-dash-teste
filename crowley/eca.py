import streamlit as st

def render(df_crowley, cookies, data_atualizacao):
    if st.button("Voltar ao Menu Crowley"):
        st.query_params["view"] = "menu"
        st.rerun()
    st.subheader("Relatório ECA")
    st.info("Módulo em desenvolvimento.")