# pages/inicio.py

import streamlit as st

def render(df=None):
    # ==================== CSS GLOBAL PARA ESTA PÁGINA ====================
    st.markdown("""
        <style>
        /* 1. CENTRALIZAÇÃO DE TÍTULOS E TEXTOS GLOBAIS */
        h1, h2, h3, h4, h5, h6, .stMarkdown p, .stCaption {
            text-align: center !important;
        }

        /* 2. CENTRALIZAÇÃO DO GRID DE BOTÕES */
        .nb-container {
            display: flex;
            justify-content: center;
            align-items: center;
            flex-direction: column;
            width: 100%;
            margin-top: 1rem;
        }

        .nb-grid {
            display: grid;
            grid-template-columns: repeat(3, 240px);
            grid-template-rows: repeat(3, 130px);
            gap: 1.5rem;
            justify-content: center;
        }
        
        /* Estilo dos Cards (Botões) */
        .nb-card {
            background-color: #007dc3;
            border: 2px solid white;
            border-radius: 15px;
            color: white !important;
            text-decoration: none !important; 
            font-size: 1rem;
            font-weight: 600;
            height: 120px;
            width: 240px;
            display: flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            box-shadow: 0 4px 10px rgba(0, 0, 0, 0.15);
            transition: all 0.25s ease-in-out;
            text-align: center;
        }
        
        .nb-card:hover {
            background-color: #00a8e0;
            transform: scale(1.05);
            box-shadow: 0 6px 12px rgba(0, 0, 0, 0.25);
            text-decoration: none !important; 
        }

        .nb-card:active {
            transform: scale(0.97);
            background-color: #004b8d;
        }
        
        /* Texto introdutório centralizado */
        .intro-text {
            text-align: center !important;
            font-size: 1.1rem;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            line-height: 1.6;
            display: block;
        }

        @media (max-width: 900px) {
            .nb-grid {
                grid-template-columns: repeat(2, 200px);
            }
            .nb-card {
                width: 200px;
                height: 110px;
            }
        }
        </style>
    """, unsafe_allow_html=True)
    
    # ==================== TÍTULO E INTRODUÇÃO ====================
    st.markdown("""
    <h1 style='text-align: center; color: #003366; margin-bottom: 2rem;'>Dashboard Vendas</h1>
    
    <h2 style='text-align: center; color: #003366; margin-top: 0px;'>Bem-vindo(a)!</h2>
    <div class='intro-text'>
        Este painel foi desenvolvido para a equipe da <b>Novabrasil</b> com o objetivo de oferecer 
        uma visão completa sobre o desempenho comercial e de marketing.
    </div>
    <br>
    <h3 style='text-align: center; color: #444; font-size: 1rem;'>Acesse diretamente uma das seções:</h3>
    """, unsafe_allow_html=True)

    # ==================== BOTÕES (GRID) ====================
    # O Botão "Top 10 Anunciantes" foi renomeado para "Top Anunciantes"
    st.markdown("""
    <div class="nb-container">
      <div class="nb-grid">
        <a href="?nav=1" target="_self" class="nb-card">Visão Geral</a>
        <a href="?nav=2" target="_self" class="nb-card">Clientes & Faturamento</a>
        <a href="?nav=3" target="_self" class="nb-card">Perdas & Ganhos</a>
        <a href="?nav=4" target="_self" class="nb-card">Cruzamentos & Interseções</a>
        <a href="?nav=5" target="_self" class="nb-card">Top Anunciantes</a>
        <a href="?nav=6" target="_self" class="nb-card">Relatório ABC</a>
        <a href="?nav=7" target="_self" class="nb-card">Eficiência / KPIs</a>
        <a href="https://novabrasil-datadriven-crowley.streamlit.app" target="_blank" class="nb-card">Relatório Crowley</a>
      </div>
    </div>
    """, unsafe_allow_html=True)