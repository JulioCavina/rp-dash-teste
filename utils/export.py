# utils/export.py

import io
import zipfile
import pandas as pd
import re

def clean_sheet_name(name):
    """
    Limpa o nome para abas do Excel (max 31 chars).
    """
    # Remove caracteres proibidos
    clean = re.sub(r'[\[\]:*?/\\]', '', str(name))
    
    if len(clean) <= 31:
        return clean
    
    # Estratégia de abreviação: Pega os primeiros 20 chars + ".." + últimos 9 chars
    return clean[:20] + ".." + clean[-9:]

def clean_chart_title(title_key):
    """
    Limpa o título para o gráfico PNG.
    1. Remove numeração inicial (Ex: '1. ')
    2. Remove o texto '(Gráfico)' ou variações.
    """
    # 1. Remove padrão "número + ponto + espaço" do início
    s = re.sub(r'^\d+\.\s*', '', str(title_key))
    
    # 2. Remove o sufixo exato " (Gráfico)"
    s = s.replace(" (Gráfico)", "")
    
    # 3. Limpeza extra para casos como "(Gráfico 2024)" -> "(2024)"
    s = s.replace("(Gráfico ", "(")
    
    return s

def to_excel_with_images(data_dict, filter_info):
    """
    Gera um arquivo Excel em memória contendo DataFrames e Imagens (Plots).
    """
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # --- ABA 1: FILTROS ---
        df_info = pd.DataFrame([{"Filtros Aplicados": filter_info}])
        df_info.to_excel(writer, sheet_name="Filtros", index=False)
        worksheet_filtros = writer.sheets["Filtros"]
        worksheet_filtros.set_column('A:A', 100)
        worksheet_filtros.hide_gridlines(2) 
        
        # --- ABAS DE DADOS E GRÁFICOS ---
        for key, value in data_dict.items():
            sheet_name = clean_sheet_name(key)
            
            # 1. Se for Tabela
            if 'df' in value and value['df'] is not None and not value['df'].empty:
                value['df'].to_excel(writer, sheet_name=sheet_name, index=False)
                worksheet = writer.sheets[sheet_name]
                worksheet.set_column('A:Z', 18)

            # 2. Se for Gráfico
            elif 'fig' in value and value['fig'] is not None:
                pd.DataFrame().to_excel(writer, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                worksheet.hide_gridlines(2)
                
                try:
                    # Limpa o título (Remove "1." e "(Gráfico)")
                    chart_title = clean_chart_title(key)
                    fig_to_export = value['fig']
                    
                    # === REGRAS DE LAYOUT ===
                    layout_args = {
                        'title': {
                            'text': chart_title,
                            'y': 0.95,
                            'x': 0.5,
                            'xanchor': 'center',
                            'yanchor': 'top'
                        },
                        'title_font': dict(size=24, color="#003366", family="Arial, sans-serif"),
                        'margin': dict(t=80), 
                        'paper_bgcolor': 'rgba(0,0,0,0)',
                        'plot_bgcolor': 'rgba(0,0,0,0)'
                    }

                    # === REGRA EXCLUSIVA PARA EVOLUÇÃO MENSAL ===
                    # Empurra título para cima e gráfico para baixo para não bater na legenda
                    if "Evolução Mensal" in key:
                        layout_args['margin'] = dict(t=150)
                        layout_args['title']['y'] = 0.98
                    
                    fig_to_export.update_layout(**layout_args)

                    # Geração da Imagem
                    img_bytes = fig_to_export.to_image(
                        format="png", 
                        width=1200, 
                        height=700, 
                        scale=2, 
                        engine="kaleido"
                    )
                    
                    image_stream = io.BytesIO(img_bytes)
                    worksheet.insert_image('A1', f'{sheet_name}.png', {'image_data': image_stream})
                except Exception as e:
                    print(f"Erro ao converter imagem {key}: {e}")
                    worksheet.write('A1', f"Erro ao gerar imagem: {e}")

    return output.getvalue()

def create_zip_package(data_dict, filter_info, excel_filename="Relatorio.xlsx"):
    output_excel = to_excel_with_images(data_dict, filter_info)
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        if not excel_filename.lower().endswith(".xlsx"):
            excel_filename += ".xlsx"
        zip_file.writestr(excel_filename, output_excel)
    return zip_buffer.getvalue()