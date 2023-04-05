import requests
import pandas as pd
import json
import zipfile
import io
import re
import os
import xml.etree.ElementTree as ET
import logging
import datetime
from datetime import datetime, timedelta

# Configure o registro
logging.basicConfig(filename='Log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Obtendo a data atual como uma string no formato "YYYY-MM-DD"

data_inicial = datetime.today() - timedelta(days=9)
data_inicial = data_inicial.strftime('%Y-%m-%d')
data_atual = datetime.today().strftime('%Y-%m-%d')


url_login = 'https://console-api.comprovei.com/exports/documentSAC'

# Adicione as credenciais de autenticação como uma tupla
auth = ("dislab", "qO5e6CYfma3SzW51AftBxPLYb59gurCn")
login_payload = {
    "formato_exportacao": "csv",
    "filtros": {
        "data_inicial": data_atual,
        "data_final": data_atual

    },
    "campos": [
        "Documento",
        "Emissão",
        "CNPJ Embarcador",
        "Embarcador",
        "Região",
        "Modelo",
        "CNPJ Cliente",
        "Código Cliente",
        "Código Int Cliente",
        "Tipo",
        "Cliente",
        "Cidade Destino",
        "UF Destino",
        "Data Finalização",
        "Ultima Ocorrência",
        "Status",
        "Data Pagamento",
        "Data Agendamento",
        "Qtd Reentregas",
        "Qtd Paradas",
        "Chave",
        "Valor",
        "Volume",
        "Qtd volumes",
        "Conferidos",
        "Rota/Roteiro",
        "Motorista",
        "Cód. Motorista",
        "Placa",
        "Data da rota",
        "Transportadora",
        "CNPJ Transp.",
        "Data Últ. Ocorr.",
        "Gerente Cód.",
        "Gerente Nome",
        "Gerente Email",
        "Gerente Tel.",
        "Supervisor Cód.",
        "Supervisor Nome",
        "Supervisor Email",
        "Supervisor Tel.",
        "Gerente Sênior Cód.",
        "Gerente Sênior Nome",
        "Gerente Sênior Email",
        "Gerente Sênior Tel.",
        "Vendedor Cód.",
        "Vendedor Nome",
        "Vendedor Email",
        "Vendedor Tel.",
        "Pedido",
        "Base Origem",
        "Base Destino",
        "Prazo SLA",
        "Status SLA",
        "Tipo de Frete",
        "Modal",
        "Data Atualização",
        "AWB",
        "Remessa",
        "Possui Foto",
        "Performance SLA",
        "Justificativa",
        "Acatado",
        "Comentário da Justificativa",
        "Chegada Cliente",
        "Ajuste Manual",
        "Horario Ajuste Manual",
        "Usuário Ajuste Manual",
        "Código IBGE Cidade",
        "BU",
        "CFOP",
        "Campo Livre 1",
        "Campo Livre 2",
        "Campo Livre 3",
        "Campo Livre 4",
        "Campo Livre 5",
        "Email SLA Atrasado"
    ]


}

response = requests.post(url_login, auth=auth, json=login_payload)

zip_url = None

if response.status_code == 200:
    text = response.text

    # Use uma expressão regular para encontrar uma URL no texto
    url_pattern = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    match = url_pattern.search(text)

    if match:
        zip_url = match.group()
        print(f"A URL do arquivo ZIP é: {zip_url}")
else:
    print(f"Não encontrei dados de exportação do dia {data_atual}")
    logging.error(f"Não encontrei dados de exportação do dia {data_atual}")



if zip_url != None:    
     url = zip_url  # Substitua pelo URL do arquivo zip que você deseja baixar
else:
    raise Exception('A URL do arquivo ZIP não foi encontrada.')


# Baixar o arquivo zip
response = requests.get(url)

# Verificar se a resposta é bem-sucedida
if response.status_code == 200:
    # Ler o conteúdo do arquivo zip
    arquivo_zip = io.BytesIO(response.content)

    # Abrir e extrair o arquivo zip
    with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
        # Substitua pelo diretório em que deseja extrair os arquivos
        zip_ref.extractall("C:\ComproveiSAC\extraidos")

    print("Arquivo baixado e extraído com sucesso!")
else:
    print(f"Erro ao baixar o arquivo: {response.status_code}")


dir_csv = 'C:\ComproveiSAC\extraidos'  # Substitua pelo diretório onde estão os arquivos XML
# Substitua pelo nome do arquivo de saída
arquivo_saida = 'C:\ComproveiSAC\dados.csv'
# Substitua pelo nome do arquivo de saída
arquivo_saida_excel = 'C:\ComproveiSAC\dados.xlsx'

# Lista para armazenar os DataFrames
lista_dfs = []

# Iterar pelos arquivos csv no diretório especificado
for filename in os.listdir(dir_csv):
    if filename.endswith('.csv') and filename != 'dados.csv':
        # Ler o arquivo csv e armazenar em um DataFrame
        df = pd.read_csv(os.path.join(dir_csv, filename))

        # Adicionar o DataFrame à lista
        lista_dfs.append(df)

# Concatenar todos os DataFrames na lista
df_concatenado = pd.concat(lista_dfs, ignore_index=True)
df_concatenado = pd.DataFrame(df_concatenado)

# Excluindo linhas duplicadas
df_concatenado = df_concatenado.drop_duplicates()
#df_concatenado = df_concatenado.sort_values(by=['Emissão'], ascending=False)
print("Arquivos CSV concatenados com sucesso!")

#Alterando o type de algumas colunas
colunas = ['Pedido', 'CNPJ Embarcador', 'CNPJ Cliente', 'CNPJ Transp.']

for coluna in colunas:
    df_concatenado[coluna] = df_concatenado[coluna].astype(pd.Int64Dtype())

df_concatenado = df_concatenado.sort_index()
df_concatenado = df_concatenado.drop_duplicates(subset=['Documento'], keep='last')
df_concatenado = df_concatenado.sort_values(by=['Emissão'], ascending=False)

# Salvar o arquivo CSV concatenado
df_concatenado.to_csv(arquivo_saida, index=False, sep=';')
logging.info(f'Arquivo {arquivo_saida} salvo com sucesso')
df_concatenado.to_excel(arquivo_saida_excel, index=False)
