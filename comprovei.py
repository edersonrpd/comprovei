import numpy as np
import requests
import pandas as pd
import zipfile
import io
import re
import os
import xml.etree.ElementTree as ET
import logging
import datetime
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import dotenv_values, load_dotenv
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
# Carregando informações um arquivo externo.
load_dotenv()
config = dotenv_values("config.env")
username = config['USERNAME']
password = config["PASSWORD"]
auth = (username, password)

# Configuração de pastas e arquivos
DATA_DIR = config['DATADIR']
CSV_DATA_DIR = config['CSV_DATA_DIR']
CSV_DATA_DIR_BI = config['CSV_DATA_DIR_BI']
DATA_EXTRACTION_DIR = os.path.join(DATA_DIR, 'extraidos')
CSV_TEMP_DIR = os.path.join(DATA_DIR, 'temp')
CSV_TEMP_OUTPUT_FILE = os.path.join(CSV_TEMP_DIR, 'dados_temp.csv')
CSV_OUTPUT_FILE = os.path.join(CSV_DATA_DIR, 'dados.csv')
CSV_OUTPUT_FILE_BI = os.path.join(CSV_DATA_DIR_BI, 'dados.csv')
EXCEL_OUTPUT_FILE = os.path.join(DATA_DIR, 'dados.xlsx')

# Configure o registro
logging.basicConfig(filename='Log.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Obtendo a data 10 dias atrás como uma string no formato "YYYY-MM-DD"
default_data_inicial = (
    datetime.today() - timedelta(days=10)).strftime('%Y-%m-%d')

periodo = (datetime.today() - timedelta(days=41)).strftime('%Y-%m-%d')

# Obtendo a data atual como uma string no formato "YYYY-MM-DD"
default_data_atual = datetime.today().strftime('%Y-%m-%d')

url_login = 'https://console-api.comprovei.com/exports/documentSAC'


def create_login_payload(data_inicial, data_atual):
    # Não alterar os campos
    return {
        "formato_exportacao": "csv",
        "filtros": {
            "data_inicial": data_inicial,
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


parser = argparse.ArgumentParser(
    description="Seu script para baixar e processar dados do Comprovei SAC")
parser.add_argument('data_inicial', type=str,
                    help="Data inicial no formato 'YYYY-MM-DD' ou 'hoje' para a data atual")
parser.add_argument('data_atual', type=str,
                    help="Data atual (final) no formato 'YYYY-MM-DD' ou 'hoje' para a data atual")

args = parser.parse_args()

if args.data_inicial.lower() == 'hoje':
    data_inicial = datetime.today().strftime('%Y-%m-%d')
elif args.data_inicial.lower() == 'tres':
    data_inicial = (datetime.today() - timedelta(days=3)).strftime('%Y-%m-%d')
elif args.data_inicial.lower() == 'cinco':
    data_inicial = (datetime.today() - timedelta(days=5)).strftime('%Y-%m-%d')
elif args.data_inicial.lower() == 'dez':
    data_inicial = (datetime.today() - timedelta(days=10)).strftime('%Y-%m-%d')
elif args.data_inicial.lower() == 'ontem':
    data_inicial = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
else:
    data_inicial = args.data_inicial

if args.data_atual.lower() == 'hoje':
    data_atual = datetime.today().strftime('%Y-%m-%d')
else:
    data_atual = args.data_atual

login_payload = create_login_payload(data_inicial, data_atual)


def autenticar_e_solicitar_dados(data_inicial, data_atual):
    login_payload = create_login_payload(data_inicial, data_atual)
    try:
        response = requests.post(url_login, auth=auth, json=login_payload)
        response.raise_for_status()

        if 'erro' in response.json():
            print("Erro na resposta da API: ", response.json()['erro'])
            return None
    except HTTPError as exc:
        print(exc)
        logging.error(f'URL {url_login} não encontrada.')
    return response


retorno = autenticar_e_solicitar_dados(data_inicial, data_atual)

zip_url = None


if retorno.status_code == 200:
    text = retorno.text

    # Use uma expressão regular para encontrar uma URL no texto
    url_pattern = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    match = url_pattern.search(text)

    if match:
        zip_url = match.group()
        print(f"A URL do arquivo ZIP é: {zip_url}")
    text = retorno.text
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
        zip_ref.extractall(DATA_EXTRACTION_DIR)

    print("Arquivo baixado e extraído com sucesso!")
else:
    print(f"Erro ao baixar o arquivo: {response.status_code}")

# Lista de arquivos no diretório ordenados por data de criação
arquivos = sorted(Path(DATA_EXTRACTION_DIR).glob('*.csv'))
tipos_colunas = {
    'Documento': str,
    'CNPJ Embarcador': str,
    'CNPJ Cliente': str,
    'CNPJ Transp.': str,
    'Status': str,
    'Modelo': str,
    'Gerente Cód.': str,
    'Gerente Nome': str,
    'Gerente Emai': str,
    'Gerente Tel.': str,
    'Supervisor Cód.': str,
    'Supervisor Nome': str,
    'Supervisor Email': str,
    'Supervisor Tel.': str,
    'Gerente Sênior Cód.': str,
    'Gerente Sênior Nome': str,
    'Gerente Sênior Email': str,
    'Gerente Sênior Tel.': str,
    'Vendedor Cód.': str,
    'Vendedor Nome': str,
    'Vendedor Email': str,
    'Vendedor Tel.': str,
    'Pedido': str,
    'AWB': str,
    'Remessa': str,
    'Data Pagamento': str,
    'Data Agendamento': str
}

# Se existir arquivo temporário, ler o arquivo temporário e  concatenar com os arquivos novos
# Senão existir, criar um dataframe vazio
if os.path.isfile(CSV_TEMP_OUTPUT_FILE):
    lista_dfs = [pd.read_csv(CSV_TEMP_OUTPUT_FILE,
                             dtype=tipos_colunas, sep=';', low_memory=False)]
    # dtype=tipos_colunas, low_memory=False, sep=';']
else:
    lista_dfs = []


def processar_csv():
    arquivos = sorted(Path(DATA_EXTRACTION_DIR).glob('*.csv'))

    for arquivo in arquivos:
        try:
            filename = arquivo.name
            if filename != 'dados.csv':
                # Ler o arquivo csv e armazenar em um DataFrame
                df = pd.read_csv(os.path.join(DATA_EXTRACTION_DIR, filename),
                                 dtype=tipos_colunas, low_memory=False)

                lista_dfs.append(df)
        except Exception as e:
            print(f"Erro ao ler o arquivo {filename}: {e}")
    df_concatenado = pd.concat(lista_dfs, ignore_index=True)
    print("Arquivos CSV concatenados com sucesso!")
    return df_concatenado


df_concatenado = processar_csv()

colunas = ['Pedido']

for coluna in colunas:
    df_concatenado[coluna] = df_concatenado[coluna].astype(pd.Int64Dtype())


def preenche_colunas_vazio(dataframe):
    dataframe[['Tipo', 'Modelo', 'CNPJ Embarcador',
               'CNPJ Cliente', 'Código Cliente']] = np.nan
    return dataframe


# Preenche colunas não utilizadas no BI por vazio
df_concatenado = preenche_colunas_vazio(df_concatenado)


def drop_duplicates(df_concatenado):
    df_concatenado = df_concatenado.drop_duplicates(
        subset=['Documento', 'Chave'], keep='last')
    return df_concatenado


# Elimina duplicados
df_concatenado = drop_duplicates(df_concatenado)

# Filtra apenas dados superiores a variavel mes
df_concatenado = df_concatenado[df_concatenado['Emissão'] >= periodo]


def save_output(df_concatenado):
    # df_concatenado.to_csv(CSV_OUTPUT_FILE, index=False, sep=';')
    df_concatenado.to_csv(CSV_TEMP_OUTPUT_FILE, index=False, sep=';')
    df_concatenado.to_csv(CSV_OUTPUT_FILE_BI, index=False, sep=';')
    logging.info(f'Arquivo {CSV_OUTPUT_FILE} salvo com sucesso')
    df_concatenado.to_excel(EXCEL_OUTPUT_FILE, index=False)


def clean_directory(directory, keep_file):
    for file in os.listdir(directory):
        file_path = os.path.join(directory, file)
        if os.path.isfile(file_path) and file_path != keep_file:
            os.remove(file_path)
            print(f"Arquivo {file_path} excluído com sucesso!")
            logging.info(f'Arquivo {file_path} excluído com sucesso!')


if __name__ == '__main__':
    create_login_payload(data_inicial, data_atual)
    save_output(df_concatenado)
    clean_directory(DATA_EXTRACTION_DIR, CSV_OUTPUT_FILE)
