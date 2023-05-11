import requests
from requests.exceptions import HTTPError
import pandas as pd
import zipfile
import io
import re
import os
import logging
import datetime
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import dotenv_values, load_dotenv

default_data_atual = datetime.today().strftime('%Y-%m-%d')
dia_atual = datetime.today().strftime('%Y%m%d%H%M')

# Carregando informações um arquivo externo.
load_dotenv()
config = dotenv_values("config.env")
username = config['USERNAME']
password = config["PASSWORD"]
auth = (username, password)

# Configuração de pastas e arquivos
DATA_DIR = config['DATADIR']
CSV_DATA_DIR = config['CSV_DATA_DIR']
DATA_EXTRACTION_DIR = os.path.join(DATA_DIR, 'extraidos')
CSV_OUTPUT_TMP_DIR = os.path.join(DATA_DIR, 'temp')
CSV_OUTPUT_TMP_FILE = os.path.join(
    CSV_OUTPUT_TMP_DIR, f'dados_{dia_atual}.csv')
CSV_OUTPUT_FILE = os.path.join(CSV_DATA_DIR, 'dados_csv')
EXCEL_OUTPUT_FILE = os.path.join(DATA_DIR, 'dados.xlsx')


# Configure o registro
logging.basicConfig(filename='Log.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Obtendo a data 10 dias atrás como uma string no formato "YYYY-MM-DD"
default_data_inicial = (
    datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

# Obtendo a data atual como uma string no formato "YYYY-MM-DD"
default_data_atual = datetime.today().strftime('%Y-%m-%d')

url_login = 'https://console-api.comprovei.com/exports/documentSAC'


def create_login_payload(data_inicial, data_atual):
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

args = parser.parse_args(['hoje', 'hoje'])

if args.data_inicial.lower() == 'hoje':
    data_inicial = datetime.today().strftime('%Y-%m-%d')
elif args.data_inicial.lower() == 'ontem':
    data_inicial = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
else:
    data_inicial = args.data_inicial

if args.data_atual.lower() == 'hoje':
    data_atual = datetime.today().strftime('%Y-%m-%d')
else:
    data_atual = args.data_atual

def processar_dados_comprovei_sac(data_inicial, data_atual):
    if data_inicial.lower() == 'hoje':
        data_inicial = datetime.today().strftime('%Y-%m-%d')
    elif data_inicial.lower() == 'ontem':
        data_inicial = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    if data_atual.lower() == 'hoje':
        data_atual = datetime.today().strftime('%Y-%m-%d')

login_payload = create_login_payload(data_inicial, data_atual)

def login(url_login, auth, login_payload):
    try:
        response = requests.post(url_login, auth=auth, json=login_payload)
        response.raise_for_status()
    except (HTTPError, ConnectionError) as exc:
        print(exc)
        logging.error(
            f'Erro ao tentar logar na URL {url_login}. Detalhes do erro: {exc}')
        raise
    return response


response = login(url_login, auth, login_payload)
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



def download_arquivo(url):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Erro ao baixar o arquivo: {response.status_code}")
        return None
    arquivo_zip = io.BytesIO(response.content)
    return arquivo_zip


def extrair_arquivo(arquivo_zip, diretório):
    with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
        zip_ref.extractall(diretório)
    print("Arquivo baixado e extraído com sucesso!")


# Array para armazenar o csv concatenado
lista_dfs = []
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
    'Remessa': str
}


lista_dfs = [pd.read_csv(os.path.join(DATA_EXTRACTION_DIR, arquivo.name), dtype=tipos_colunas, low_memory=False)
             for arquivo in arquivos if arquivo.name != 'dados.csv']

# Concatenar todos os DataFrames na lista
df_concatenado = pd.concat(lista_dfs, ignore_index=True)


# Excluindo linhas duplicadas
df_concatenado = df_concatenado.drop_duplicates()
# df_concatenado = df_concatenado.sort_values(by=['Emissão'], ascending=False)
print("Arquivos CSV concatenados com sucesso!")

# Alterando o type de algumas colunas
colunas = ['Pedido', 'CNPJ Embarcador', 'CNPJ Cliente', 'CNPJ Transp.']

for coluna in colunas:
    df_concatenado[coluna] = df_concatenado[coluna].astype(pd.Int64Dtype())


# Excluindo elementos duplicados e mantendo apenas ultimo registro

df_concatenado = (df_concatenado.sort_index()
                  .drop_duplicates(
    subset=['Documento', 'Chave'], keep='last')
    .sort_values(by=['Emissão'], ascending=False))

# Salvar o arquivo CSV concatenado


def save_output(df_concatenado):
    df_concatenado.to_csv(CSV_OUTPUT_FILE, index=False, sep=';')
    df_concatenado.to_csv(CSV_OUTPUT_TMP_FILE, index=False, sep=';')
    logging.info(f'Arquivo {CSV_OUTPUT_FILE} salvo com sucesso')


if __name__ == '__main__':
    create_login_payload(data_inicial, data_atual)
    save_output(df_concatenado)
