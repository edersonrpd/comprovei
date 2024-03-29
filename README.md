# ComproveiSac

Este código é um script em Python para baixar, extrair, concatenar e salvar arquivos CSV de um conjunto de dados exportado de uma plataforma chamada ComproveiSAC. Ele também realiza o processamento dos dados baixados antes de salvá-los em um arquivo de saída.

## Índice

1. [Bibliotecas Utilizadas](#bibliotecas-utilizadas)
2. [Resumo das Etapas](#resumo-das-etapas)
3. [Documentação](#documentação)
4. [Instalação](#instalação)

## Bibliotecas Utilizadas

O projeto utiliza as seguintes bibliotecas:

- requests: para fazer requisições HTTP.
- pandas: para manipular e analisar os dados.
- json: para lidar com o formato JSON.
- zipfile e io: para baixar e extrair arquivos zip.
- re: para lidar com expressões regulares.
- os: para lidar com funções de sistema operacional.
- xml.etree.ElementTree: para lidar com arquivos XML.
- logging: para registro de eventos.
- datetime: para lidar com datas e horários.
- pathlib: para lidar com caminhos de arquivos.
- python-dotenv: para salvar configurações em um arquivo local.

## Resumo das Etapas

O código realiza as seguintes etapas:

1. Configurar o registro de eventos usando o módulo logging.
2. Obter a data inicial e a data atual como strings no formato "YYYY-MM-DD".
3. Definir a URL de login e as credenciais de autenticação.
4. Criar o payload para a requisição POST e enviar a requisição.
5. Verificar se a resposta foi bem-sucedida e extrair a URL do arquivo zip.
6. Baixar o arquivo zip e extrair o conteúdo em um diretório especificado.
7. Ler e armazenar cada arquivo CSV em um DataFrame.
8. Concatenar todos os DataFrames e excluir linhas duplicadas.
9. Alterar os tipos de dados de algumas colunas e remover elementos duplicados, mantendo apenas o último registro.
10. Salvar o DataFrame final em um arquivo CSV e um arquivo Excel.

## Documentação

A documentação das bibliotecas utilizadas pode ser encontrada nos seguintes links:

- [Python](https://www.python.org/doc/)
- [Requests](https://docs.python-requests.org/en/latest/)
- [Pandas](https://pandas.pydata.org/pandas-docs/stable/index.html)
- [JSON](https://docs.python.org/3/library/json.html)
- [ZipFile](https://docs.python.org/3/library/zipfile.html)
- [IO](https://docs.python.org/3/library/io.html)
- [RE](https://docs.python.org/3/library/re.html)
- [OS](https://docs.python.org/3/library/os.html)
- [XML](https://docs.python.org/3/library/xml.etree.elementtree.html)
- [Logging](https://docs.python.org/3/library/logging.html)
- [Datetime](https://docs.python.org/3/library/datetime.html)
- [Pathlib](https://docs.python.org/3/library/pathlib.html)
- [Dotenv](https://pypi.org/project/python-dotenv/)


## Instalação de pacotes necessários
py -m pip install -r requirements.txt
