import os
import datetime

# Insira o caminho da pasta onde estão os arquivos
pasta = 'C:\ComproveiSAC\extraidos'
hoje = datetime.datetime.now().date()  # Data de hoje
# Data limite para manter os arquivos
cutoff = hoje - datetime.timedelta(days=0)


for filename in os.listdir(pasta):
    file_path = os.path.join(pasta, filename)

    # Verifica se é um arquivo e se sua data de modificação é anterior ao limite
    if os.path.isfile(file_path) and datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).date() < cutoff:
        os.remove(file_path)
        print(f"Arquivo '{filename}' removido.")
