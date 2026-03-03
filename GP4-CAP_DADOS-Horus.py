import psutil
import csv
from datetime import datetime
import os
import time

arquivo_csv = "captura_dados-brutos_maquina.csv"
arquivo_ja_existe = os.path.isfile(arquivo_csv) and os.path.getsize(arquivo_csv) > 0

with open(arquivo_csv, 'a', newline='') as csvfile:
    while(True): 
        colunas = ['CPU','RAM','DISCO','DATA_HORA']
        CSV_DIC_WRITER = csv.DictWriter(csvfile, fieldnames=colunas)
        if not arquivo_ja_existe:
            CSV_DIC_WRITER.writeheader()

        cpu_usage = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory() 
        disk = psutil.disk_usage("/")
        tempo_agora = datetime.now()

        print(f"Escrevendo dados: \n CPU: {cpu_usage}\n RAM_TOTAL: {ram.total} : RAM_USADA: {ram.used} : RAM_PORCENTAGEM: {ram.percent}\n DISCO_TOTAL: {disk.total} : DISCO_USADA: {disk.used} : DISCO_PORCENTAGEM: {disk.percent}\n DATA_HORA: {tempo_agora}")
        print()
        dados_dict =  {'CPU': cpu_usage, 'RAM': [ram.total, ram.used, ram.percent], 'DISCO':[disk.total, disk.used, disk.percent], 'DATA_HORA': tempo_agora}

        CSV_DIC_WRITER.writerow(dados_dict)
        csvfile.flush()
        time.sleep(2)
    