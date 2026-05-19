import psutil
from datetime import datetime
import time
import csv
import os

def capturar_processos(): 

    processos = []

    for proc in psutil.process_iter([
        'pid',
        'name',
        'username',
        'status'
    ]):

        try:
            cpu = proc.cpu_percent(interval=0.1)
            ram_percent = proc.memory_percent()
            ram_mb = proc.memory_info().rss / 1024 / 1024 # transforma bytes para KB e depois para MB
            tempo_execucao = (
                datetime.now() -
                datetime.fromtimestamp(proc.create_time())
            )

            inicio = time.perf_counter()
            proc.memory_percent()
            fim = time.perf_counter()
            latencia = (fim - inicio) * 1000

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            processo = {
                "timestamp": timestamp,
                "pid": proc.info['pid'],
                "nome": proc.info['name'],
                "usuario": proc.info['username'],
                "cpu": round(cpu, 2),
                "ram_percent": round(ram_percent, 2),
                "ram_mb": round(ram_mb, 2),
                "status": proc.info['status'],
                "tempo_execucao": str(tempo_execucao),
                "latencia_ms": round(latencia, 2)
            }

            processos.append(processo)
            print(processo)

        except Exception as e:
            print(e)
    
    return processos


def salvar_csv(processos):

    arquivo_existe = os.path.isfile("raw_processos.csv")
    with open("raw_processos.csv", "a", newline="", encoding="utf-8") as arquivo:

        colunas = processos[0].keys()

        writer = csv.DictWriter(
            arquivo,
            fieldnames=colunas
        )

        if not arquivo_existe:
            writer.writeheader()

        writer.writerows(processos)

    print("\nCSV gerado com sucesso!")


processos = capturar_processos()
salvar_csv(processos)

