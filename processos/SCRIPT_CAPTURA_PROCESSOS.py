import psutil
from datetime import datetime
import time

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
            ram_mb = proc.memory_info().rss / 1024 / 1024
            tempo_execucao = (
                datetime.now() -
                datetime.fromtimestamp(proc.create_time())
            )

            inicio = time.perf_counter()
            proc.memory_percent()
            fim = time.perf_counter()
            latencia = (fim - inicio) * 1000

            processo = {
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

