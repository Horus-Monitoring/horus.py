import csv
import os

CPU_CRITICA = 80
CPU_ALERTA = 50

RAM_CRITICA_PERCENT = 20
RAM_ALERTA_PERCENT = 10

# definir valores 
LATENCIA_CRITICA = 100
LATENCIA_ALERTA = 50

def processos_criticidade(cpu, ram_percent, latencia):

    # cpu (%), ram (%) e latencia (ms)
    if cpu >= CPU_CRITICA or ram_percent > RAM_CRITICA_PERCENT or latencia > LATENCIA_CRITICA:
        return "Crítico"

    elif cpu > CPU_ALERTA or ram_percent > RAM_ALERTA_PERCENT or latencia > LATENCIA_ALERTA:
        return "Alerta"

    return "Estável"

dados_tratados = []

# modificar para ler do S3
with open("raw_processos.csv", "r", encoding="utf-8") as arquivo:

    leitor = csv.DictReader(arquivo)

    for linha in leitor:

        # transformando string para tipo numérico
        cpu = float(linha["cpu"])

        ram_percent = float(
            linha["ram_percent"]
        )

        latencia = float(
            linha["latencia_ms"]
        )

        criticidade = processos_criticidade(
            cpu,
            ram_percent,
            latencia
        )

        processo_tratado = {
            "timestamp": linha["timestamp"],
            "pid": linha["pid"],
            "nome": linha["nome"],
            "usuario": linha["usuario"],
            "cpu": cpu,
            "ram_percent": ram_percent,
            "ram_mb": linha["ram_mb"],
            "status": linha["status"],
            "tempo_execucao": linha["tempo_execucao"],
            "latencia_ms": latencia,
            "criticidade": criticidade
        }

        dados_tratados.append(
            processo_tratado
        )

arquivo_existe = os.path.isfile("processos_tratados.csv")
with open("processos_tratados.csv", "a", newline="", encoding="utf-8") as arquivo_saida:

    # cabeçalho do csv
            colunas = dados_tratados[0].keys()

            writer = csv.DictWriter(
                arquivo_saida,
                fieldnames=colunas
            )

            if not arquivo_existe:
                writer.writeheader()

            writer.writerows(dados_tratados)


print("ETL finalizada!")