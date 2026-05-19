import csv
import os
import pandas
from io import StringIO

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
def processos_tratados():
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
            
        dfProcessos = pandas.read_csv(StringIO(dados_tratados), on_bad_lines='skip')
        return dfProcessos


def top5cpu(processos_tratados):
      top5 = sorted(processos_tratados, key=lambda x: x['cpu'], reverse=True)[:5]

      return top5

def top5ram(processos_tratados):
      top5 = sorted(processos_tratados, key=lambda x: x['ram_percent'], reverse=True)[:5]

      return top5

def processos_criticos(processos_tratados):
    totalCriticos = 0;
    for processo in processos_tratados:
        if(processo.criticidade == "Crítico"):
            totalCriticos + 1
            
    return totalCriticos

def maior_latencia():
     
    maior_valor = None

    with open('processos_tratados.csv', mode='r', encoding='utf-8') as arquivo:
            leitor = csv.DictReader(arquivo)
            
            for linha in leitor:
                valor_atual = float(linha['latencia_ms'])
                
                if maior_valor is None or valor_atual > maior_valor:
                    maior_valor = valor_atual

            print(f"O maior valor é: {maior_valor}")

            maior_latencia = {
                    "nome": linha['nome'],
                    "latencia": valor_atual,
                    "pid": ['pid']
                }
            
    print(f"O maior valor é: {maior_latencia['latencia']} ms")
    print(f"Processo: {maior_latencia['nome']}")
    print(f"PID: {maior_latencia['pid']}")

    return maior_latencia

print("ETL finalizada!")

def limite(processos_tratados):
    total_processos = len(processos_tratados)
    limite_30 = total_processos * 0.30
    return limite_30
   

def contar_status(processos_tratados):

    status_count = {
        "running": 0,
        "sleeping": 0,
        "stopped": 0
    }

    for processo in processos_tratados:

        status = processo["status"].lower()

        if status in status_count:
            status_count[status] += 1

    return status_count