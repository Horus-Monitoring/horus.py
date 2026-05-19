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
    if (
        cpu >= CPU_CRITICA
        or ram_percent > RAM_CRITICA_PERCENT
        or latencia > LATENCIA_CRITICA
    ):
        return "Crítico"

    elif (
        cpu > CPU_ALERTA
        or ram_percent > RAM_ALERTA_PERCENT
        or latencia > LATENCIA_ALERTA
    ):
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

    with open(
        "processos_tratados.csv",
        "a",
        newline="",
        encoding="utf-8"
    ) as arquivo_saida:

        # cabeçalho do csv
        colunas = dados_tratados[0].keys()

        writer = csv.DictWriter(
            arquivo_saida,
            fieldnames=colunas
        )

        if not arquivo_existe:
            writer.writeheader()

        writer.writerows(dados_tratados)

    dfProcessos = pandas.DataFrame(dados_tratados)

    print(dfProcessos)

    return dfProcessos


def top5cpu(dfProcessos):

    # arrumar cpu nucleos
    # top5 = sorted(processos_tratados, key=lambda x: x['cpu'], reverse=True)[:5]

    dfTop5 = dfProcessos.sort_values(
        'cpu',
        ascending=False
    )

    # print(dfTop5)
    print("teste cpu!")

    # print(dfTop5.nlargest(5, 'cpu'))
    print(dfTop5.head())

    return dfTop5


def top5ram(dfProcessos):

    dfTop5 = dfProcessos.sort_values(
        'ram_percent',
        ascending=False
    )

    # print(dfTop5)
    print("teste ram!")

    # print(dfTop5.nlargest(5, 'cpu'))
    print(dfTop5.head())

    return dfTop5


def processos_criticos(processos_tratados):

    totalCriticos = 0

    for processo in processos_tratados:

        if processo.criticidade == "Crítico":
            totalCriticos + 1

    return totalCriticos


def maior_latencia():

    maior_valor = None

    with open(
        'processos_tratados.csv',
        mode='r',
        encoding='utf-8'
    ) as arquivo:

        leitor = csv.DictReader(arquivo)

        for linha in leitor:

            valor_atual = float(
                linha['latencia_ms']
            )

            if (
                maior_valor is None
                or valor_atual > maior_valor
            ):
                maior_valor = valor_atual

    print(f"O maior valor é: {maior_valor}")

    maior_latencia = {
        "nome": linha['nome'],
        "latencia": valor_atual,
        "pid": linha['pid']
    }

    print(
        f"O maior valor é: {maior_latencia['latencia']} ms"
    )

    print(
        f"Processo: {maior_latencia['nome']}"
    )

    print(
        f"PID: {maior_latencia['pid']}"
    )

    return maior_latencia


def limite(processos_tratados):

    total_processos = len(processos_tratados)
    print("total processos: ")
    print(total_processos)

    limite_30 = total_processos * 0.30

    return limite_30


def contar_status(processos_tratados):

    i = 0

    status_count = {
        "running": 0,
        "sleeping": 0,
        "stopped": 0
    }

    while i < len(processos_tratados):

        # pega o status da linha atual
        status = processos_tratados.iloc[i]["status"].lower()

        # print(status)

        if status in status_count:
            status_count[status] += 1

        i += 1

    print(status_count)
    return status_count


def contar_criticos(processos_tratados):

    i = 0

    criticos_count = {
        "latencia": 0,
        "cpu": 0,
        "ram": 0,
    }

    while i < len(processos_tratados):

        processos = processos_tratados.iloc[i]

        if processos["criticidade"] == "Crítico":

                print("Entrei no for.")
                if processos["cpu"] >= CPU_CRITICA:
                    criticos_count["cpu"] += 1

                if (
                    processos["ram_percent"]
                    > RAM_CRITICA_PERCENT
                ):
                    criticos_count["ram"] += 1

                if (
                    processos["latencia_ms"]
                    > LATENCIA_CRITICA
                ):
                    criticos_count["latencia"] += 1
                
                print(criticos_count)
            
        i += 1

    return criticos_count


#def main():
dfProcessos = processos_tratados()

df5cpu = top5cpu(dfProcessos)

df5ram = top5ram(dfProcessos)

dfLatencia = maior_latencia()

limite = limite(dfProcessos)
print(limite)

dfStatus = contar_status(dfProcessos)

dfCriticos = contar_criticos(dfProcessos)