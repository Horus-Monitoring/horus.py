import csv
import os
import pandas 
from io import StringIO
import boto3
import json
import time

AWS_CONFIG = {
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "aws_session_token": "",
    "region_name": "us-east-1",
    "bucket_name": "horus-monitoring"
}


s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_CONFIG["aws_access_key_id"],
    aws_secret_access_key=AWS_CONFIG["aws_secret_access_key"],
    aws_session_token=AWS_CONFIG["aws_session_token"],
    region_name=AWS_CONFIG["region_name"]
)

CPU_CRITICA = 80
CPU_ALERTA = 50

RAM_CRITICA_PERCENT = 20
RAM_ALERTA_PERCENT = 10

LATENCIA_CRITICA = 100
LATENCIA_ALERTA = 50

def verificar_csv():

    response = s3.list_objects_v2(
        Bucket=AWS_CONFIG["bucket_name"],
        Prefix="raw/"
    )

    return [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]


def ler_csv_s3(key):

    obj = s3.get_object(
        Bucket=AWS_CONFIG["bucket_name"],
        Key=key
    )

    conteudo = obj['Body'].read().decode('utf-8')

    df = pandas.read_csv(
        StringIO(conteudo),
        on_bad_lines='skip'
    )

    return df


def salvar_s3(conteudo, key):

    s3.put_object(
        Bucket=AWS_CONFIG["bucket_name"],
        Key=key,
        Body=conteudo
    )

    print(f"Arquivo enviado: {key}")


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

    with open("process_raw.csv", "r", encoding="utf-8") as arquivo:

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

    arquivo_existe = os.path.isfile("process_raw.csv")

    with open(
        "process_raw.csv",
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

    dfTop5 = dfProcessos.sort_values(
        'cpu',
        ascending=False
    )

    print("teste cpu!")

    cpu5 = {}

    for i in range (5):
        cpu5[f"nome_cpu{i+1}"] = dfTop5["nome"].iloc[i]
        cpu5[f"cpu_{i+1}"] = float(dfTop5["cpu"].iloc[i])

    print(cpu5)
    return cpu5


def top5ram(dfProcessos):

    dfTop5 = dfProcessos.sort_values(
        'ram_percent',
        ascending=False
    )

    print("teste ram!")

    ram5 = {}

    for i in range (5):
        ram5[f"nome_ram{i+1}"] = dfTop5["nome"].iloc[i]
        ram5[f"ram_{i+1}"] = float(dfTop5["ram_percent"].iloc[i])

    print(ram5)
    return ram5


def processos_criticos(df_processos):
    total_criticos = (df_processos['criticidade'] == 'Crítico').sum()
    return {"totalCriticos": int(total_criticos)}


def maior_latencia():

    maior_valor = None

    with open(
        'process_raw.csv',
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
                nome = linha["nome"]
                pid = linha["pid"]

    maior_latencia = {
        "nome": nome,
        "latencia_ms": maior_valor,
        "pid": pid
    }

    print(
        f"O maior valor é: {maior_latencia['latencia_ms']} ms"
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

    return {"limite": limite_30}


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
        "total": 0
    }

    while i < len(processos_tratados):

        processos = processos_tratados.iloc[i]

        if processos["criticidade"] == "Crítico":

                print("Entrei no for.")
                if processos["cpu"] >= CPU_CRITICA:
                    criticos_count["cpu"] += 1
                    criticos_count["total"] += 1

                if (
                    processos["ram_percent"]
                    > RAM_CRITICA_PERCENT
                ):
                    criticos_count["ram"] += 1
                    criticos_count["total"] += 1

                if (
                    processos["latencia_ms"]
                    > LATENCIA_CRITICA
                ):
                    criticos_count["latencia"] += 1
                    criticos_count["total"] += 1
                
                print(criticos_count)
            
        i += 1

    return criticos_count

def criticos_24h_em_blocos_4h(dfProcessos):

    df = dfProcessos.copy()

    # garantir datetime
    df["timestamp"] = pandas.to_datetime(df["timestamp"])

    agora = pandas.Timestamp.now()

    # filtrar últimas 24h
    df_24h = df[
        df["timestamp"] >= agora - pandas.Timedelta(hours=24)
    ]

    # só críticos
    df_24h = df_24h[df_24h["criticidade"] == "Crítico"]

    # cria "horas desde agora"
    df_24h["horas_atras"] = (agora - df_24h["timestamp"]).dt.total_seconds() / 3600

    # função para definir bucket de 4h
    def bucket(horas):
        if horas <= 4:
            return "0-4h"
        elif horas <= 8:
            return "4-8h"
        elif horas <= 12:
            return "8-12h"
        elif horas <= 16:
            return "12-16h"
        elif horas <= 20:
            return "16-20h"
        else:
            return "20-24h"

    df_24h["bloco"] = df_24h["horas_atras"].apply(bucket)

    # contar por bloco
    resultado = df_24h["bloco"].value_counts().to_dict()

    # garantir todas as chaves mesmo se zeradas
    blocos = ["0-4h", "4-8h", "8-12h", "12-16h", "16-20h", "20-24h"]

    final = {b: resultado.get(b, 0) for b in blocos}

    return final


# dados_blocos = criticos_24h_em_blocos_4h(dfProcessos)


def salvar_json_processos(df, key):

    processos_json = df.to_dict(
        orient="records"
    )

    salvar_s3(
        json.dumps(
            processos_json,
            indent=4,
            ensure_ascii=False
        ),
        key
    )


def salvar_json_dashboard(dashboard, key):

    salvar_s3(
        json.dumps(
            dashboard,
            indent=4,
            ensure_ascii=False
        ),
        key
    )


def main():

    arquivos = verificar_csv()

    if len(arquivos) == 0:
        print("Nenhum CSV encontrado.")
        return

    for arquivo in arquivos:

        print(f"Processando arquivo: {arquivo}")

        key = "raw/empresa_1/c0:35:32:c7:0b:59/process_raw.csv"

        dfRaw = ler_csv_s3(key)

        dfRaw.to_csv(
            "process_raw.csv",
            index=False
        )

        dfProcessos = processos_tratados()

        print(dfProcessos)

        kpis = {}

        kpis.update(
            top5cpu(dfProcessos)
        )

        kpis.update(
            top5ram(dfProcessos)
        )

        kpis.update(
            processos_criticos(dfProcessos)
        )

        kpis.update(
            maior_latencia()
        )

        kpis.update(
            limite(dfProcessos)
        )

        kpis.update(
            contar_status(dfProcessos)
        )

        kpis.update(
            contar_criticos(dfProcessos)
        )
        print("---------------------------------")
        #print(criticos24h(dfProcessos))

        dados_blocos = criticos_24h_em_blocos_4h(dfProcessos)


        nome_base = (
            arquivo
            .split("/")[-1]
            .replace(".csv", "")
        )

        key_processos = (
            f"client/processos/{nome_base}.json"
        )

        key_kpis = (
            f"client/kpis/{nome_base}_kpis.json"
        )

        key_blocos = (f"client/kpis/{nome_base}_criticos_4h.json")

        salvar_json_processos(
            dfProcessos,
            key_processos
        )

        salvar_json_dashboard(
            kpis,
            key_kpis
        )

        salvar_json_dashboard(
            dados_blocos,
            key_blocos
        )

        print("ETL finalizada.")


if __name__ == "__main__":
    main()