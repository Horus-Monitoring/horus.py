import boto3
import csv
import json
import mysql.connector
from datetime import datetime
from io import StringIO
import pandas

AWS_CONFIG = {
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "aws_session_token": "",
    "region_name": "us-east-1",
    "bucket_name": "horus-monitoring"
}

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": ""
}

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_CONFIG["aws_access_key_id"],
    aws_secret_access_key=AWS_CONFIG["aws_secret_access_key"],
    aws_session_token=AWS_CONFIG["aws_session_token"],
    region_name=AWS_CONFIG["region_name"]
)

def verificar_csv():
    response = s3.list_objects_v2(
        Bucket=AWS_CONFIG["bucket_name"],
        Prefix="raw/"
    )
    return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]

def ler_csv_s3(key):
    obj = s3.get_object(Bucket=AWS_CONFIG["bucket_name"],
                         Key=key)
    
    conteudo = obj['Body'].read().decode('utf-8')

    df = pandas.read_csv(StringIO(conteudo))
    print(df)
    print(df.total_aeronaves)
    return df

def limpar_dados(df):
    colunas_numericas = [
        "bytes_recv",
        "bytes_sent",
        "pack_recv",
        "pack_sent",
        "packet_loss_internet",
        "internet",
        "latency_min_ms",
        "latency_avg_ms",
        "latency_max_ms",
        "lat_adsb_rastreamento",
        "lat_rastreamento_correlacao",
        "lat_correlacao_rotas",
        "lat_rotas_api",
        "lat_api_bd",
        "lat_bd_sync",
        "rastreamento_mbps",
        "rotas_mbps",
        "correlacao_mbps",
        "api_gateway_mbps",
        "bd_mbps",
        "sync_service_mbps",
        "rastreamento_loss",
        "correlacao_loss",
        "rotas_loss",
        "api_loss",
        "bd_loss",
        "sync_loss",
        "total_aeronaves",
        "avg_adsb_update_seconds"
    ]

    for coluna in colunas_numericas:
       df[coluna] = pandas.to_numeric(df[coluna], errors="coerce")

    df["timestamp"] = pandas.to_datetime(df["timestamp"])
    df["opensky_timestamp"] = pandas.to_datetime(df["opensky_timestamp"])
    df = df.fillna(0)

    return df

def salvar_s3(conteudo, key):
    s3.put_object(
        Bucket=AWS_CONFIG["bucket_name"],
        Key=key,
        Body=conteudo
    )

def atualizar_status_servidor(servidor_id, novo_status):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT status_servidor FROM servidor WHERE id_servidor = %s
    """, (servidor_id,))
    
    atual = cursor.fetchone()

    if atual and atual[0] != novo_status:
        cursor.execute("""
            UPDATE servidor
            SET status_servidor = %s,
                data_status = CURRENT_TIMESTAMP
            WHERE id_servidor = %s
        """, (novo_status, servidor_id))

        conn.commit()
        print(f"Status atualizado: {atual[0]} -> {novo_status}")
    else:
        print("Status não mudou")

    cursor.close()
    conn.close()

def determinar_status_servidor(severidades):
    prioridade = {
        "crítico": 5,
        "alta": 4,
        "média": 3,
        "baixa": 2,
        "normal": 1
    }

    if not severidades:
        return "Online"

    pior = max(severidades, key=lambda s: prioridade.get(s, 0))

    if pior == "crítico":
        return "Crítico"
    elif pior == "alta":
        return "Crítico"
    elif pior == "média":
        return "Atenção"
    elif pior == "baixa":
        return "Online"
    else:
        return "Online"

def classificar_latencia(valor):
    valor = float(valor)

    if valor > 250:
        return "critico"
    elif valor > 200:
        return "alto"
    elif valor > 150:
        return "medio"
    elif valor > 100:
        return "baixo"
    else:
        return "normal"
    
def severidade_servidor_latencia(linha):
    status = [
        linha["status_latency_avg"],
        linha["status_adsb"],
        linha["status_api_bd"],
        linha["status_bd_sync"]
    ]

    prioridade = {
        "critico": 4,
        "alto": 3,
        "medio": 2,
        "baixo": 1,
        "normal": 0
    }

    pior_status = max(status, key=lambda x: prioridade[x])

    return pior_status

def classificar_pacotes(valor):
    valor = float(valor)

    if valor > 20:
        return "critico"
    elif valor > 15:
        return "alto"
    elif valor > 10:
        return "medio"
    elif valor > 5:
        return "baixo"
    else:
        return "normal"

def severidade_servidor_pacotes(linha):
    status = [
        linha["packet_loss"],
        linha["rastreamento_loss"],
        linha["correlacao_loss"],
        linha["rotas_loss"],
        linha["api_loss"],
        linha["bd_loss"],
        linha["sync_loss"]
    ]

    prioridade = {
        "critico": 4,
        "alto": 3,
        "medio": 2,
        "baixo": 1,
        "normal": 0
    }

    pior_status = max(status, key=lambda x: prioridade[x])

    return pior_status

def kpi_perda_media(df):
    colunas = [
        "packet_loss_internet",
        "rastreamento_loss",
        "correlacao_loss",
        "rotas_loss",
        "api_loss",
        "bd_loss",
        "sync_loss"
    ]

    return df[colunas].mean().mean()

def kpi_latencia_media(df):
    colunas = [
        "latency_avg_ms",
        "lat_adsb_rastreamento",
        "lat_rastreamento_correlacao",
        "lat_correlacao_rotas",
        "lat_rotas_api",
        "lat_api_bd",
        "lat_bd_sync"
    ]

    return df[colunas].mean().mean()

def kpi_adsb_update(df):
    media = df["avg_adsb_update_seconds"].mean()

    if media <= 2:
        return 100   # excelente
    elif media <= 5:
        return 85    # baixo
    elif media <= 10:
        return 70    # medio
    elif media <= 30:
        return 40    # alto
    else:
        return 10    # crítico
    
ler_csv_s3("raw/empresa_1/c0:35:32:c7:0b:59/network_raw.csv")
