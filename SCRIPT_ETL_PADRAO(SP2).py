import boto3
import csv
import json
import mysql.connector
from datetime import datetime
from io import StringIO
import pandas
import os

AWS_CONFIG = {
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "aws_session_token": "",
    "region_name": "us-east-1",
    "bucket_name": "horus-monitoring"
}

DB_CONFIG = {
    "host": "localhost",
    "user": "horus",
    "password": "Horus123456",
    "database": "horus_db3"
}

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_CONFIG["aws_access_key_id"],
    aws_secret_access_key=AWS_CONFIG["aws_secret_access_key"],
    aws_session_token=AWS_CONFIG["aws_session_token"],
    region_name=AWS_CONFIG["region_name"]
)

def listar_raw():
    response = s3.list_objects_v2(
        Bucket=AWS_CONFIG["bucket_name"],
        Prefix="raw/"
    )
    return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]

def ler_raw():
    caminho_arquivo = os.path.join("raw", "raw.csv")
    print(caminho_arquivo)

    return pandas.read_csv(caminho_arquivo)

def ler_csv_s3(key):
    obj = s3.get_object(Bucket=AWS_CONFIG["bucket_name"], Key=key)
    return obj['Body'].read().decode('utf-8')

def salvar_s3(conteudo, key):
    s3.put_object(
        Bucket=AWS_CONFIG["bucket_name"],
        Key=key,
        Body=conteudo
    )

def obter_limites(servidor_id):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT c.tipo, sc.limite
        FROM servidor_componente sc
        JOIN componente c ON sc.fk_componente = c.id_componente
        WHERE sc.fk_servidor = %s
    """, (servidor_id,))

    limites = {row[0]: float(row[1]) for row in cursor.fetchall()}

    cursor.close()
    conn.close()

    return limites

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

def classificar_metrica(valor, limite):

    if limite == 0:
        return "Online", "normal"
    if valor == 0 and limite > 0:
        return "Offline", "crítico"
    if valor >= limite:
        return "Crítico", "crítico"
    elif valor >= 0.9 * limite:
        return "Crítico", "alta"
    elif valor >= 0.8 * limite:
        return "Atenção", "média"
    elif valor >= 0.7 * limite:
        return "Online", "baixa"
    else:
        return "Online", "normal"

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
    
def processar():
    arquivos = listar_raw()

    for key in arquivos:
        csv_content = ler_csv_s3(key)
        reader = csv.DictReader(StringIO(csv_content))

        trusted_output = StringIO()
        writer = None

        client_json = []
        alertas = []

        alertas_por_dia = {}
        alertas_por_servidor = {}

        severidades_detectadas = []

        for row in reader:
            servidor_id = int(row["servidor_id"])
            empresa_id = int(row["id_empresa"])

            limites = obter_limites(servidor_id)

            cpu = float(row["cpu"])
            ram = float(row["ram"])
            disco = float(row["disco"])

            row_trusted = row.copy()
            row_trusted["cpu"] = f"{cpu}%"
            row_trusted["ram"] = f"{ram}%"
            row_trusted["disco"] = f"{disco}%"

            if writer is None:
                writer = csv.DictWriter(trusted_output, fieldnames=row_trusted.keys())
                writer.writeheader()

            writer.writerow(row_trusted)

            client_json.append({
                "data_hora": row["data_hora"],
                "empresa_id": empresa_id,
                "servidor_id": servidor_id,
                "metricas": {
                    "cpu": cpu,
                    "ram": ram,
                    "disco": disco,
                    "rede_rx": row.get("rede_rx"),
                    "rede_tx": row.get("rede_tx"),
                    "processos": row.get("processos")
                }
            })

            for componente, valor in [
                ("CPU", cpu),
                ("RAM", ram),
                ("DISCO", disco)
            ]:
                if componente in limites:
                    limite = limites[componente]
                    status, severidade = classificar_metrica(valor, limite)

                    severidades_detectadas.append(severidade)

                    if severidade != "normal":
                        alertas.append({
                            "data_hora": row["data_hora"],
                            "empresa": empresa_id,
                            "servidor": servidor_id,
                            "componente": componente,
                            "limite": limite,
                            "valor": valor,
                            "status": status,
                            "severidade": severidade
                        })

                        data = row["data_hora"].split(" ")[0]
                        alertas_por_dia[data] = alertas_por_dia.get(data, 0) + 1
                        alertas_por_servidor[servidor_id] = alertas_por_servidor.get(servidor_id, 0) + 1

        base_path = key.replace("raw/", "")

        salvar_s3(trusted_output.getvalue(), f"trusted/{base_path}")

        salvar_s3(
            json.dumps(client_json, indent=2, ensure_ascii=False),
            f"client/{base_path.replace('.csv', '.json')}"
        )

        salvar_s3(
            json.dumps(alertas, indent=2, ensure_ascii=False),
            f"client/alertas/{base_path.replace('.csv', '_alertas.json')}"
        )

        dia_critico = max(alertas_por_dia, key=alertas_por_dia.get) if alertas_por_dia else None
        servidor_critico = max(alertas_por_servidor, key=alertas_por_servidor.get) if alertas_por_servidor else None

        resumo = {
            "dia_mais_critico": dia_critico,
            "servidor_mais_critico": servidor_critico,
            "total_alertas": len(alertas)
        }

        salvar_s3(
            json.dumps(resumo, indent=2, ensure_ascii=False),
            f"client/resumo/{base_path.replace('.csv', '_resumo.json')}"
        )

        status_final = determinar_status_servidor(severidades_detectadas)
        atualizar_status_servidor(servidor_id, status_final)

#if __name__ == "__main__":
#    processar()

import pandas as pd

def calcular_penalidade(persistencia, peso_maximo):

    if persistencia < 0.20:
        percentual_penalidade = 0

    elif persistencia < 0.40:
        percentual_penalidade = 0.25

    elif persistencia < 0.60:
        percentual_penalidade = 0.50

    elif persistencia < 0.80:
        percentual_penalidade = 0.75

    else:
        percentual_penalidade = 1

    return peso_maximo * percentual_penalidade

def classificar_status(score):

    if score >= 90:
        return "Saudável"

    elif score >= 80:
        return "Atenção"

    return "Crítico"


def calcular_persistencia_alertas(df, hostname, coluna_tempo="timestamp"):

    obter_limites_bd = obter_limites(1)

    limite_cpu = obter_limites_bd.get("CPU")
    limite_ram = obter_limites_bd.get("RAM")
    limite_disco = obter_limites_bd.get("DISCO")
    limite_adsb = 10

    df = df.copy()

    df[coluna_tempo] = pd.to_datetime(df[coluna_tempo])

    if hostname is not None:
        df = df[df["hostname"] == hostname]
        print('estou no hostname')

    agora = df[coluna_tempo].max()
    print(agora)

    periodos = {
        "1h": pd.Timedelta(hours=1),
        "12h": pd.Timedelta(hours=12),
        "24h": pd.Timedelta(hours=24),
        "7d": pd.Timedelta(days=7)
    }

    resultados = {}

    for nome_periodo, delta in periodos.items():

        inicio_periodo = agora - delta

        df_periodo = df[
            df[coluna_tempo] >= inicio_periodo
        ]

        total_coletas = len(df_periodo)

        if total_coletas == 0:
            resultados[nome_periodo] = {
                "cpu": 0,
                "ram": 0,
                "disco": 0,
                "adsb": 0
            }


        alertas_cpu = (
            df_periodo["cpu"] > limite_cpu
        ).sum()

        alertas_ram = (
            df_periodo["ram"] > limite_ram
        ).sum()

        alertas_disco = (
            df_periodo["disco"] > limite_disco
        ).sum()

        alertas_adsb = (
            df_periodo["avg_adsb_update_seconds"] > limite_adsb
        ).sum()

        if(total_coletas ==  0): continue

        resultados[nome_periodo] = {
            "total_coletas": total_coletas,

            "cpu": {
                "alertas": int(alertas_cpu),
                "persistencia": float(round(
                    alertas_cpu / total_coletas,
                    2
                ))
            },

            "ram": {
                "alertas": int(alertas_ram),
                "persistencia": float(round(
                    alertas_ram / total_coletas,
                    2
                ))
            },

            "disco": {
                "alertas": int(alertas_disco),
                "persistencia": float(round(
                    alertas_disco / total_coletas,
                    2
                ))
            },

            "adsb": {
                "alertas": int(alertas_adsb),
                "persistencia": float(round(
                    alertas_adsb / total_coletas,
                    2
                ))
            }
        }
    
    return resultados


def calcular_score_servidor(dados, hostname):

    persistencias = calcular_persistencia_alertas(dados, hostname)

    resultados_score = {}

    for periodo, dados in persistencias.items():

        persistencia_cpu = dados["cpu"]["persistencia"]
        persistencia_ram = dados["ram"]["persistencia"]
        persistencia_disco = dados["disco"]["persistencia"]
        persistencia_adsb = dados["adsb"]["persistencia"]

        penalidade_cpu = calcular_penalidade(persistencia_cpu, 30)

        penalidade_ram = calcular_penalidade(persistencia_ram, 30)

        penalidade_disco = calcular_penalidade(persistencia_disco, 15)

        penalidade_adsb = calcular_penalidade(persistencia_adsb, 25)

        score_final = (100 - penalidade_cpu - penalidade_ram - penalidade_disco - penalidade_adsb)

        score_final = max(0, min(100, score_final))

        resultados_score[periodo] = {

            "score": round(score_final, 2),

            "status": classificar_status(score_final),

            "penalidades": {

                "cpu": penalidade_cpu,
                "ram": penalidade_ram,
                "disco": penalidade_disco,
                "adsb": penalidade_adsb
            },

            "persistencias": {

                "cpu": persistencia_cpu,
                "ram": persistencia_ram,
                "disco": persistencia_disco,
                "adsb": persistencia_adsb
            }
        }

    return resultados_score