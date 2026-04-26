import boto3
import csv
import json
import mysql.connector
from datetime import datetime
from io import StringIO

AWS_CONFIG = {
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "aws_session_token": "",
    "region_name": "",
    "bucket_name": ""
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

def listar_raw():
    response = s3.list_objects_v2(
        Bucket=AWS_CONFIG["bucket_name"],
        Prefix="raw/"
    )
    return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]

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
        SELECT c.nome, sc.limite
        FROM servidor_componente sc
        JOIN componente c ON sc.fk_componente = c.id_componente
        WHERE sc.fk_servidor = %s
    """, (servidor_id,))

    limites = {row[0]: float(row[1]) for row in cursor.fetchall()}

    cursor.close()
    conn.close()

    return limites

def atualizar_status_servidor(servidor_id, status):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE servidor
        SET status_inicial = %s
        WHERE id_servidor = %s
    """, (status, servidor_id))

    conn.commit()
    cursor.close()
    conn.close()

def classificar_metrica(valor, limite):
    if valor == 0:
        return "offline", "crítica"
    if valor >= limite:
        return "crítico", "crítica"
    elif valor >= 0.9 * limite:
        return "crítico", "alta"
    elif valor >= 0.8 * limite:
        return "atenção", "média"
    elif valor >= 0.7 * limite:
        return "online", "baixa"
    else:
        return "normal", "normal"

def determinar_status_servidor(severidades):
    prioridade = {
        "crítica": 5,
        "alta": 4,
        "média": 3,
        "baixa": 2,
        "normal": 1
    }

    if not severidades:
        return "Operacional"

    pior = max(severidades, key=lambda s: prioridade.get(s, 0))

    if pior in ["crítica", "alta"]:
        return "Crítico"
    elif pior == "média":
        return "Atenção"
    else:
        return "Operacional"

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

if __name__ == "__main__":
    processar()