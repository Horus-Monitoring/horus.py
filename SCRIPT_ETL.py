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
    "region_name": "us-east-1",
    "bucket_name": "horus-monitoring-gustavo"
}

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Uhtred5236",
    "database": "horus_db"
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
            health_score = float(row["health_score"])
            status_cpu = row["status_cpu"]
            status_ram = row["status_ram"]
            status_disco = row["status_disco"]

            ip = row["ip"]
            hostname = row["hostname"]

            cpu = float(row["cpu"])
            ram = float(row["ram"])
            disco = float(row["disco"])

            saude_cpu = 100 - cpu
            saude_ram = 100 - ram
            saude_disco = 100 - disco

            health_score = (
            saude_cpu * 0.40 +
            saude_ram * 0.40 +
            saude_disco * 0.20
                )

            criticos = sum(
                1 for s in [saude_cpu, saude_ram, saude_disco]
                if s < 40
            )

            if criticos == 1:
                health_score -= 5
            elif criticos == 2:
                health_score -= 15
            elif criticos == 3:
                health_score -= 25

            health_score = round(max(0, min(100, health_score)), 2)
            
            if health_score >= 70:
                status_health = "estavel"
            elif health_score >= 40:
                status_health = "atencao"
            else:
                status_health = "critico"



            row_trusted = row.copy()
            row_trusted["cpu"] = f"{cpu}%"
            row_trusted["ram"] = f"{ram}%"
            row_trusted["disco"] = f"{disco}%"
            row_trusted["health_score"] = f"{health_score}%"
            row_trusted["status_health"] = f"{status_health}"

            if writer is None:
                writer = csv.DictWriter(trusted_output, fieldnames=row_trusted.keys())
                writer.writeheader()



                writer.writerow(row_trusted)

            client_json.append({
                "data_hora": row["data_hora"],
                "empresa_id": empresa_id,
                "servidor_id": servidor_id,
                "hostname": hostname,
                "ip": ip,

                "metricas": {
                    "cpu": cpu,
                    "ram": ram,
                    "disco": disco,
                    "health_score": health_score,
                    "status_health": status_health
                },
                "status_componentes":{
                    "cpu": status_cpu,
                    "ram": status_ram,
                    "disco": status_disco
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