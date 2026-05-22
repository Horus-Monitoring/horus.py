import psutil
import time
import socket
import csv
import boto3
import mysql.connector
from datetime import datetime
from botocore.exceptions import ClientError

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

INTERVALO = 10


def conectar_s3():
    return boto3.client(
        's3',
        aws_access_key_id=AWS_CONFIG["aws_access_key_id"],
        aws_secret_access_key=AWS_CONFIG["aws_secret_access_key"],
        aws_session_token=AWS_CONFIG["aws_session_token"],
        region_name=AWS_CONFIG["region_name"]
    )


def gerar_chave_s3(empresa_id, hostname):
    return f"raw/empresa_{empresa_id}/{hostname}/metricas.csv"


def arquivo_existe_s3(s3, key):
    try:
        s3.head_object(Bucket=AWS_CONFIG["bucket_name"], Key=key)
        return True
    except ClientError:
        return False


def baixar_csv_s3(s3, key):
    s3.download_file(AWS_CONFIG["bucket_name"], key, "temp.csv")


def enviar_csv_s3(s3, key):
    s3.upload_file("temp.csv", AWS_CONFIG["bucket_name"], key)


def obter_servidor_info(hostname):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_servidor, fk_empresa
        FROM servidor
        WHERE hostname = %s
    """, (hostname,))

    result = cursor.fetchone()

    cursor.close()
    conn.close()

    if result:
        return {
            "servidor_id": result[0],
            "empresa_id": result[1]
        }

    return None


def obter_componentes_servidor(servidor_id):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT c.tipo
        FROM servidor_componente sc
        JOIN componente c ON sc.fk_componente = c.id_componente
        WHERE sc.fk_servidor = %s
    """, (servidor_id,))

    dados = [r[0] for r in cursor.fetchall()]

    cursor.close()
    conn.close()

    return dados


def gerar_status(valor):
    if valor >= 85:
        return "critico"
    elif valor >= 70:
        return "atencao"
    return "estavel"


def coletar_metricas(componentes):
    dados = {}

    hostname = socket.gethostname()

    try:
        ip = socket.gethostbyname(hostname)
    except:
        ip = "0.0.0.0"

    dados['ip'] = ip

    if 'CPU' in componentes:
        dados['cpu'] = psutil.cpu_percent(interval=1)

    if 'RAM' in componentes:
        dados['ram'] = psutil.virtual_memory().percent

    if 'DISCO' in componentes:
        dados['disco'] = psutil.disk_usage('/').percent


    cpu = dados.get('cpu', 0)
    ram = dados.get('ram', 0)
    disco = dados.get('disco', 0)


# o calculo do heath score é uma media de porcentagem de todos os componentes. de 100 - 70 é estavel, de 40 - 69 é alerta e de 0 a 39 é critico.
    health_score = 100 - ((cpu + ram + disco) / 3)

    dados['health_score'] = round(
        max(0, health_score),
        2
    )

    dados['status_cpu'] = gerar_status(cpu)
    dados['status_ram'] = gerar_status(ram)
    dados['status_disco'] = gerar_status(disco)

    return dados


def atualizar_csv_local(
    hostname,
    servidor_id,
    empresa_id,
    dados,
    existe
):
    mode = 'a' if existe else 'w'

    with open("temp.csv", mode, newline='') as file:

        writer = csv.writer(file)

        if mode == 'w':
            writer.writerow([
                "data_hora",
                "hostname",
                "id_empresa",
                "servidor_id",
                "ip",
                "cpu",
                "ram",
                "disco",
                "health_score",
                "status_cpu",
                "status_ram",
                "status_disco"
            ])

        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            hostname,
            empresa_id,
            servidor_id,
            dados.get('ip'),
            dados.get('cpu'),
            dados.get('ram'),
            dados.get('disco'),
            dados.get('health_score'),
            dados.get('status_cpu'),
            dados.get('status_ram'),
            dados.get('status_disco')
        ])


def main():
    hostname = socket.gethostname()

    print(f"Hostname: {hostname}")

    s3 = conectar_s3()

    while True:
        try:
            print("\nNova coleta...")

            info = obter_servidor_info(hostname)

            if not info:
                print("Servidor não encontrado!")

                time.sleep(INTERVALO)
                continue

            servidor_id = info["servidor_id"]
            empresa_id = info["empresa_id"]

            componentes = obter_componentes_servidor(servidor_id)

            if not componentes:
                print("Nenhum componente ativo!")

                time.sleep(INTERVALO)
                continue

            dados = coletar_metricas(componentes)

            key = gerar_chave_s3(
                empresa_id,
                hostname
            )

            existe = arquivo_existe_s3(s3, key)

            if existe:
                print("Baixando CSV do S3...")
                baixar_csv_s3(s3, key)

            else:
                print("Criando novo CSV...")

            atualizar_csv_local(
                hostname,
                servidor_id,
                empresa_id,
                dados,
                existe
            )

            print("Enviando CSV para S3...")

            enviar_csv_s3(s3, key)

            print("Coleta finalizada!")

        except Exception as e:
            print(f"Erro: {e}")

        time.sleep(INTERVALO)


if __name__ == "__main__":
    main()