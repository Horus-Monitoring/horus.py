import psutil
import time
import socket
import csv
import boto3
import mysql.connector
import os
from datetime import datetime
from botocore.exceptions import ClientError

AWS_CONFIG = {
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "aws_session_token": "",
    "region_name": "",
    "bucket_name": ""
}

DB_CONFIG = {
    "host": "",
    "user": "",
    "password": "",
    "database": ""
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
        return {"servidor_id": result[0], "empresa_id": result[1]}
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

def coletar_metricas(componentes):
    dados = {}

    if 'CPU' in componentes:
        dados['cpu'] = psutil.cpu_percent(interval=1)

    if 'RAM' in componentes:
        dados['ram'] = psutil.virtual_memory().percent

    if 'DISCO' in componentes:
        dados['disco'] = psutil.disk_usage('/').percent

    if 'REDE_RX' in componentes or 'REDE_TX' in componentes:
        net1 = psutil.net_io_counters()
        time.sleep(1)
        net2 = psutil.net_io_counters()
        dados['rede_rx'] = net2.bytes_recv - net1.bytes_recv
        dados['rede_tx'] = net2.bytes_sent - net1.bytes_sent

    if 'PROCESSOS' in componentes:
        num_cores = psutil.cpu_count()

        for proc in psutil.process_iter():
            try:
                proc.cpu_percent(None)
            except:
                pass

        time.sleep(1)

        processos = []
        for proc in psutil.process_iter(['name', 'cpu_percent']):
            try:
                cpu_normalizado = proc.info['cpu_percent'] / num_cores
                processos.append({
                    "name": proc.info['name'],
                    "cpu": round(cpu_normalizado, 2)
                })
            except:
                pass

        top5 = sorted(processos, key=lambda x: x['cpu'], reverse=True)[:5]

        dados['processos'] = "; ".join(
            [f"{p['name']}({p['cpu']}%)" for p in top5]
        )

    return dados

def atualizar_csv_local(hostname, servidor_id, empresa_id, dados, existe):
    mode = 'a' if existe else 'w'

    with open("temp.csv", mode, newline='') as file:
        writer = csv.writer(file)

        if mode == 'w':
            writer.writerow([
                "data_hora",
                "hostname",
                "id_empresa",
                "servidor_id",
                "cpu",
                "ram",
                "disco",
                "rede_rx",
                "rede_tx",
                "processos"
            ])

        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            hostname,
            empresa_id,
            servidor_id,
            dados.get('cpu'),
            dados.get('ram'),
            dados.get('disco'),
            dados.get('rede_rx'),
            dados.get('rede_tx'),
            dados.get('processos')
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

            key = gerar_chave_s3(empresa_id, hostname)

            existe = arquivo_existe_s3(s3, key)

            if existe:
                print("Baixando CSV do S3...")
                baixar_csv_s3(s3, key)
            else:
                print("Criando novo CSV...")

            atualizar_csv_local(hostname, servidor_id, empresa_id, dados, existe)

            print("Enviando CSV para S3...")
            enviar_csv_s3(s3, key)

            print("Coleta finalizada!")

        except Exception as e:
            print(f"Erro: {e}")

        time.sleep(INTERVALO)

if __name__ == "__main__":
    main()