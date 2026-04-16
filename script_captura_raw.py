
import psutil 
import csv
import os
from datetime import datetime
import time
import boto3 
from botocore.exceptions import NoCredentialsError 
import socket
import mysql.connector 
from io import StringIO
import json

# Configurações do S3
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "seu-bucket-s3") # Substitua pelo nome do seu bucket
S3_RAW_FILE_KEY = "raw/all_metrics.csv" # Caminho para o CSV geral na camada RAW

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "user": os.environ.get("DB_USER", "root"),
    "password": os.environ.get("DB_PASSWORD", "Nm.05/08/03"), # ATENÇÃO: Não use senhas hardcoded em produção
    "database": os.environ.get("DB_DATABASE", "horus_db")
}


CSV_HEADERS = [
    "data_hora",
    "empresa_id",
    "empresa_nome",
    "servidor_id",
    "servidor_nome",
    "componente_id",
    "componente_v",
    "tipo",
    "valor",
    "limite",
    "top_5_cpu_processes_json"
]

def conectar_db():
    """Conecta ao banco de dados MySQL."""
    return mysql.connector.connect(**DB_CONFIG)

def obter_info_maquina_db(hostname):
    """Busca ID da empresa e servidor no DB com base no hostname."""
    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)
    empresa_id = None
    empresa_nome = None
    servidor_id = None
    servidor_nome = hostname 

  
    query = """
    SELECT
        e.idEmpresa, e.nome_empresa,
        s.idServidor, s.nome_servidor
    FROM Servidor s
    JOIN Empresa e ON s.fk_empresa = e.idEmpresa
    WHERE s.nome_servidor = %s
    """
    cursor.execute(query, (hostname,))
    result = cursor.fetchone()

    if result:
        empresa_id = result["idEmpresa"]
        empresa_nome = result["nome_empresa"]
        servidor_id = result["idServidor"]
        servidor_nome = result["nome_servidor"]
    else:
        print(f"Aviso: Hostname \'{hostname}\' não encontrado no banco de dados. Usando valores padrão/hostname.")

    cursor.close()
    conn.close()

    return empresa_id, empresa_nome, servidor_id, servidor_nome

def buscar_componentes_db(empresa_id, servidor_id):
    """Busca componentes a serem monitorados para um dado servidor/empresa no DB."""
    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)
    componentes = []

    query = """
    SELECT
        c.idComponentes,
        c.nome_componente,
        c.tipo_componente,
        cs.limite,
        cs.id_componente_v
    FROM Servidor s
    JOIN CompServidor cs ON cs.fk_servidor = s.idServidor
    JOIN Componentes c ON c.idComponentes = cs.fk_componente
    WHERE s.idServidor = %s AND s.fk_empresa = %s
    AND cs.ativo = TRUE
    """
    cursor.execute(query, (servidor_id, empresa_id))
    componentes = cursor.fetchall()

    cursor.close()
    conn.close()
    return componentes

def coletar_valor(tipo):
    """Coleta o valor da métrica especificada."""
    tipo = tipo.upper()

    if tipo == "CPU":
        return psutil.cpu_percent(interval=1)
    elif tipo == "RAM":
        return psutil.virtual_memory().percent
    elif tipo == "DISCO":
        return psutil.disk_usage("/").percent
    elif tipo == "REDE":
        net_io = psutil.net_io_counters()
        return net_io.bytes_sent + net_io.bytes_recv

    return None

def get_top_5_cpu_processes():
    """Identifica os 5 processos com maior uso de CPU."""
    processes_cpu = []

    psutil.cpu_percent(interval=0.1)

    for proc in psutil.process_iter(["pid", "name", "cpu_percent"]):
        try:
            cpu_percent = proc.cpu_percent(interval=None)
            if cpu_percent > 0:
                processes_cpu.append({"name": proc.name(), "percent": round(cpu_percent, 2)})
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

    top_5 = sorted(processes_cpu, key=lambda x: x["percent"], reverse=True)[:5]
    return top_5

def download_s3_file_content(bucket_name, s3_key):
    """Baixa o conteúdo de um arquivo do S3 e retorna como string."""
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
    return obj["Body"].read().decode("utf-8")

def upload_to_s3(data_buffer, bucket_name, s3_key):
    """Faz o upload de um buffer de dados para o S3."""
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=data_buffer.getvalue())
    print(f"Arquivo {s3_key} enviado com sucesso para s3://{bucket_name}/{s3_key}")
    return True

def monitorar_e_enviar():
    """Coleta métricas, anexa ao CSV geral e envia para S3."""
    hostname = socket.gethostname()
    empresa_id, empresa_nome, servidor_id, servidor_nome = obter_info_maquina_db(hostname)

    if not empresa_id or not servidor_id:
        raise ValueError("Não foi possível identificar a empresa ou o servidor. Verifique o banco de dados.")

    print(f"Monitorando máquina: {servidor_nome} (ID: {servidor_id}) da Empresa: {empresa_nome} (ID: {empresa_id})")

    componentes_monitorar = buscar_componentes_db(empresa_id, servidor_id)
    if not componentes_monitorar:
        print(f"Nenhum componente ativo encontrado para o servidor {servidor_id} da empresa {empresa_id}. Usando componentes padrão.")
        componentes_monitorar = [
            {"idComponentes": 1, "nome_componente": "CPU", "tipo_componente": "CPU", "limite": 80, "id_componente_v": "cpu_v1"},
            {"idComponentes": 2, "nome_componente": "RAM", "tipo_componente": "RAM", "limite": 70, "id_componente_v": "ram_v1"},
            {"idComponentes": 3, "nome_componente": "DISCO", "tipo_componente": "DISCO", "limite": 90, "id_componente_v": "disco_v1"},
            {"idComponentes": 4, "nome_componente": "REDE", "tipo_componente": "REDE", "limite": 100, "id_componente_v": "rede_v1"},
        ]

    while True:
        existing_content = None
        try:
            existing_content = download_s3_file_content(S3_BUCKET_NAME, S3_RAW_FILE_KEY)
        except NoCredentialsError:
            raise NoCredentialsError("Credenciais da AWS não encontradas. Certifique-se de que estão configuradas.")
        except boto3.client("s3").exceptions.NoSuchKey:
            print(f"Arquivo {S3_RAW_FILE_KEY} não encontrado no S3. Será criado um novo.")
        except Exception as e:
            raise Exception(f"Erro ao baixar arquivo do S3 ({S3_RAW_FILE_KEY}): {e}")

        data_buffer = StringIO()
        writer = csv.writer(data_buffer)


        if not existing_content:
            writer.writerow(CSV_HEADERS)
        else:
            lines = existing_content.splitlines()
            if lines and lines[0].strip() == ",".join(CSV_HEADERS):
                data_buffer.write(existing_content)
            else:
                writer.writerow(CSV_HEADERS)
                data_buffer.write(existing_content)

        top_5_cpu_processes = get_top_5_cpu_processes()
        top_5_cpu_processes_json = json.dumps(top_5_cpu_processes) # Converte para string JSON

        for item in componentes_monitorar:
            valor = coletar_valor(item["tipo_componente"])

            dado = {
                "data_hora": datetime.now().isoformat(),
                "empresa_id": empresa_id,
                "empresa_nome": empresa_nome,
                "servidor_id": servidor_id,
                "servidor_nome": servidor_nome,
                "componente_id": item["idComponentes"],
                "componente_v": item["id_componente_v"],
                "tipo": item["tipo_componente"],
                "valor": valor,
                "limite": item["limite"],
                "top_5_cpu_processes_json": top_5_cpu_processes_json # Adiciona a string JSON
            }
            writer.writerow([
                dado["data_hora"],
                dado["empresa_id"],
                dado["empresa_nome"],
                dado["servidor_id"],
                dado["servidor_nome"],
                dado["componente_id"],
                dado["componente_v"],
                dado["tipo"],
                dado["valor"],
                dado["limite"],
                dado["top_5_cpu_processes_json"]
            ])

        upload_to_s3(data_buffer, S3_BUCKET_NAME, S3_RAW_FILE_KEY)

        print(f"Monitoramento concluído para este ciclo. Próximo em 60 segundos.")
        time.sleep(30)

if __name__ == "__main__":
    print("Iniciando o script de captura de métricas para o CSV geral RAW do S3...")
    print("Certifique-se de que as variáveis de ambiente AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME, DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE estão configuradas.")
    monitorar_e_enviar()
