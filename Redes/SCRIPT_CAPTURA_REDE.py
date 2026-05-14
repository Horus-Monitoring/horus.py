import psutil 
import random
import time
import boto3
import mysql.connector
import csv
from datetime import datetime
from getmac import get_mac_address #Função específica para MAC Adress
import subprocess #Permite executar comandos no sistema operacional
import re #Manipulação de strings
import requests #Fazer requisições para APIs
from botocore.exceptions import ClientError
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
    "user": "root",
    "password": "",
    "database": ""
}

INTERVALO = 1800

#Aviationstack

def dados_aviationstack():
    
    params = {
    'access_key': '', #chave da API - limite de 6/100 requisições
    'dep_iata': 'GRU',
    'limit': 100
    }

    response = requests.get('https://api.aviationstack.com/v1/flights', params = params)

    if response.status_code != 200:
        print("Erro AviationStack")
        return []
    
    data = response.json()
    data_aviationstack = data['data']
    data_api = []
    #Matriz para transformação em CSV e armazenamento do histórico de voos

    for voo in data_aviationstack:
        registro = {
            "timestamp_coleta": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "numero_voo": voo.get('flight', {}).get('iata'), #IATA é um código composto pela companhia + numero do voo
            "status": voo.get('flight_status'),
            "origem": voo.get('departure', {}).get('airport'),
            "destino": voo.get('arrival', {}).get('airport'),
            "delay_origem": voo.get('departure', {}).get('delay'),
            "delay_destino": voo.get('arrival', {}).get('delay') 
        }
        data_api.append(registro)
    
    return(data_api)

def conectar_s3():
    return boto3.client(
        's3',
        aws_access_key_id=AWS_CONFIG["aws_access_key_id"],
        aws_secret_access_key=AWS_CONFIG["aws_secret_access_key"],
        aws_session_token=AWS_CONFIG["aws_session_token"],
        region_name=AWS_CONFIG["region_name"]
    )

def gerar_chave_s3(empresa_id, macadress):
    return f"raw/empresa_{empresa_id}/{macadress}/metricas.csv"

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

def obter_servidor_info(macadress):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_servidor, fk_empresa
        FROM servidor
        WHERE macadress = %s
    """, (macadress,)) #alterar para MAC Adress

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

def tempo_atual(): #Coleta a data-hora
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def coletar_mac_address(): #Coleta o MAC Adress
    return get_mac_address()

def coletar_dados_rede(): #Coleta dados para métricas de fluxo de rede e pacotes
    network = psutil.net_io_counters();
    return{
        "bytes_recv": network.bytes_recv,
        "bytes_sent": network.bytes_sent,
        "pack_recv": network.packets_recv,
        "pack_sent": network.packets_sent
    }
def ping_shell():
    cmd = ["ping", "-n", "10", "8.8.8.8" ]

    try:
        cmd = ["ping", "-n", "10", "8.8.8.8" ] #-n para Windowns e -c para Ubuntu

        resultado = subprocess.run(cmd, capture_output=True, text=True, check=True) #Executa o comando no shell
        saida = resultado.stdout #Captura a saída (Standard Output)
        
        saida = " ".join(saida.split()) #remove quebras de linha para facilitar o regex

        return saida

    except subprocess.CalledProcessError: #Chama uma "exception", como no Java
        print("Erro ao executar o comando.")
        return None

def coletar_pacotes(saida):
    
    padrao = r"\((\d+)% de perda\)" #Verificar a saída padrão no ubuntu para modificar
    #Manipulando string onde \d+ recebe qualquer número, \(\) busca por parênteses e \s considera quebra de linha 
    match = re.search(padrao, saida) #Buscando a string na saida do shell
        
    if match:
        perda = match.group(1) #captura o primeiro resultado obtido na expressão regular na var padrao
        return int(perda)
    else:
        return None
        
def coletar_latencia(saida):

    padrao_tempo = r"tempo=(\d+)ms" 
    tempos = re.findall(padrao_tempo, saida)

    if tempos:
        return tempos
    else:
        print("Erro ao capturar o tempo de latência.")
        return None
    
def coletar_latencia_componentes(): #simulação da latencia entre os diferentes componentes do SAGITARIO
    return {
        "lat_adsb_rastreamento": round(random.uniform(20,50),2),
        "lat_rastreamento_correlacao": round(random.uniform(30,80),2),
        "lat_correlacao_rotas": round(random.uniform(40,100),2),
        "lat_rotas_api": round(random.uniform(10,40),2),
        "lat_api_bd": round(random.uniform(50,150),2),
        "lat_bd_sync": round(random.uniform(30,90),2)
    }

def dados_opensky():
    url = "https://opensky-network.org/api/states/all"
    response = requests.get(url) #Response é um objeto HTTP
    response_json = response.json()
    if response.status_code == 200: #Requisição bem sucedida
        return response_json
    else:
        print("Erro na requisição à API OpenSky Network.")
        return None

def opensky_timestamp(response_json):
    return response_json["time"]

def opensky_aeronaves(response_json):
    total_flights = 0
    for r in response_json["states"]:
        if r[2] and r[2] == "Brazil" or r[2] == "Brasil" or r[2] == "BR":
            total_flights += 1
    return total_flights

def contato_adsb(response_json):
    tempo_atual = response_json["time"]
    atualizacao = []
    for r in response_json["states"]:
        if r[2] == "Brazil" or r[2] == "Brasil" or r[2] == "BR":
            ultima_atualizacao = r[4]
            atualizacao.append(tempo_atual - ultima_atualizacao)
    return atualizacao   

def coletar_banda_processos(total_aeronaves):
    return {
        "rastreamento_mbps": round(total_aeronaves * random.uniform(0.4,0.8),2),
        "rotas_mbps": round(total_aeronaves * random.uniform(0.2,0.5),2),
        "correlacao_mbps": round(total_aeronaves * random.uniform(0.3,0.7),2),
        "api_gateway_mbps": round(total_aeronaves * random.uniform(0.1,0.4),2),
        "bd_mbps": round(total_aeronaves * random.uniform(0.2,0.6),2),
        "sync_service_mbps": round(total_aeronaves * random.uniform(0.15,0.5),2)
    }

def perda_pacotes_componentes():

    return {
        "rastreamento_loss": round(random.expovariate(3),2),
        "correlacao_loss": round(random.expovariate(3),2),
        "rotas_loss": round(random.expovariate(3),2),
        "api_loss": round(random.expovariate(3),2),
        "bd_loss": round(random.expovariate(3),2),
        "sync_loss": round(random.expovariate(3),2)
    }
    #Uso de variação exponencial para tornar a perda mais próxima de 1%

def atualizar_csv_local(
    hostname,
    servidor_id,
    empresa_id,
    mac_address,
    rede,
    packet_loss,
    latencia,
    lat_componentes,
    banda_processos,
    perda_componentes,
    opensky_data,
    total_aeronaves,
    avg_adsb_update,
    existe
):

    mode = 'a' if existe else 'w'

    with open("temp.csv", mode, newline='', encoding="utf-8") as file:
        writer = csv.writer(file)

        if mode == 'w':
            writer.writerow([
                "timestamp",
                "hostname",
                "empresa_id",
                "servidor_id",
                "mac_address",

                # rede real
                "bytes_recv",
                "bytes_sent",
                "pack_recv",
                "pack_sent",
                "packet_loss_internet",

                # latência internet
                "latency_min_ms",
                "latency_avg_ms",
                "latency_max_ms",

                # latência componentes
                "lat_adsb_rastreamento",
                "lat_rastreamento_correlacao",
                "lat_correlacao_rotas",
                "lat_rotas_api",
                "lat_api_bd",
                "lat_bd_sync",

                # banda
                "rastreamento_mbps",
                "rotas_mbps",
                "correlacao_mbps",
                "api_gateway_mbps",
                "bd_mbps",
                "sync_service_mbps",

                # perda componentes
                "rastreamento_loss",
                "correlacao_loss",
                "rotas_loss",
                "api_loss",
                "bd_loss",
                "sync_loss",

                # opensky
                "opensky_timestamp",
                "total_aeronaves",
                "avg_adsb_update_seconds",
            ])

        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            hostname,
            empresa_id,
            servidor_id,
            mac_address,

            # rede
            rede["bytes_recv"],
            rede["bytes_sent"],
            rede["pack_recv"],
            rede["pack_sent"],
            packet_loss,

            # latência internet
            min(latencia) if latencia else None,
            sum(latencia)/len(latencia) if latencia else None,
            max(latencia) if latencia else None,

            # latência componentes
            lat_componentes["lat_adsb_rastreamento"],
            lat_componentes["lat_rastreamento_correlacao"],
            lat_componentes["lat_correlacao_rotas"],
            lat_componentes["lat_rotas_api"],
            lat_componentes["lat_api_bd"],
            lat_componentes["lat_bd_sync"],

            # banda processos
            banda_processos["rastreamento_mbps"],
            banda_processos["rotas_mbps"],
            banda_processos["correlacao_mbps"],
            banda_processos["api_gateway_mbps"],
            banda_processos["bd_mbps"],
            banda_processos["sync_service_mbps"],

            # perda componentes
            perda_componentes["rastreamento_loss"],
            perda_componentes["correlacao_loss"],
            perda_componentes["rotas_loss"],
            perda_componentes["api_loss"],
            perda_componentes["bd_loss"],
            perda_componentes["sync_loss"],

            # opensky
            opensky_timestamp(opensky_data),
            total_aeronaves,
            avg_adsb_update
        ])

def salvar_voos_csv(voos): #Conselho da Profa. Giu 
    
    arquivo = "flights_raw.csv"
    existe = os.path.exists(arquivo)

    with open(arquivo, mode="a", newline="", encoding="utf-8") as file:

        writer = csv.DictWriter(
            file,
            fieldnames=[
                "timestamp_coleta",
                "numero_voo",
                "status",
                "origem",
                "destino",
                "delay_origem",
                "delay_destino"
            ]
        )

        if not existe:
            writer.writeheader()

        for voo in voos:
            writer.writerow(voo)


#Abstrair voos sem atualização com base no histórico coletado pelo Aviation Stack 
# (voos com o mesmo status estão desatualizados)
#Rotas sem atualização com base na contagem entre origem e destino
# (mesma quantidade de aeronaves = sem atualização)