import psutil 
import json
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
import socket
from dotenv import load_dotenv
from pathlib import Path

atual = Path(__file__).resolve().parent
pai = atual.parent
load_dotenv(dotenv_path= pai / ".env")  #Buscando a .env


AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "aws_session_token": os.getenv("AWS_SESSION_TOKEN"),
    "region_name": os.getenv("AWS_REGION_NAME"),
    "bucket_name": os.getenv("AWS_BUCKET_NAME")
}

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE")
}

API_CONFIG = {
    "access_key": os.getenv("AVIATIONSTACK_ACCESS_KEY"),
    "client_id": os.getenv("OPENSKY_CLIENT_ID"),
    "secret": os.getenv("OPENSKY_SECRET")
}

API_CLIMA = {
    "url_sensor" : os.getenv("URL"),
    "api_key_clima" : os.getenv("API_KEY_CLIMA"),
}

CIDADE = "Sao Paulo"

URL_CLIMA = (
    f"https://api.openweathermap.org/data/2.5/weather"
    f"?q={CIDADE}"
    f"&appid={API_CLIMA["api_key_clima"]}"
    f"&units=metric"
    f"&lang=pt_br"
)
URL_SENSOR = "http://localhost:8085/data.json"
TEMP_MAX_CPU = 90
INTERVALO = 600

def conectar_s3():
    return boto3.client(
        's3',
        aws_access_key_id=AWS_CONFIG["aws_access_key_id"],
        aws_secret_access_key=AWS_CONFIG["aws_secret_access_key"],
        aws_session_token=AWS_CONFIG["aws_session_token"],
        region_name=AWS_CONFIG["region_name"]
    )

def arquivo_existe_s3(s3, key):
    try:
        s3.head_object(Bucket=AWS_CONFIG["bucket_name"], Key=key)
        return True
    except ClientError:
        return False

def baixar_csv_s3(s3, key_s3, arquivo_local):
    s3.download_file(
        AWS_CONFIG["bucket_name"],
        key_s3,
        arquivo_local
    )

def enviar_csv_s3(s3, arquivo_local, key):
    s3.upload_file(arquivo_local, AWS_CONFIG["bucket_name"], key)

def obter_servidor_info(mac_address):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_servidor, fk_empresa, mac_address
        FROM servidor
        WHERE mac_address = %s
    """, (mac_address,)) #alterar para MAC Adress

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
    mac = get_mac_address()
    print(mac.upper())
    return mac.upper().replace("-", ":")


# CLASSIFICAÇÃO TEMPERATURA

def classificar(temp_max):

    if temp_max < 65:
        return "Normal"

    elif temp_max < 75:
        return "Alerta"

    elif temp_max < 85:
        return "Medio"

    return "Critico"

# MARGEM TÉRMICA

def calcular_margem_termica(temp_max):

    return round(
        TEMP_MAX_CPU - temp_max,
        1
    )

# STATUS MARGEM

def classificar_margem(margem):

    if margem > 30:
        return "Excelente"

    elif margem > 20:
        return "Boa"

    elif margem > 10:
        return "Atencao"

    elif margem > 0:
        return "Critico"

    return "Throttling"

# LATÊNCIA

def obter_latencia():

    try:

        comando = subprocess.run(

            ["ping", "-n", "1", "8.8.8.8"],

            capture_output=True,

            text=True

        )

        saida = comando.stdout

        for linha in saida.split("\n"):

            if "tempo=" in linha.lower():

                valor = (
                    linha
                    .split("tempo=")[1]
                    .split("ms")[0]
                    .replace("<", "")
                    .strip()
                )

                return int(valor)

    except:
        pass

    return 0

# THROTTLING

def verificar_throttling(temp_max):

    return "SIM" if temp_max >= 90 else "NAO"

# CLIMA
def obter_clima():
    try:

        response = requests.get(
            URL_CLIMA,
            timeout=10
        )

        data = response.json()

        if "main" not in data:

            print(
                "Erro API clima:",
                data
            )

            return {

                "temperatura_ambiente": 0,

                "descricao": "indisponivel",

                "umidade": 0
            }

        return {

            "temperatura_ambiente":
                data["main"]["temp"],

            "descricao":
                data["weather"][0]["description"],

            "umidade":
                data["main"]["humidity"]
        }

    except Exception as erro:

        print(
            "Erro clima:",
            erro
        )

        return {

            "temperatura_ambiente": 0,

            "descricao": "indisponivel",

            "umidade": 0
        }
    
# TEMPERATURAS CPU

def buscar_temperaturas():

    response = requests.get(URL_SENSOR)

    data = response.json()

    temperaturas = {}

    cores_encontrados = set()

    def percorrer(node):

        if isinstance(node, dict):

            texto = node.get("Text", "")

            valor = node.get("Value", "")

            if (
                "Core #" in texto
                and "°C" in valor
            ):

                try:

                    numero_core = int(

                        texto
                        .split("#")[1]
                        .split()[0]

                    )

                    if numero_core in cores_encontrados:
                        return

                    temperatura = float(

                        valor
                        .replace("°C", "")
                        .replace(",", ".")
                        .strip()

                    )

                    temperaturas[
                        f"core_{numero_core}"
                    ] = temperatura

                    cores_encontrados.add(
                        numero_core
                    )

                except:
                    pass

            for key in node:

                percorrer(node[key])

        elif isinstance(node, list):

            for item in node:

                percorrer(item)

    percorrer(data)

    return dict(
        sorted(temperaturas.items())
    )

# COOLER RPM SIMULADO

def simular_fan_cpu(temp_cpu):

    if temp_cpu < 45:

        rpm_base = 1800

    elif temp_cpu < 55:

        rpm_base = 2400

    elif temp_cpu < 65:

        rpm_base = 3200

    elif temp_cpu < 75:

        rpm_base = 4200

    elif temp_cpu < 85:

        rpm_base = 5200

    else:

        rpm_base = 6200

    variacao = random.randint(
        -150,
        150
    )

    rpm_final = rpm_base + variacao

    if rpm_final < 0:
        rpm_final = 0

    return rpm_final

# ÍNDICE RESFRIAMENTO

def calcular_ier(

    rpm_fan,

    temp_cpu,

    temp_ambiente

):

    diferenca = (
        temp_cpu - temp_ambiente
    )

    ier = rpm_fan / (
        diferenca + 1
    )

    return round(ier, 1)

# STATUS RESFRIAMENTO

def classificar_ier(ier):

    if ier > 80:
        return "Excelente"

    elif ier > 50:
        return "Boa"

    elif ier > 30:
        return "Atencao"

    return "Critico"

#=========================
#REDE
#=========================

def coletar_dados_rede(): #Coleta dados para métricas de fluxo de rede e pacotes
    network = psutil.net_io_counters();
    return{
        "bytes_recv": network.bytes_recv,
        "bytes_sent": network.bytes_sent,
        "pack_recv": network.packets_recv,
        "pack_sent": network.packets_sent
    }


def ping_shell():
    cmd = ["ping", "-n", "10", "8.8.8.8"]

    try:
        resultado = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )

        return resultado.stdout

    except subprocess.CalledProcessError:
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

    padrao_tempo = r'tempo[=<](\d+)ms'
    tempos = re.findall(padrao_tempo, saida)

    if tempos:
        return [int(x) for x in tempos]

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
    url_auth = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    url_api = "https://opensky-network.org/api/states/all"

    try:
        auth_response = requests.post(
            url_auth,
            data={
                "grant_type": "client_credentials",
                "client_id": API_CONFIG["client_id"],
                "client_secret": API_CONFIG["secret"]
            },
            timeout=10
        )

        if auth_response.status_code != 200:
            print("Erro autenticação OpenSky:", auth_response.text)
            return None

        token = auth_response.json()["access_token"]

        response = requests.get(
            url_api,
            headers={
                "Authorization": f"Bearer {token}"
            },
            timeout=10
        )

        if response.status_code == 200:
            return response.json()

        print("Erro OpenSky:", response.status_code)
        return None

    except Exception as e:
        print(f"Erro OpenSky: {e}")
        return None

def opensky_timestamp(response_json):
    return response_json["time"]

def opensky_aeronaves(response_json):
    total_flights = 0

    for r in response_json.get("states", []):
        if r[2] in ["Brazil", "Brasil", "BR"]:
            total_flights += 1

    return total_flights

def dados_aviationstack():
   try: 
        params = {
        'access_key': API_CONFIG["access_key"], #chave da API - limite de 6/100 requisições
        'dep_iata': 'GRU',  
        'limit': 100
        }

        response = requests.get('https://api.aviationstack.com/v1/flights', params = params, timeout=10)

        if response.status_code != 200:
            print("Erro AviationStack")
            return []
        
        data = response.json()

        if "data" not in data:
                return []
        
        data_aviationstack = data['data']
        data_api = []
        #JSON para transformação em CSV e armazenamento do histórico de voos

        for voo in data_aviationstack:
            registro = {
                "timestamp_coleta": tempo_atual(),
                "numero_voo": voo.get('flight', {}).get('iata'), #IATA é um código composto pela companhia + numero do voo
                "status": voo.get('flight_status'),
                "origem": voo.get('departure', {}).get('airport'),
                "destino": voo.get('arrival', {}).get('airport'),
                "delay_origem": voo.get('departure', {}).get('delay'),
                "delay_destino": voo.get('arrival', {}).get('delay') 
            }
            data_api.append(registro)
        
        return(data_api)
   
   except Exception as e:
       print(f"Erro na API AviationStack: {e}")
       return []

def contato_adsb(response_json):
    tempo_atual = response_json["time"]
    atualizacao = []

    for r in response_json.get("states", []):
        if r[2] in ["Brazil", "Brasil", "BR"]:
            ultima_atualizacao = r[4]

            if ultima_atualizacao:
                atualizacao.append(
                    tempo_atual - ultima_atualizacao
                )

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
    dados,
    processos,
    rede,
    packet_loss,
    latencia,
    lat_componentes,
    banda_processos,
    perda_componentes,
    opensky_data,
    total_aeronaves,
    avg_adsb_update,
    metricas_temp
):

    os.makedirs("raw", exist_ok=True)

    arquivo = "raw/raw.csv"

    existe = (
        os.path.exists(arquivo)
        and
        os.path.getsize(arquivo) > 0
    )

    mode = 'a' if existe else 'w'

    with open(
        arquivo,
        mode,
        newline='',
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        if not existe:

            writer.writerow([
                "timestamp",
                "hostname",
                "empresa_id",
                "servidor_id",
                "mac_address",

                "ip",
                "cpu",
                "ram",
                "disco",
                "health_score",
                "status_cpu",
                "status_ram",
                "status_disco",

                "temp_max_cpu",
                "status_temperatura",
                "margem_termica",
                "status_margem",
                "temperatura_ambiente",
                "clima",
                "umidade",
                "fan_principal_rpm",
                "indice_resfriamento",
                "status_resfriamento",
                "throttling",
                
                "quantidade_cores",
                "temperaturas_cores",

                "bytes_recv",
                "bytes_sent",
                "pack_recv",
                "pack_sent",
                "packet_loss_internet",

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

                "opensky_timestamp",
                "total_aeronaves",
                "avg_adsb_update_seconds",
            ])

        writer.writerow([
            datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

            hostname,
            empresa_id,
            servidor_id,
            mac_address,

            dados.get('ip'),
            dados.get('cpu'),
            dados.get('ram'),
            dados.get('disco'),
            dados.get('health_score'),
            dados.get('status_cpu'),
            dados.get('status_ram'),
            dados.get('status_disco'),

            metricas_temp["temp_max"],
            metricas_temp["status"],
            metricas_temp["margem_termica"],
            metricas_temp["status_margem"],
            metricas_temp["temperatura_ambiente"],
            metricas_temp["clima"],
            metricas_temp["umidade"],
            metricas_temp["fan_principal_rpm"],
            metricas_temp["indice_resfriamento"],
            metricas_temp["status_resfriamento"],
            metricas_temp["throttling"],
            len(metricas_temp["temperaturas_cores"])
            if metricas_temp.get("temperaturas_cores")
            else 0,

            json.dumps(
                 metricas_temp.get("temperaturas_cores", {})
            ),

            rede["bytes_recv"],
            rede["bytes_sent"],
            rede["pack_recv"],
            rede["pack_sent"],

            packet_loss,

            min(latencia)
            if latencia else None,

            sum(latencia) / len(latencia)
            if latencia else None,

            max(latencia)
            if latencia else None,

            lat_componentes[
                "lat_adsb_rastreamento"
            ],

            lat_componentes[
                "lat_rastreamento_correlacao"
            ],

            lat_componentes[
                "lat_correlacao_rotas"
            ],

            lat_componentes[
                "lat_rotas_api"
            ],

            lat_componentes[
                "lat_api_bd"
            ],

            lat_componentes[
                "lat_bd_sync"
            ],

            banda_processos[
                "rastreamento_mbps"
            ],

            banda_processos[
                "rotas_mbps"
            ],

            banda_processos[
                "correlacao_mbps"
            ],

            banda_processos[
                "api_gateway_mbps"
            ],

            banda_processos[
                "bd_mbps"
            ],

            banda_processos[
                "sync_service_mbps"
            ],

            perda_componentes[
                "rastreamento_loss"
            ],

            perda_componentes[
                "correlacao_loss"
            ],

            perda_componentes[
                "rotas_loss"
            ],

            perda_componentes[
                "api_loss"
            ],

            perda_componentes[
                "bd_loss"
            ],

            perda_componentes[
                "sync_loss"
            ],

            opensky_timestamp(opensky_data)
            if opensky_data else None,

            total_aeronaves,

            avg_adsb_update
        ])

        file.flush()

        os.fsync(file.fileno())

def salvar_voos_csv(voos): #Conselho da Profa. Giu 
    
    os.makedirs("raw", exist_ok=True)

    arquivo = "raw/flights_raw.csv"
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
        
        file.flush()
        os.fsync(file.fileno())

#==========================
#PROCESSOS
#==========================
def capturar_processos(): 

    processos = []

    for proc in psutil.process_iter([
        'pid',
        'name',
        'username',
        'status'
    ]):

        try:
            cpu_percent = proc.cpu_percent(interval=0.1)
            num_cpu = psutil.cpu_count(logical=True)
            cpu = cpu_percent / num_cpu
            ram_percent = proc.memory_percent()
            ram_mb = proc.memory_info().rss / 1024 / 1024 # transforma bytes para KB e depois para MB
            tempo_execucao = (
                datetime.now() -
                datetime.fromtimestamp(proc.create_time())
            )

            # formatação de tempo de execução
            dias = tempo_execucao.days
            horas = tempo_execucao.seconds // 3600
            minutos = (tempo_execucao.seconds % 3600) // 60

            tempo_formatado = f"{dias}d {horas}h {minutos}min"
            # =================================================

            inicio = time.perf_counter()
            proc.memory_percent()
            fim = time.perf_counter()
            latencia = (fim - inicio) * 1000

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            processo = {
                "timestamp": timestamp,
                "hostname": socket.gethostname(),
                "pid": proc.info['pid'],
                "nome": proc.info['name'],
                "usuario": proc.info['username'],
                "cpu": round(cpu, 2),
                "ram_percent": round(ram_percent, 2),
                "ram_mb": round(ram_mb, 2),
                "status": proc.info['status'],
                "tempo_execucao": str(tempo_formatado),
                "latencia_ms": round(latencia, 2)
            }

            processos.append(processo)

        except Exception as e:
            print(e)
    
    return processos

def salvar_processos_csv(processos):

    os.makedirs("raw", exist_ok=True)

    arquivo = "raw/process_raw.csv"
    existe = os.path.exists(arquivo)

    with open(arquivo, mode="a", newline="", encoding="utf-8") as file:

        writer = csv.DictWriter(
            file,
            fieldnames=[
                "timestamp",
                "hostname",
                "pid",
                "nome",
                "usuario",
                "cpu",
                "ram_percent",
                "ram_mb",
                "status",
                "tempo_execucao",
                "latencia_ms"
            ]
        )

        if not existe:
            writer.writeheader()

        writer.writerows(processos)

        file.flush()
        os.fsync(file.fileno())
#==========================
#Servidores
#==========================

def gerar_status(valor):
    if valor >= 85:
        return "Critico"
    elif valor >= 70:
        return "Atencao"
    return "Estavel"

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
        dados['disco'] = psutil.disk_usage(os.path.abspath(os.sep)).percent


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

#==========================
#MAIN
#==========================

def main():

    ultima_execucao_aviation = 0
    ultima_execucao_upload = 0

    INTERVALO_AVIATION = 14400
    INTERVALO_UPLOAD = 3600

    print("Iniciando coleta local...")

    os.makedirs("raw", exist_ok=True)

    # ==========================
    # AWS
    # ==========================

    s3 = conectar_s3()

    # ==========================
    # MAC ADDRESS
    # ==========================

    mac_address = coletar_mac_address()

    # ==========================
    # MYSQL
    # ==========================

    info = obter_servidor_info(mac_address)

    if not info:

        print("Servidor não encontrado!")

        time.sleep(INTERVALO)

        return

    servidor_id = info["servidor_id"]
    empresa_id = info["empresa_id"]

    # ==========================
    # KEYS S3
    # ==========================

    key = (
        f"raw/empresa_{empresa_id}/"
        f"{mac_address}/raw.csv"
    )

    process_key = (
        f"raw/empresa_{empresa_id}/"
        f"{mac_address}/process_raw.csv"
    )

    flights_key = (
        f"raw/empresa_{empresa_id}/"
        f"{mac_address}/flights_raw.csv"
    )

    # ==========================
    # DOWNLOAD CSVs S3
    # ==========================

    if arquivo_existe_s3(s3, key):

        baixar_csv_s3(
            s3,
            key,
            "raw/raw.csv"
        )

    else:

        print(
            "Arquivo raw não existe na S3."
        )

    if arquivo_existe_s3(s3, flights_key):

        baixar_csv_s3(
            s3,
            flights_key,
            "raw/flights_raw.csv"
        )

    else:

        print(
            "Arquivo flights não existe na S3."
        )

    if arquivo_existe_s3(s3, process_key):

        baixar_csv_s3(
            s3,
            process_key,
            "raw/process_raw.csv"
        )

    else:

        print(
            "Arquivo process não existe na S3."
        )

    # ==========================
    # LOOP PRINCIPAL
    # ==========================

    while True:

        try:

            tempo_atual_loop = time.time()

            hostname = socket.gethostname()

            # ==========================
            # CPU / RAM / DISCO
            # ==========================

            print(
                "Coletando dados de CPU/RAM/DISCO..."
            )

            componentes = obter_componentes_servidor(
                servidor_id
            )

            if not componentes:

                print(
                    "Nenhum componente ativo!"
                )

                time.sleep(INTERVALO)

                continue

            dados = coletar_metricas(
                componentes
            )

            # ==========================
            # PROCESSOS
            # ==========================

            print(
                "Coletando processos..."
            )

            processos = capturar_processos()

            salvar_processos_csv(
                processos
            )

            # ==========================
            # REDE
            # ==========================

            print(
                "Coletando rede..."
            )

            saida_ping = ping_shell()

            if saida_ping:

                packet_loss = coletar_pacotes(
                    saida_ping
                )

                latencia = coletar_latencia(
                    saida_ping
                )

                if latencia:

                    latencia = [
                        int(x)
                        for x in latencia
                    ]

            else:

                packet_loss = None
                latencia = None

            rede = coletar_dados_rede()

            # ==========================
            # OPENSKY
            # ==========================

            print(
                "Consultando OpenSky..."
            )

            opensky_data = dados_opensky()

            if opensky_data:

                total_aeronaves = (
                    opensky_aeronaves(
                        opensky_data
                    )
                )

                adsb_updates = contato_adsb(
                    opensky_data
                )

                avg_adsb_update = (

                    sum(adsb_updates)
                    / len(adsb_updates)

                    if adsb_updates
                    else None
                )

            else:

                opensky_data = None
                total_aeronaves = None
                avg_adsb_update = None

            # ==========================
            # LATÊNCIA COMPONENTES
            # ==========================

            lat_componentes = (
                coletar_latencia_componentes()
            )

            # ==========================
            # BANDA PROCESSOS
            # ==========================

            banda_processos = (
                coletar_banda_processos(
                    total_aeronaves
                    if total_aeronaves
                    else 0
                )
            )

            # ==========================
            # PERDA COMPONENTES
            # ==========================

            perda_componentes = (
                perda_pacotes_componentes()
            )

            # ==========================
            # TEMPERATURA CPU
            # ==========================

            print(
                "Coletando temperatura CPU..."
            )

            try:

                temperaturas = (
                    buscar_temperaturas()
                )

                if temperaturas:

                    clima = obter_clima()

                    temp_max = max(
                        temperaturas.values()
                    )

                    fan_principal = (
                        simular_fan_cpu(
                            temp_max
                        )
                    )

                    ier = calcular_ier(

                        fan_principal,

                        temp_max,

                        clima[
                            "temperatura_ambiente"
                        ]
                    )

                    margem = (
                        calcular_margem_termica(
                            temp_max
                        )
                    )

                    metricas_temp = {

                        "temp_max":
                            temp_max,
                        
                        "temperaturas_cores": 
                            temperaturas,

                        "status":
                            classificar(
                                temp_max
                            ),

                        "margem_termica":
                            margem,

                        "status_margem":
                            classificar_margem(
                                margem
                            ),

                        "temperatura_ambiente":
                            clima[
                                "temperatura_ambiente"
                            ],

                        "clima":
                            clima[
                                "descricao"
                            ],

                        "umidade":
                            clima[
                                "umidade"
                            ],

                        "fan_principal_rpm":
                            fan_principal,

                        "indice_resfriamento":
                            ier,

                        "status_resfriamento":
                            classificar_ier(
                                ier
                            ),

                        "throttling":
                            verificar_throttling(
                                temp_max
                            )
                    }

                else:

                    metricas_temp = {

                        "temp_max": None,
                        "temperaturas_cores": {},
                        "status": None,
                        "margem_termica": None,
                        "status_margem": None,
                        "temperatura_ambiente": None,
                        "clima": None,
                        "umidade": None,
                        "fan_principal_rpm": None,
                        "indice_resfriamento": None,
                        "status_resfriamento": None,
                        "throttling": None
                    }

            except Exception as e:

                print(
                    f"Erro temperatura CPU: {e}"
                )

                metricas_temp = {

                    "temp_max": None,
                    "status": None,
                    "margem_termica": None,
                    "status_margem": None,
                    "temperatura_ambiente": None,
                    "clima": None,
                    "umidade": None,
                    "fan_principal_rpm": None,
                    "indice_resfriamento": None,
                    "status_resfriamento": None,
                    "throttling": None
                }

            # ==========================
            # AVIATIONSTACK
            # ==========================

            if (

                tempo_atual_loop
                - ultima_execucao_aviation

                >= INTERVALO_AVIATION
            ):

                print(
                    "Consultando AviationStack..."
                )

                voos = dados_aviationstack()

                if voos:

                    salvar_voos_csv(
                        voos
                    )

                    print(
                        f"{len(voos)} voos salvos."
                    )

                ultima_execucao_aviation = (
                    tempo_atual_loop
                )

            # ==========================
            # CSV
            # ==========================

            atualizar_csv_local(

                hostname=hostname,

                servidor_id=servidor_id,

                empresa_id=empresa_id,

                mac_address=mac_address,

                dados=dados,

                processos=processos,

                rede=rede,

                packet_loss=packet_loss,

                latencia=latencia,

                lat_componentes=lat_componentes,

                banda_processos=banda_processos,

                perda_componentes=perda_componentes,

                opensky_data=opensky_data,

                total_aeronaves=total_aeronaves,

                avg_adsb_update=avg_adsb_update,

                metricas_temp=metricas_temp
            )

            print(
                "raw salvo com sucesso."
            )

            print(
                "Aguardando próxima coleta..."
            )

            # ==========================
            # UPLOAD S3
            # ==========================

            if (

                tempo_atual_loop
                - ultima_execucao_upload

                >= INTERVALO_UPLOAD
            ):

                print(
                    "Enviando CSVs para S3..."
                )

                enviar_csv_s3(
                    s3,
                    "raw/raw.csv",
                    key
                )

                if os.path.exists(
                    "raw/process_raw.csv"
                ):

                    enviar_csv_s3(

                        s3,

                        "raw/process_raw.csv",

                        process_key
                    )

                if os.path.exists(
                    "raw/flights_raw.csv"
                ):

                    enviar_csv_s3(

                        s3,

                        "raw/flights_raw.csv",

                        flights_key
                    )

                ultima_execucao_upload = (
                    tempo_atual_loop
                )

        except Exception as e:

            print(
                f"Erro geral na coleta: {e}"
            )

        # ==========================
        # ESPERA
        # ==========================

        minutos_total = INTERVALO // 60

        print(
            f"Nova coleta em "
            f"{minutos_total} minutos..."
        )

        for minuto in range(
            1,
            minutos_total + 1
        ):

            time.sleep(60)

            barra = "|" * minuto

            restante = "." * (
                minutos_total - minuto
            )

            print(

                f"\rAguardando próxima coleta: "
                f"[{barra}{restante}] "
                f"{minuto}/{minutos_total} min",

                end="",

                flush=True
            )

        print()


if __name__ == "__main__":

    main()