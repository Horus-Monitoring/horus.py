import boto3
import json
import mysql.connector
from datetime import datetime, timedelta
from io import StringIO
import pandas
import math
from dotenv import load_dotenv
from collections import defaultdict
from getmac import get_mac_address
import os
from collections import Counter
import numpy as np

USAR_LOCAL = True

load_dotenv()
env = os.getenv

def ler_csv(key):

    if USAR_LOCAL:

        caminho_local = normalizar_path(
            key.replace("/", os.sep)
        )

        if not os.path.exists(caminho_local):
            print(f"Arquivo não encontrado: {caminho_local}")
            return pandas.DataFrame()

        return pandas.read_csv(caminho_local, on_bad_lines='skip')

    else:

        obj = s3.get_object(
            Bucket=AWS_CONFIG["bucket_name"],
            Key=key
        )

        conteudo = obj['Body'].read().decode('utf-8')

        return pandas.read_csv(
            StringIO(conteudo),
            on_bad_lines='skip'
        )

AWS_CONFIG = {
    "aws_access_key_id": env("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": env("AWS_SECRET_ACCESS_KEY"),
    "aws_session_token": env("AWS_SESSION_TOKEN"),
    "region_name": env("AWS_REGION_NAME"),
    "bucket_name": env("AWS_BUCKET_NAME")
}

DB_CONFIG = {
    "host": env("DB_HOST"),
    "user": env("DB_USER"),
    "password": env("DB_PASSWORD"),
    "database": env("DB_DATABASE")
}

SEVERIDADE = {
    "crítico": 5,
    "alta": 4, 
    "média": 3, 
    "baixa": 2, 
    "normal": 1
    }

PERIODOS = {
    "24h": timedelta(hours=24),
    "7d":  timedelta(days=7),
    "30d": timedelta(days=30),
}

PESOS_COMPONENTES = {
    "CPU": 0.8,
    "RAM": 1.0,
    "DISCO": 1.3
}

CPU_CRITICA = 80
CPU_ALERTA = 50

RAM_CRITICA_PERCENT = 20
RAM_ALERTA_PERCENT = 10

LATENCIA_CRITICA = 100
LATENCIA_ALERTA = 50


def coletar_mac():

    mac = get_mac_address()

    if not mac:
        raise Exception("MAC Address não encontrado.")

    return (mac.lower().replace("-", ":"))

#Informações da S3
def get_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_CONFIG["aws_access_key_id"],
        aws_secret_access_key=AWS_CONFIG["aws_secret_access_key"],
        aws_session_token=AWS_CONFIG["aws_session_token"],
        region_name=AWS_CONFIG["region_name"]
    )

s3 = get_s3()


def ler_csv_s3(key):
    obj = s3.get_object(Bucket=AWS_CONFIG["bucket_name"],
                         Key=key)
    
    conteudo = obj['Body'].read().decode('utf-8')

    df = pandas.read_csv(StringIO(conteudo), on_bad_lines='skip')
    return df

def salvar_s3(conteudo, key):    
    s3.put_object(
        Bucket=AWS_CONFIG["bucket_name"],
        Key=key,
        Body=conteudo
    )

def listar_arquivos_client(s3, empresa_id):

    if USAR_LOCAL:

        base = normalizar_path(
            f"client/empresa_{empresa_id}"
        )

        arquivos = []

        for root, _, files in os.walk(base):

            for file in files:

                if file.endswith(".json"):

                    caminho = os.path.join(root, file)

                    arquivos.append(caminho)

        return arquivos

    prefix = f"client/empresa_{empresa_id}/"
    
    res = s3.list_objects_v2(
        Bucket=AWS_CONFIG["bucket_name"],
        Prefix=prefix
    )

    return [
        obj["Key"]
        for obj in res.get("Contents", [])
        if obj["Key"].endswith(".json")
    ]


def ler_json_s3(s3, key):

    if USAR_LOCAL:

        caminho = normalizar_path(key)

        if not os.path.exists(caminho):
            print(f"JSON não encontrado: {caminho}")
            return []

        with open(caminho, "r", encoding="utf-8") as f:
            return json.load(f)
        
    obj = s3.get_object(Bucket=AWS_CONFIG["bucket_name"], Key=key)
    content = obj["Body"].read().decode("utf-8")
    return json.loads(content)

#MySQL

def get_db():
    return mysql.connector.connect(**DB_CONFIG)

def obter_empresas():
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id_empresa, razao_social FROM empresa")
    rows = cursor.fetchall()
    
    cursor.close(); 
    conn.close()
    return rows

def obter_servidores_empresa(id_empresa):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT id_servidor, hostname, status_servidor
        FROM servidor
        WHERE fk_empresa = %s
    """, (id_empresa,))
    rows = cursor.fetchall()
    cursor.close(); conn.close()
    return rows

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

def obter_analistas_por_servidor(empresa_id):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT sa.fk_servidor AS servidor_id, COUNT(*) AS total_analistas
        FROM acesso_servidor sa
        JOIN servidor s ON sa.fk_servidor = s.id_servidor
        WHERE s.fk_empresa = %s
        GROUP BY sa.fk_servidor
    """, (empresa_id,))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {r["servidor_id"]: r["total_analistas"] for r in rows}


def obter_limites_servidor(servidor_id, dict_cursor=False):
    conn = None
    cursor = None

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=dict_cursor)

        cursor.execute("""
            SELECT c.tipo, sc.limite
            FROM servidor_componente sc
            JOIN componente c ON sc.fk_componente = c.id_componente
            WHERE sc.fk_servidor = %s
        """, (servidor_id,))

        rows = cursor.fetchall()

        if dict_cursor:
            return {r["tipo"]: float(r["limite"]) for r in rows}
        else:
            return {r[0]: float(r[1]) for r in rows}

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

#CPU/RAM/DISCO
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

#Limpeza de Dados para o Trusted
def limpar_dados(df):
    colunas_numericas = [
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
        "total_aeronaves",
        "avg_adsb_update_seconds"
    ]

    for coluna in colunas_numericas:
        if coluna in df.columns:
            df[coluna] = pandas.to_numeric(df[coluna], errors="coerce")
       
    df[colunas_numericas] = df[colunas_numericas].fillna(0)
    df["timestamp"] = pandas.to_datetime(df["timestamp"])
    df["label_24h"] = df["timestamp"].dt.strftime("%H:%M")
    df["label_3d"] = df["timestamp"].dt.strftime("%d/%m %Hh")
    df["label_7d"] = df["timestamp"].dt.strftime("%d/%m")
    df["opensky_timestamp"] = pandas.to_datetime(df["opensky_timestamp"]) 
    

    return df

def limpar_voos(df):

    df["timestamp_coleta"] = pandas.to_datetime(
        df["timestamp_coleta"]
    )

    df["delay_origem"] = pandas.to_numeric(
        df["delay_origem"],
        errors="coerce"
    )

    df["delay_destino"] = pandas.to_numeric(
        df["delay_destino"],
        errors="coerce"
    )

    df["delay_origem"] = df["delay_origem"].fillna(0)
    df["delay_destino"] = df["delay_destino"].fillna(0)

    df["origem"] = df["origem"].str.strip()  #Remover quebra de linha
    df["destino"] = df["destino"].str.strip()

    df = df.dropna(subset=["numero_voo"])

    return df

#Atualização de Status no Banco de Dados
def determinar_status_servidor(severidades):
    prioridade = SEVERIDADE

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

#Calculo de score do servidor
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

    obter_limites_bd = obter_limites_servidor(1)

    limite_cpu = obter_limites_bd.get("CPU")
    limite_ram = obter_limites_bd.get("RAM")
    limite_disco = obter_limites_bd.get("DISCO")
    limite_adsb = 10

    df = df.copy()

    df[coluna_tempo] = pandas.to_datetime(df[coluna_tempo])

    if hostname is not None:
        df = df[df["hostname"] == hostname]
        
    agora = df[coluna_tempo].max()

    periodos_alerta = {
        "1h": pandas.Timedelta(hours=1),
        "12h": pandas.Timedelta(hours=12),
        "24h": pandas.Timedelta(hours=24),
        "7d": pandas.Timedelta(days=7)
    }

    resultados = {}

    for nome_periodo, delta in periodos_alerta.items():

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

#Classificação e tratamento de dados
def classificar_latencia(valor):
    valor = float(valor)

    if valor > 250:
        return "critico"
    elif valor > 200:
        return "alta"
    elif valor > 150:
        return "media"
    elif valor > 100:
        return "baixa"
    else:
        return "normal"
    
def severidade_servidor_latencia(linha):
    status = [
    linha["status_latency_avg"],
    linha["status_adsb"],
    linha["status_correlacao_rotas"],
    linha["status_rotas_api"],
    linha["status_api_bd"],
    linha["status_bd_sync"]
]

    prioridade = SEVERIDADE

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
        linha["status_packet_loss"],
        linha["status_rastreamento_loss"],
        linha["status_correlacao_loss"],
        linha["status_rotas_loss"],
        linha["status_api_loss"],
        linha["status_bd_loss"],
        linha["status_sync_loss"]
    ]

    prioridade = SEVERIDADE

    return max(status, key=lambda x: prioridade[x])

def enriquecer_dados(df): #classifica cada dado e acrescenta uma coluna extra ao df

    df["status_packet_loss"] = df["packet_loss_internet"].apply(classificar_pacotes)
    df["status_rastreamento_loss"] = df["rastreamento_loss"].apply(classificar_pacotes)
    df["status_correlacao_loss"] = df["correlacao_loss"].apply(classificar_pacotes)
    df["status_rotas_loss"] = df["rotas_loss"].apply(classificar_pacotes)
    df["status_api_loss"] = df["api_loss"].apply(classificar_pacotes)
    df["status_bd_loss"] = df["bd_loss"].apply(classificar_pacotes)
    df["status_sync_loss"] = df["sync_loss"].apply(classificar_pacotes)
    df["media_loss_servicos"] = df[
                                    [
                                        "rastreamento_loss",
                                        "correlacao_loss",
                                        "rotas_loss",
                                        "api_loss",
                                        "bd_loss",
                                        "sync_loss"
                                    ]].mean(axis=1)

    df["status_servidor_pacotes"] = df.apply(
    severidade_servidor_pacotes,
    axis=1
    )

    df["status_latency_avg"] = df["latency_avg_ms"].apply(classificar_latencia)
    df["status_adsb"] = df["lat_adsb_rastreamento"].apply(classificar_latencia)
    df["status_api_bd"] = df["lat_api_bd"].apply(classificar_latencia)
    df["status_bd_sync"] = df["lat_bd_sync"].apply(classificar_latencia)
    df["status_correlacao_rotas"] = df["lat_correlacao_rotas"].apply(classificar_latencia)
    df["status_rotas_api"] = df["lat_rotas_api"].apply(classificar_latencia)

    df["status_servidor_latencia"] = df.apply(
        severidade_servidor_latencia,
        axis=1
    )

    return df

def agrupar_periodo(df, periodo, coluna_tempo="timestamp"):

    df = df.copy()

    if periodo == "24h":
        df["grupo"] = df[coluna_tempo].dt.strftime("%H:%M")

    elif periodo == "3d":
        df["grupo"] = df[coluna_tempo].dt.strftime("%d/%m %Hh")

    else:
        df["grupo"] = df[coluna_tempo].dt.strftime("%d/%m")

    colunas_agregacao = [
        # Banda
        "rastreamento_mbps",
        "rotas_mbps",
        "correlacao_mbps",
        "api_gateway_mbps",
        "bd_mbps",
        "sync_service_mbps",

        # Latência
        "latency_avg_ms",
        "lat_adsb_rastreamento",
        "lat_rastreamento_correlacao",
        "lat_correlacao_rotas",
        "lat_rotas_api",
        "lat_api_bd",
        "lat_bd_sync",

        # Packet loss
        "packet_loss_internet",
        "rastreamento_loss",
        "rotas_loss",
        "correlacao_loss",
        "api_loss",
        "bd_loss",
        "sync_loss",

        # ADS-B
        "avg_adsb_update_seconds"
    ]

    colunas_existentes = {
        coluna: "mean"
        for coluna in colunas_agregacao
        if coluna in df.columns
    }

    agrupado = (
        df.groupby("grupo")
        .agg(colunas_existentes)
        .reset_index()
    )

    return agrupado

#KPIs
def perda_pacotes_servico(df):
    return {
        "Rastreamento": round(df["rastreamento_loss"].mean(), 2),
        "Rotas": round(df["rotas_loss"].mean(), 2),
        "Correlação": round(df["correlacao_loss"].mean(), 2),
        "API Gateway": round(df["api_loss"].mean(), 2),
        "Banco de Dados": round(df["bd_loss"].mean(), 2),
        "Sync Service": round(df["sync_loss"].mean(), 2)
    }

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
        return round(100 - media * 2, 1)  #2s é tolerável e alcança no mínimo 96%

    indice = 96 * math.exp(-(media-2)/20) #Degradação exponencial a partir de 96%. 

    return round(max(indice, 0), 1) #max impede números negativos ao retornar o número maior entre indice e 0

def rotas_sem_atualizacao(df_voos):
    agrupado = df_voos.groupby(["numero_voo", "origem", "destino", "status"]).size()

    rotas_paradas = agrupado[agrupado >= 3]

    return len(rotas_paradas)

def taxa_transferencia(df):
    taxa_total_mb = (
        df["bytes_recv"] + df["bytes_sent"]
    ) / (1024 * 1024)

    return taxa_total_mb.mean()


def consumo_banda_servico(df):
    return {
        "Rastreamento": round(df["rastreamento_mbps"].mean(), 2),
        "Rotas": round(df["rotas_mbps"].mean(), 2),
        "Correlacao": round(df["correlacao_mbps"].mean(), 2),
        "API Gateway": round(df["api_gateway_mbps"].mean(), 2),
        "Banco de Dados": round(df["bd_mbps"].mean(), 2),
        "Sync Service": round(df["sync_service_mbps"].mean(), 2)
    }

def detectar_incidentes(df):

    incidentes = []

    if df.empty:
        return incidentes

    ultima = df.iloc[-1]

    hostname = ultima.get(
        "hostname",
        "desconhecido"
    )

    componentes_criticos = 0

    # =========================
    # LATÊNCIA
    # =========================

    severidade_latencia = ultima[
        "status_servidor_latencia"
    ]

    if severidade_latencia in [
        "critico",
        "alto"
    ]:

        if severidade_latencia == "critico":
            componentes_criticos += 1

        incidentes.append({

            # compatibilidade Java
            "titulo": "Latência elevada no servidor",
            "criticidade": severidade_latencia,
            "servidor": hostname,
            "componente": "rede",

            # extras úteis
            "tipo": "latencia",
            "valor": float(
                ultima["latency_avg_ms"]
            ),
            "timestamp": str(
                ultima["timestamp"]
            )
        })

    # =========================
    # PACKET LOSS
    # =========================

    severidade_pacotes = ultima[
        "status_servidor_pacotes"
    ]

    if severidade_pacotes in [
        "critico",
        "alto"
    ]:

        if severidade_pacotes == "critico":
            componentes_criticos += 1

        incidentes.append({

            "titulo": "Perda de pacotes elevada",
            "criticidade": severidade_pacotes,
            "servidor": hostname,
            "componente": "rede",

            "tipo": "packet_loss",
            "valor": float(
                ultima["packet_loss_internet"]
            ),
            "timestamp": str(
                ultima["timestamp"]
            )
        })

    # =========================
    # ADS-B
    # =========================

    if ultima[
        "avg_adsb_update_seconds"
    ] > 10:

        componentes_criticos += 1

        incidentes.append({

            "titulo": "Delay elevado no ADS-B",
            "criticidade": "critico",
            "servidor": hostname,
            "componente": "rede",

            "tipo": "adsb",
            "valor": float(
                ultima[
                    "avg_adsb_update_seconds"
                ]
            ),
            "timestamp": str(
                ultima["timestamp"]
            )
        })

    # =========================
    # INCIDENTE SISTÊMICO
    # =========================

    if componentes_criticos >= 2:

        incidentes.append({

            "titulo": "Múltiplos componentes críticos na rede",
            "criticidade": "critico",
            "servidor": hostname,
            "componente": "rede",

            "tipo": "infraestrutura",
            "valor": componentes_criticos,
            "timestamp": str(
                ultima["timestamp"]
            )
        })

    return incidentes

#Processos

def processos_tratados_s3(df_raw):

    dados_tratados = []

    for linha in df_raw.to_dict(orient="records"):

        cpu = float(linha["cpu"])
        ram_percent = float(linha["ram_percent"])
        latencia = float(linha["latencia_ms"])

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

        dados_tratados.append(processo_tratado)

    return pandas.DataFrame(dados_tratados)

def processos_criticidade(cpu, ram_percent, latencia):

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


def top5cpu(dfProcessos):

    dfTop5 = dfProcessos.sort_values(
        'cpu',
        ascending=False
    )

    cpu5 = {}

    for i in range (5):
        cpu5[f"nome_cpu_{i+1}"] = dfTop5["nome"].iloc[i]
        cpu5[f"cpu_{i+1}"] = float(dfTop5["cpu"].iloc[i])
    return cpu5


def top5ram(dfProcessos):

    dfTop5 = dfProcessos.sort_values(
        'ram_percent',
        ascending=False
    )

    ram5 = {}

    for i in range (5):
        ram5[f"nome_ram_{i+1}"] = dfTop5["nome"].iloc[i]
        ram5[f"ram_{i+1}"] = float(dfTop5["ram_percent"].iloc[i])

    return ram5


def processos_criticos(df_processos):
    # Como True vale 1 e False vale 0, a soma dá o total de acertos
    total_criticos = (df_processos['criticidade'] == 'Crítico').sum()
    return {"totalCriticos": int(total_criticos)}


def maior_latencia(df):
    if df.empty:
        return {
            "nome": None,
            "latencia_ms": 0,
            "pid": None
        }

    linha_maior = df.loc[df["latencia_ms"].idxmax()]

    maior_latencia = {
        "nome": linha_maior["nome"],
        "latencia_ms": float(linha_maior["latencia_ms"]),
        "pid": int(linha_maior["pid"])
    }
    return maior_latencia

def limites_processos(processos_tratados):
    return {"limite": len(processos_tratados) * 0.30}

def gerar_raw_criticos_4h(df):

    if df.empty:
        return {
            "atual": 0,
            "0-2h59min": 0,
            "3-5h59min": 0,
            "6-8h59min": 0,
            "9-11h59min": 0,
            "12-14h59min": 0,
            "15-17h59min": 0,
            "18-20h59min": 0,
            "21-23h59min": 0
        }

    df = df.copy()

    # timestamp
    df["timestamp"] = pandas.to_datetime(df["timestamp"])

    agora = pandas.Timestamp.now()

    # diferença em horas
    df["horas_atras"] = (
        (agora - df["timestamp"])
        .dt.total_seconds() / 3600
    )

    # apenas críticos
    df_criticos = df[
        df["criticidade"] == "Crítico"
    ]

    def bucket(horas):

        if horas < (5 / 60):
            return "atual"

        elif horas < 3:
            return "0-2h59min"

        elif horas < 6:
            return "3-5h59min"

        elif horas < 9:
            return "6-8h59min"

        elif horas < 12:
            return "9-11h59min"

        elif horas < 15:
            return "12-14h59min"

        elif horas < 18:
            return "15-17h59min"

        elif horas < 21:
            return "18-20h59min"

        else:
            return "21-23h59min"

    df_criticos["bloco"] = (
        df_criticos["horas_atras"]
        .apply(bucket)
    )

    resultado = (
        df_criticos["bloco"]
        .value_counts()
        .to_dict()
    )

    blocos = [
        "atual",
        "0-2h59min",
        "3-5h59min",
        "6-8h59min",
        "9-11h59min",
        "12-14h59min",
        "15-17h59min",
        "18-20h59min",
        "21-23h59min"
    ]

    return {
        bloco: resultado.get(bloco, 0)
        for bloco in blocos
    }

def contar_status(df):

    contagem = (
        df["status"]
        .str.lower()
        .value_counts()
        .to_dict()
    )

    return {
        "running": contagem.get("running", 0),
        "sleeping": contagem.get("sleeping", 0),
        "stopped": contagem.get("stopped", 0)
    }

def contar_criticos(df):

    criticos = df[
        df["criticidade"] == "Crítico"
    ]

    cpu = (criticos["cpu"] >= CPU_CRITICA).sum()

    ram = (
        criticos["ram_percent"] > RAM_CRITICA_PERCENT
    ).sum()

    latencia = (
        criticos["latencia_ms"] > LATENCIA_CRITICA
    ).sum()

    return {
        "cpu": int(cpu),
        "ram": int(ram),
        "latencia": int(latencia),
        "total": int(cpu + ram + latencia)
    }

#Temperatura 
def gerar_alerta(row):

    alertas = []

    status_temp = str(
        row.get(
            "status_temperatura",
            ""
        )
    ).lower()

    status_margem = str(
        row.get(
            "status_margem",
            ""
        )
    ).lower()

    status_resfriamento = str(
        row.get(
            "status_resfriamento",
            ""
        )
    ).lower()

    throttling = str(
        row.get(
            "throttling",
            ""
        )
    ).lower()

    # TEMPERATURA

    if status_temp in [
        "critical",
        "medium",
        "alert"
    ]:

        alertas.append(
            "Temperatura CPU elevada"
        )

    # MARGEM

    if status_margem in [
        "critica",
        "atencao",
        "throttling"
    ]:

        alertas.append(
            "Margem térmica crítica"
        )

    # RESFRIAMENTO

    if status_resfriamento in [
        "critica",
        "atencao"
    ]:

        alertas.append(
            "Resfriamento ineficiente"
        )

    # THROTTLING

    if throttling in ["sim", "true", "1"]:

        alertas.append(
            "CPU em throttling"
        )

    return " | ".join(alertas)

# DIA COM MAIS ALERTAS
def calcular_dia_mais_alertas(df):

    df_alertas = df[
        df["alertas"] != ""
    ]

    if df_alertas.empty:

        return None

    dias = pandas.to_datetime(
        df_alertas["timestamp"]
    ).dt.strftime("%A")

    contador = Counter(dias)

    return contador.most_common(1)[0][0]

#Gestor

def filtrar_periodo(leituras, periodo):

    delta = PERIODOS[periodo]
    corte = datetime.now() - delta

    filtradas = []

    for r in leituras:
        if not isinstance(r, dict):
            continue
        data_str = r.get("data_hora")

        if not data_str:
            continue
        try:
            data = pandas.to_datetime(data_str)
        except:
            continue

        if data >= corte:
            filtradas.append(r)

    return filtradas

def classificar(valor, limite):
    if limite == 0:
        return "normal"
    if valor == 0 and limite > 0:
        return "crítico"
    
    razao = valor / limite

    if razao >= 1.0:
        return "crítico"
    if razao >= 0.90:
        return "alta"
    if razao >= 0.80:
        return "média"
    if razao >= 0.70:
        return "baixa"
    return "normal"

def calcular_disponibilidade(leituras, limites):
    online = 0

    for r in leituras:
        servidor = r["servidor_id"]
        m = r["metricas"]

        cpu = classificar(m["cpu"], limites[servidor]["CPU"])
        ram = classificar(m["ram"], limites[servidor]["RAM"])
        disco = classificar(m["disco"], limites[servidor]["DISCO"])

        if cpu != "crítico" and ram != "crítico" and disco != "crítico":
            online += 1

        if not leituras:
            return 0

    return (online / len(leituras)) * 100

def calcular_nivel_risco(leituras, limites):
    total = 0
    quantidade = 0

    for r in leituras:
        servidor = r["servidor_id"]
        m = r["metricas"]

        cpu = classificar(m["cpu"], limites[servidor]["CPU"])
        ram = classificar(m["ram"], limites[servidor]["RAM"])
        disco = classificar(m["disco"], limites[servidor]["DISCO"])

        total += SEVERIDADE[cpu]
        total += SEVERIDADE[ram]
        total += SEVERIDADE[disco]

        quantidade += 3

    if quantidade > 0:
        media = total / quantidade
        return ((media - 1) / 4) * 100
    
    else:
        return 0

def calcular_incidentes_criticos(leituras, limites):
    criticos = 0

    for r in leituras:
        s = r["servidor_id"]
        m = r["metricas"]

        cpu = classificar(m["cpu"], limites[s]["CPU"])
        ram = classificar(m["ram"], limites[s]["RAM"])
        disco = classificar(m["disco"], limites[s]["DISCO"])

        if (
            cpu == "crítico" or
            ram == "crítico" or
            disco == "crítico"
        ):
            criticos += 1

    return criticos

def calcular_estabilidade_operacional(leituras, limites):
    estaveis = 0

    for r in leituras:
        s = r["servidor_id"]
        m = r["metricas"]

        cpu = m["cpu"] / limites[s]["CPU"]
        ram = m["ram"] / limites[s]["RAM"]
        disco = m["disco"] / limites[s]["DISCO"]

        if cpu < 0.80 and ram < 0.80 and disco < 0.80:
            estaveis += 1

    return (estaveis / len(leituras)) * 100

# def calcular_mttr(leituras, limites):

def calcular_tendencia(leituras, limites):

    agora = datetime.now()

    atual = []
    anterior = []

    for r in leituras:
        horario = datetime.strptime(
            r["data_hora"],
            "%Y-%m-%d %H:%M:%S"
        )

        if horario >= agora - timedelta(hours=1):
            atual.append(r)

        elif horario >= agora - timedelta(hours=2):
            anterior.append(r)

    risco_atual = calcular_nivel_risco(atual, limites)
    risco_anterior = calcular_nivel_risco(anterior, limites)

    if risco_atual > risco_anterior:
        return "Subindo"

    elif risco_atual < risco_anterior:
        return "Caindo"

    else:
        return "Estável"

def grafico_estabilidade(leituras, limites):
    valores = []
    grupos = {}
    labels = []

    for r in leituras:
        hora = r["data_hora"][:13]

        if hora not in grupos:
            grupos[hora] = []

        grupos[hora].append(r)

    for hora in sorted(grupos.keys()):
        estabilidade = calcular_estabilidade_operacional(grupos[hora], limites)
        labels.append(hora[11:] + ":00")
        valores.append(estabilidade)

    return {"labels": labels[-7:], "valores": valores[-7:]}

def gerar_mensagem(metrica, nivel, previsao, limite):
        pct = round(previsao / limite * 100, 1)

        mensagens = {
            "CPU": {
                "baixa": f"CPU prevista em {pct}% do limite - Leve aumento na carga de processamento. Monitore a tendência.",
                "média": f"CPU prevista em {pct}% do limite - Carga de processamento em elevação. Verifique rotinas de cálculo de rotas e separação de aeronaves em execução.",
                "alta": f"CPU prevista em {pct}% do limite - Processamento de dados de radar pode ser impactado. Considere redistribuir carga entre os nós do Sagitário.",
                "crítico": f"CPU prevista em {pct}% do limite - Risco de atraso no processamento de dados de voo. Notifique um analista responsável imediatamente."
            },
            "RAM": {
                "baixa": f"RAM prevista em {pct}% do limite - Leve crescimento no consumo de memória. Monitore a tendência.",
                "média": f"RAM prevista em {pct}% do limite - Consumo de memória crescente. Verifique buffers de dados de radar e faixas de voo ativas.",
                "alta": f"RAM prevista em {pct}% do limite - Risco de degradação no gerenciamento de planos de voo. Verifique processos de correlação de pistas.",
                "crítico": f"RAM prevista em {pct}% do limite - Risco de falha no rastreamento de aeronaves. Reinicie processos não essenciais e acione o sistema de contingência do Sagitário."
            },
            "DISCO": {
                "baixa": f"Disco previsto em {pct}% do limite - Leve crescimento no uso de armazenamento. Monitore a tendência.",
                "média": f"Disco previsto em {pct}% do limite - Crescimento no volume de logs operacionais. Verifique retenção de gravações de voz e registros de radar.",
                "alta": f"Disco previsto em {pct}% do limite - Armazenamento de dados de voo pode ser comprometido. Realize purga de arquivos temporários e logs antigos.",
                "crítico": f"Disco previsto em {pct}% do limite - Risco de interrupção no registro de dados operacionais. Arquive ou remova gravações antigas imediatamente e acione o suporte técnico."
            }
        }

        return mensagens[metrica][nivel]

JANELA_PREVISAO = 12

def calcular_previsao_falhas(leituras, limites):
    por_servidor = {}

    for r in sorted(leituras, key=lambda r: r["data_hora"]):
        servidor = r["servidor_id"]
        if servidor not in por_servidor:
            por_servidor[servidor] = {"cpu": [], "ram": [], "disco": []}

        por_servidor[servidor]["cpu"].append(r["metricas"]["cpu"])
        por_servidor[servidor]["ram"].append(r["metricas"]["ram"])
        por_servidor[servidor]["disco"].append(r["metricas"]["disco"])

    alertas_previsao = []

    for servidor_id, series in por_servidor.items():
        for metrica in ["cpu", "ram", "disco"]:
            valores = series[metrica]
            
            if len(valores) < 3:
                continue

            valores_recentes = valores[-JANELA_PREVISAO:]
            limite = limites[servidor_id][metrica.upper()]

            # mudança pra gerar alerta se o estado ja estiver crítico ou alto
            nivel_atual = classificar(valores_recentes[-1], limite)
            if nivel_atual in ["crítico", "alta"]:
                alertas_previsao.append({
                    "servidor_id": servidor_id,
                    "metrica": metrica.upper(),
                    "nivel_previsao": nivel_atual,
                    "mensagem": gerar_mensagem(metrica.upper(), nivel_atual, valores_recentes[-1], limite)
                })
                continue
            # fim da alteração
            x = np.arange(len(valores_recentes))
            a, b = np.polyfit(x, valores_recentes, 1)
            previsao = a * len(valores_recentes) + b

            atual = valores_recentes[-1] / limite
            nivel_previsao = previsao / limite

            if a > 0 and nivel_previsao > 0.60 and nivel_previsao > atual:
                nivel_classificado = classificar(previsao, limite)

                if nivel_classificado == "normal":
                    continue

                alertas_previsao.append({
                    "servidor_id": servidor_id,
                    "metrica": metrica.upper(),
                    "nivel_previsao": nivel_classificado,
                    "mensagem": gerar_mensagem(metrica.upper(), nivel_classificado, previsao, limite)
                })

    return alertas_previsao

def calcular_impacto_componente(leituras, limites):

    por_servidor = {}

    # Agrupa impactos por servidor
    for r in leituras:
        servidor = r["servidor_id"]

        if servidor not in por_servidor:
            por_servidor[servidor] = {
                "cpu": [],
                "ram": [],
                "disco": []
            }

        metricas = r["metricas"]

        impacto_cpu = min(
            (metricas["cpu"] / limites[servidor]["CPU"]) * 100,
            100
        )

        impacto_ram = min(
            (metricas["ram"] / limites[servidor]["RAM"]) * 100,
            100
        )

        impacto_disco = min(
            (metricas["disco"] / limites[servidor]["DISCO"]) * 100,
            100
        )

        por_servidor[servidor]["cpu"].append(impacto_cpu)
        por_servidor[servidor]["ram"].append(impacto_ram)
        por_servidor[servidor]["disco"].append(impacto_disco)

    # Médias por componente
    medias = {
        "CPU": [],
        "RAM": [],
        "DISCO": []
    }

    for _, dados in por_servidor.items():

        medias["CPU"].append(
            sum(dados["cpu"]) / len(dados["cpu"])
        )

        medias["RAM"].append(
            sum(dados["ram"]) / len(dados["ram"])
        )

        medias["DISCO"].append(
            sum(dados["disco"]) / len(dados["disco"])
        )

    # Média final global
    cpu_final = round(
        sum(medias["CPU"]) / len(medias["CPU"]), 1
    ) if medias["CPU"] else 0

    ram_final = round(
        sum(medias["RAM"]) / len(medias["RAM"]), 1
    ) if medias["RAM"] else 0

    disco_final = round(
        sum(medias["DISCO"]) / len(medias["DISCO"]), 1
    ) if medias["DISCO"] else 0

    # Faixa de severidade
    def faixa_severidade(valor):

        if valor >= 80:
            return "crítico"

        elif valor >= 60:
            return "alto"

        elif valor >= 40:
            return "moderado"

        return "baixo"

    return {
        "CPU": {
            "valor": cpu_final,
            "severidade": faixa_severidade(cpu_final)
        },

        "RAM": {
            "valor": ram_final,
            "severidade": faixa_severidade(ram_final)
        },

        "DISCO": {
            "valor": disco_final,
            "severidade": faixa_severidade(disco_final)
        }
    }

def listar_info_servidores(leituras, limites, servidores, analistas):
    resultado = []

    for srv in servidores:
        sid = srv["id_servidor"]
        incidentes = 0

        for r in leituras:
            if r["servidor_id"] != sid:
                continue

            m = r["metricas"]

            if (
                m["cpu"] >= limites[sid]["CPU"]
                or
                m["ram"] >= limites[sid]["RAM"]
                or
                m["disco"] >= limites[sid]["DISCO"]
            ):
                incidentes += 1

        qtd_analistas = analistas.get(sid, 0)

        status = srv["status_servidor"]

        resultado.append({
            "servidor": srv["hostname"],
            "incidentes": incidentes,
            "analistas": qtd_analistas,
            "status": status
        })

    return resultado

#Geração de JSON para o Client
def gerar_json_dashboard(df_network, df_flights, periodo):

    df_agrupado = agrupar_periodo(
        df_network,
        periodo,
        "timestamp"
        )

    dashboard = {

        "periodo": periodo,

        "kpis": {
            "perda_pacotes": round(kpi_perda_media(df_agrupado), 2),
            "latencia_media": round(kpi_latencia_media(df_agrupado), 2),
            "adsb_update": kpi_adsb_update(df_agrupado),
            "rotas_sem_atualizacao": rotas_sem_atualizacao(df_flights)
        },

        "grafico_transferencia": {
            "labels": df_agrupado["grupo"].tolist(),
            "rastreamento": df_agrupado["rastreamento_mbps"].round(2).tolist(),
            "rotas": df_agrupado["rotas_mbps"].round(2).tolist(),
            "correlacao": df_agrupado["correlacao_mbps"].round(2).tolist()
        },

        "grafico_latencia_componentes": {
            "ADS-B": round(df_agrupado["lat_adsb_rastreamento"].mean(), 2),
            "Correlação": round(df_agrupado["lat_rastreamento_correlacao"].mean(), 2),
            "Rotas": round(df_agrupado["lat_rotas_api"].mean(), 2),
            "Banco de Dados": round(df_agrupado["lat_api_bd"].mean(), 2),
            "Sync Service": round(df_agrupado["lat_bd_sync"].mean(), 2)
        },

        "consumo_banda": consumo_banda_servico(df_agrupado),

        "perda_pacotes_servico": perda_pacotes_servico(df_agrupado)
    }

    return dashboard

def normalizar_path(path):
    return path.replace(":", "_")

def salvar_s3_unificado(s3, key, data, formato="json", bucket=None):
    if USAR_LOCAL:

        caminho = normalizar_path(key.replace("/", os.sep))

        pasta = os.path.dirname(caminho)

        os.makedirs(pasta, exist_ok=True)

        if formato == "csv":

            data.to_csv(caminho, index=False)

        else:

            with open(caminho, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

        print(f"Arquivo salvo localmente: {caminho}")

        return

    bucket = bucket or AWS_CONFIG["bucket_name"]

    if formato == "json":
        body = json.dumps(data, indent=2, ensure_ascii=False)
        content_type = "application/json"

    elif formato == "json_dashboard":
        body = json.dumps(data, indent=4, ensure_ascii=False)
        content_type = "application/json"

    elif formato == "csv":
        buffer = StringIO()
        data.to_csv(buffer, index=False)
        body = buffer.getvalue()
        content_type = "text/csv"

    else:
        raise ValueError(f"Formato não suportado: {formato}")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type
    )

def safe_float(value):
    if value is None:
        return 0.0

    if isinstance(value, str):
        value = value.replace("%", "").strip()

    try:
        return float(value)
    except:
        return 0.0

def obter_limites_batch(servidor_ids):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    format_strings = ','.join(['%s'] * len(servidor_ids))

    query = f"""
        SELECT sc.fk_servidor, c.tipo, sc.limite
        FROM servidor_componente sc
        JOIN componente c ON sc.fk_componente = c.id_componente
        WHERE sc.fk_servidor IN ({format_strings})
    """

    cursor.execute(query, servidor_ids)
    rows = cursor.fetchall()

    cursor.close(); conn.close()

    limites = {}
    for r in rows:
        sid = r["fk_servidor"]
        limites.setdefault(sid, {})[r["tipo"]] = float(r["limite"])

    return limites

def obter_servidor(mac_address):

    conn = mysql.connector.connect(
        **DB_CONFIG
    )

    cursor = conn.cursor(
        dictionary=True
    )

    query = """
        SELECT
            id_servidor,
            fk_empresa,
            hostname
        FROM servidor
        WHERE LOWER(mac_address) = LOWER(%s)
    """

    cursor.execute(
        query,
        (mac_address,)
    )

    resultado = cursor.fetchone()

    cursor.close()
    conn.close()

    return resultado

def main():

    print("Iniciando pipeline unificado...")

    s3 = get_s3()
    agora = pandas.Timestamp.now()

    mac_address = coletar_mac()

    servidor = obter_servidor(mac_address)

    if not servidor:
        print("Servidor não encontrado.")
        return

    EMPRESA_ID = servidor["fk_empresa"]
    SERVIDOR_ID = servidor["id_servidor"]
    HOSTNAME = servidor["hostname"]

    # =========================================================
    # PIPELINE SCORE SERVIDORES
    # =========================================================
    print("\n=== PIPELINE SCORE SERVIDOR ===")

    raw_key_score = f"raw/raw.csv"


    df_score = ler_csv(raw_key_score)

    if df_score.empty:
        print("CSV vazio para score.")

    servidores = df_score["hostname"].unique().tolist()

    hostname = servidores[0]

    client_score_json = calcular_score_servidor(df_score, hostname)

    salvar_s3_unificado(
        s3,
        f"client/empresa_{EMPRESA_ID}/{mac_address}/calcularIndice.json",
        client_score_json,
        formato="json_dashboard"
    )

    print("Score de servidores gerado com sucesso.")


    # =========================================================
    # PIPELINE TEMPERATURA
    # =========================================================
    print("\n=== PIPELINE TEMPERATURA ===")

    raw_key_temp = f"raw/raw.csv"

    df_temp = ler_csv(raw_key_temp)

    if df_temp.empty:
        print("CSV temperatura vazio.")
        return

    # ALERTAS
    df_temp["alertas"] = df_temp.apply(gerar_alerta, axis=1)

    df_temp["quantidade_alertas"] = df_temp["alertas"].apply(
        lambda x: len([a for a in x.split("|") if a.strip()]) if x else 0
    )

    dia_mais_alertas = calcular_dia_mais_alertas(df_temp)

    # TRUSTED
    trusted_df = df_temp.copy()

    # CLIENT JSON
    client_temp_json = {
        "empresa_id": EMPRESA_ID,
        "servidor_id": SERVIDOR_ID,
        "hostname": HOSTNAME,
        "mac_address": mac_address,
        "dia_com_mais_alertas": dia_mais_alertas,
        "total_registros": int(len(df_temp)),
        "total_alertas": int(df_temp["quantidade_alertas"].sum()),
        "dados": df_temp.to_dict(orient="records")
    }

    # SALVAMENTO
    salvar_s3_unificado(
        s3,
        f"trusted/empresa_{EMPRESA_ID}/{mac_address}/temperatura_trusted.csv",
        trusted_df,
        formato="csv"
    )

    salvar_s3_unificado(
        s3,
        f"client/empresa_{EMPRESA_ID}/{mac_address}/client_metrics.json",
        client_temp_json,
        formato="json_dashboard"
    )

    print("Temperatura processada com sucesso.")

    print("\n=== PIPELINE PROCESSOS ===")

    # ==================
    #Processos
    #====================
    raw_key_processos = f"raw/process_raw.csv"

    # 2. ler RAW
    df_raw = ler_csv(raw_key_processos)

    if df_raw.empty:
        print("CSV processos vazio.")
        return

    # 3. TRANSFORM (TRUSTED)
    dfProcessos = processos_tratados_s3(df_raw)
    raw_criticos_4h = gerar_raw_criticos_4h(dfProcessos)

    # =========================
    # KPIs
    # =========================
    kpis = {}

    kpis.update(top5cpu(dfProcessos))
    kpis.update(top5ram(dfProcessos))
    kpis.update(processos_criticos(dfProcessos))
    kpis.update(maior_latencia(dfProcessos))
    kpis.update(limites_processos(dfProcessos))
    kpis.update(contar_status(dfProcessos))
    kpis.update(contar_criticos(dfProcessos))

    dfKPI = pandas.DataFrame([kpis])


    # =========================
    # CLIENT JSON
    # =========================
    client_processos_json = {
        "empresa_id": EMPRESA_ID,
        "servidor_id": SERVIDOR_ID,
        "hostname": HOSTNAME,
        "mac_address": mac_address,
        "total_processos": int(len(dfProcessos)),
        "processos_criticos": int(dfProcessos[dfProcessos["criticidade"] != "normal"].shape[0]),
        "maior_latencia": float(dfProcessos["latencia_ms"].max()),
        "kpis": kpis,
        "dados": dfProcessos.to_dict(orient="records")
    }

    # =========================
    # SALVAMENTO S3
    # =========================

    # TRUSTED
    salvar_s3_unificado(
        s3,
        f"trusted/empresa_{EMPRESA_ID}/{mac_address}/processos_trusted.csv",
        dfProcessos,
        formato="csv"
    )

    # CLIENT JSON
    salvar_s3_unificado(
        s3,
        f"client/empresa_{EMPRESA_ID}/{mac_address}/process_raw_kpis.json",
        client_processos_json,
        formato="json_dashboard"
    )

    salvar_s3_unificado(
        s3,
        f"client/empresa_{EMPRESA_ID}/{mac_address}/raw_criticos_4h.json",
        raw_criticos_4h,
        formato="json_dashboard"
    )

    print("Processos processados com sucesso.")
    # =========================================================
    # PIPELINE 1 - PROCESSAMENTO DE ARQUIVOS RAW (EVENTOS)
    # =========================================================
    print("\n=== PIPELINE RAW → CLIENT/ALERTAS ===")

    key = f"raw/raw.csv"

    df = ler_csv(key)

     

    if "servidor_id" in df.columns:
        col = "servidor_id"
    elif "id_servidor" in df.columns:
        col = "id_servidor"
    else:
        raise ValueError("Coluna de servidor não encontrada")

    trusted_rows = []
    client_json = []
    alertas = []
    historico_ram = defaultdict(list)
    alertas_por_dia = {}
    alertas_por_servidor = {}
    severidades_detectadas = []

    servidores = df[col].unique().tolist()
    limites_map = obter_limites_batch(servidores)

    base_path = key.replace("raw/", "")


    for row in df.itertuples(index=False):

        servidor_id = int(row.servidor_id)
        empresa_id = int(row.empresa_id)

        limites = limites_map.get(servidor_id, {})

        cpu = safe_float(row.cpu)
        ram = safe_float(row.ram)
        timestamp = datetime.strptime(row._asdict()["timestamp"], "%Y-%m-%d %H:%M:%S")
        historico_ram[servidor_id].append({
            "ram": ram,
            "timestamp": timestamp
        })

        disco = safe_float(row.disco)

        saude_cpu = 100 - cpu
        saude_ram = 100 - ram
        saude_disco = 100 - disco

        health_score = (
            saude_cpu * 0.40 +
            saude_ram * 0.40 +
            saude_disco * 0.20
        )

        criticos = sum(s < 40 for s in [saude_cpu, saude_ram, saude_disco])

        if criticos == 1:
            health_score -= 5
        elif criticos == 2:
            health_score -= 15
        elif criticos == 3:
            health_score -= 25

        health_score = round(max(0, min(100, health_score)), 2)

        status_health = (
            "estavel" if health_score >= 70 else
            "atencao" if health_score >= 40 else
            "critico"
        )

        # =========================
        # TRUSTED
        # =========================
        trusted_rows.append({
            **row._asdict(),
            "cpu": f"{cpu}%",
            "ram": f"{ram}%",
            "disco": f"{disco}%",
            "health_score": f"{health_score}%",
            "status_health": status_health
        })

        historico = historico_ram[servidor_id]

        linha_real = ram
        linha_tendencia = ram
        tendencia_hora = 0

        if len(historico) >= 2:

            ultimo = historico[-1]
            penultimo = historico[-2]

            ram_atual = ultimo["ram"]
            ram_anterior = penultimo["ram"]

            tempo_atual = ultimo["timestamp"]
            tempo_anterior = penultimo["timestamp"]

            diferenca_ram = ram_atual - ram_anterior
            diferenca_tempo = (tempo_atual - tempo_anterior).total_seconds() / 3600

            if diferenca_tempo > 0:

                tendencia_hora = min(max(diferenca_ram / max(diferenca_tempo, 0.1),-100),100)

                linha_tendencia = ram_atual + tendencia_hora

                linha_tendencia = round(
                    max(0, min(100, linha_tendencia)),
                    2
                )

                tendencia_hora = round(tendencia_hora, 2)
        # =========================
        # CLIENT JSON
        # =========================
        client_json.append({
            "data_hora": row.timestamp,
            "empresa_id": empresa_id,
            "servidor_id": servidor_id,
            "hostname": row.hostname,
            "ip": row.ip,
            "metricas": {
                "cpu": cpu,
                "ram": ram,
                "disco": disco,
                "health_score": health_score,
                "status_health": status_health,
                "linha_real": linha_real,
                "linha_tendencia": linha_tendencia,
                "tendencia_aumento_hora": tendencia_hora
            },
            "status_componentes": {
                "cpu": row.status_cpu,
                "ram": row.status_ram,
                "disco": row.status_disco
            }
        })

        # =========================
        # ALERTAS
        # =========================
        for componente, valor in [("CPU", cpu), ("RAM", ram), ("DISCO", disco)]:

            if componente in limites:

                limite = limites[componente]
                status, severidade = classificar_metrica(valor, limite)

                severidades_detectadas.append(severidade)

                if severidade != "normal":

                    alertas.append({
                        "data_hora": row.timestamp,
                        "empresa": empresa_id,
                        "servidor": servidor_id,
                        "componente": componente,
                        "limite": limite,
                        "valor": valor,
                        "status": status,
                        "severidade": severidade
                    })

                    data = row.timestamp.split(" ")[0]
                    alertas_por_dia[data] = alertas_por_dia.get(data, 0) + 1
                    alertas_por_servidor[servidor_id] = alertas_por_servidor.get(servidor_id, 0) + 1

    # =========================
    # RESUMO (FORA DO LOOP)
    # =========================
    dia_critico = max(alertas_por_dia, key=alertas_por_dia.get) if alertas_por_dia else None
    servidor_critico = max(alertas_por_servidor, key=alertas_por_servidor.get) if alertas_por_servidor else None

    resumo = {
        "dia_mais_critico": dia_critico,
        "servidor_mais_critico": servidor_critico,
        "total_alertas": len(alertas)
    }

    status_final = determinar_status_servidor(severidades_detectadas)
    atualizar_status_servidor(servidor_id, status_final)

    # =========================
    # SALVAMENTOS
    # =========================
    df_trusted = pandas.DataFrame(trusted_rows)

    salvar_s3_unificado(s3, f"trusted/empresa_{empresa_id}/{mac_address}/metricas_trusted.csv", df_trusted, formato="csv")

    salvar_s3_unificado(s3, f"client/empresa_{empresa_id}/{mac_address}/metricas.json", client_json, formato="json")

    salvar_s3_unificado(s3, f"client/alertas/empresa_{empresa_id}/{mac_address}/metricas.json", alertas, formato="json")

    salvar_s3_unificado(s3, f"client/resumo/empresa_{empresa_id}/{mac_address}/metricas.json", resumo, formato="json")

    

    # =========================================================
    # PIPELINE 2 - EMPRESAS (DASHBOARD + TRUSTED + KPIs)
    # =========================================================
    print("\n=== PIPELINE COMUNICAÇÃO ===")

    empresas = obter_empresas()

    for empresa in empresas:
        empresa_id = empresa["id_empresa"]
        print(f"\n── Processando empresa {empresa_id}")

        network_key = f"raw/raw.csv"
        flights_key = f"raw/flights_raw.csv"

        df_network = ler_csv(network_key)
        df_flights = ler_csv(flights_key)

        df_network = limpar_dados(df_network)
        df_flights = limpar_voos(df_flights)

        dfN_24h = df_network[df_network["timestamp"] >= agora - pandas.Timedelta(hours=24)]
        dfV_24h = df_flights[df_flights["timestamp_coleta"] >= agora - pandas.Timedelta(hours=24)]

        dfN_24h = enriquecer_dados(dfN_24h)
        incidentes = detectar_incidentes(dfN_24h)
        
        salvar_s3_unificado(s3, f"trusted/empresa_{empresa_id}/{mac_address}/network_trusted.csv", df_network, formato="csv")
        salvar_s3_unificado(s3, f"trusted/empresa_{empresa_id}/{mac_address}/flights_trusted.csv", df_flights, formato="csv")

        dashboard = gerar_json_dashboard(dfN_24h, dfV_24h, "24h")
        dfN_3d = df_network[df_network["timestamp"] >= agora - pandas.Timedelta(days=3)]
        dfV_3d = df_flights[df_flights["timestamp_coleta"] >= agora - pandas.Timedelta(days=3)]

        dfN_7d = df_network[df_network["timestamp"] >= agora - pandas.Timedelta(days=7)]
        dfV_7d = df_flights[df_flights["timestamp_coleta"] >= agora - pandas.Timedelta(days=7)]

        dfN_3d = enriquecer_dados(dfN_3d)
        dfN_7d = enriquecer_dados(dfN_7d)

        dashboard_3d = gerar_json_dashboard(dfN_3d, dfV_3d, "3d")
        dashboard_7d = gerar_json_dashboard(dfN_7d, dfV_7d, "7d")

        base_path = f"client/empresa_{empresa_id}/{mac_address}"

        salvar_s3_unificado(s3,f"{base_path}/dashboard_rede_24h.json",dashboard,formato="json")

        salvar_s3_unificado(s3,f"{base_path}/dashboard_rede_3d.json",dashboard_3d,formato="json")

        salvar_s3_unificado( s3,f"{base_path}/dashboard_rede_7d.json",dashboard_7d,formato="json")

        salvar_s3_unificado(s3, f"client/alertas/empresa_{empresa_id}/{mac_address}/incidentes_rede_24h.json",incidentes,formato="json")

        # =========================
        # DASHBOARD GESTOR
        # =========================
        print("\n=== PIPELINE GESTOR ===")

        arquivos = listar_arquivos_client(s3, empresa_id)

        todas_leituras = []
        for key in arquivos:
            dados = ler_json_s3(s3, key)
            if isinstance(dados, dict):
                todas_leituras.append(dados)
            else:
                todas_leituras.extend(dados)

        if not todas_leituras:
            continue

        servidores = obter_servidores_empresa(empresa_id)
        analistas = obter_analistas_por_servidor(empresa_id)

        limites = {
            srv["id_servidor"]: obter_limites_servidor(srv["id_servidor"])
            for srv in servidores
        }

        resultado = {
            "empresa_id": empresa_id,
            "gerado_em": agora.strftime("%Y-%m-%d %H:%M:%S"),
            "periodos": {}
        }

        for periodo in ["24h", "7d", "30d"]:
            leituras = filtrar_periodo(todas_leituras, periodo)

            if not leituras:
                resultado["periodos"][periodo] = {"sem_dados": True}
                continue

            resultado["periodos"][periodo] = {
                "kpis": {
                    "disponibilidade_global": calcular_disponibilidade(leituras, limites),
                    "nivel_risco": calcular_nivel_risco(leituras, limites),
                    "incidentes_criticos": calcular_incidentes_criticos(leituras, limites),
                    "estabilidade_operacional": calcular_estabilidade_operacional(leituras, limites),
                    "tendencia_operacional": calcular_tendencia(leituras, limites)
                },

                "grafico_estabilidade": grafico_estabilidade(leituras, limites),

                "impacto_por_componente": calcular_impacto_componente(
                    leituras,
                    limites
                ),

                "previsao_falhas": calcular_previsao_falhas(
                    leituras,
                    limites
                ),

                "info_servidores": listar_info_servidores(
                    leituras,
                    limites,
                    servidores,
                    analistas
                )
            }

        salvar_s3_unificado(s3, f"client/gestor/empresa_{empresa_id}/dashboard_gestor.json", resultado)

    print("\nPipeline unificado executado com sucesso.")


if __name__ == "__main__":
    main()