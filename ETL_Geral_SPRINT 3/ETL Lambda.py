import os
import json
import math
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta
import io
from io import StringIO
from urllib.parse import unquote_plus

import boto3
import mysql.connector
import numpy as np
import pandas
from botocore.exceptions import ClientError


# =========================================================
# CONFIGURACAO
# =========================================================

USAR_LOCAL = False

AWS_CONFIG = {
    "bucket_name": os.environ.get("AWS_BUCKET_NAME", ""),
    "region_name": os.environ.get("AWS_REGION", "us-east-1"),
}

SEVERIDADE = {
    "Critico": 5,
    "Alto": 4,
    "Medio": 3,
    "Baixo": 2,
    "Normal": 1,
}

PERIODOS = {
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}

PESOS_COMPONENTES = {
    "CPU": 0.8,
    "RAM": 1.0,
    "DISCO": 1.3,
}

CPU_CRITICA = 80
CPU_ALERTA = 50

RAM_CRITICA_PERCENT = 20
RAM_ALERTA_PERCENT = 10

LATENCIA_CRITICA = 100
LATENCIA_ALERTA = 50

JANELA_PREVISAO = 12


# =========================================================
# JSON / DATA HELPERS
# =========================================================

def normalizar_json(value):
    if isinstance(value, dict):
        return {str(k): normalizar_json(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [normalizar_json(v) for v in value]

    if isinstance(value, pandas.DataFrame):
        return normalizar_json(value.to_dict(orient="records"))

    if isinstance(value, pandas.Series):
        return normalizar_json(value.tolist())

    if isinstance(value, pandas.Timestamp):
        return None if pandas.isna(value) else value.isoformat()

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, np.integer):
        return int(value)

    if isinstance(value, np.floating):
        value = float(value)

    if isinstance(value, float):
        return value if math.isfinite(value) else None

    try:
        if pandas.isna(value):
            return None
    except Exception:
        pass

    return value


def timestamp_utc(value):
    ts = pandas.to_datetime(value, errors="coerce", utc=True)
    if pandas.isna(ts):
        return pandas.NaT
    return ts


def data_hora_str(value):
    ts = timestamp_utc(value)
    if pandas.isna(ts):
        return None
    return ts.isoformat()


def serie_numerica(df, coluna, padrao=0):
    if coluna in df.columns:
        return pandas.to_numeric(df[coluna], errors="coerce").fillna(padrao)
    return pandas.Series(padrao, index=df.index)


def filtrar_df_por_tempo(df, coluna, agora, delta):
    if df.empty or coluna not in df.columns:
        return df.iloc[0:0].copy()
    return df[df[coluna] >= agora - delta].copy()


# =========================================================
# S3
# =========================================================

def bucket_atual(bucket=None):
    bucket = bucket or AWS_CONFIG.get("bucket_name")
    if not bucket:
        raise ValueError("Bucket S3 nao informado. Use evento S3 ou AWS_BUCKET_NAME.")
    return bucket


def ler_csv(s3_client, key, bucket):
    """
    Força a leitura do CSV diretamente do S3 usando get_object e Pandas.
    """
    print(f"[S3] Buscando objeto: s3://{bucket}/{key}")
    
    # Força a chamada do get_object
    response = s3_client.get_object(Bucket=bucket, Key=key)
    
    # Lê o corpo do arquivo (StreamingBody) em memória
    conteudo_bytes = response['Body'].read()
    
    # Converte os bytes em um buffer de arquivo simulado para o Pandas
    df = pandas.read_csv(
        io.BytesIO(conteudo_bytes),
        on_bad_lines='skip',
        engine='python'
    )
    
    return df

def salvar_s3_unificado(s3, key, data, formato="json", bucket=None):
    bucket = bucket_atual(bucket)

    if formato == "json":
        body = json.dumps(
            normalizar_json(data),
            indent=2,
            ensure_ascii=False,
        )
        content_type = "application/json"

    elif formato == "json_dashboard":
        body = json.dumps(
            normalizar_json(data),
            indent=4,
            ensure_ascii=False,
        )
        content_type = "application/json"

    elif formato == "csv":
        buffer = StringIO()
        data.to_csv(buffer, index=False)
        body = buffer.getvalue()
        content_type = "text/csv"

    else:
        raise ValueError(f"Formato nao suportado: {formato}")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType=content_type,
    )
    print(f"[S3] Salvo: s3://{bucket}/{key}")


def listar_arquivos_client(s3, empresa_id, bucket=None):
    prefix = f"client/empresa_{empresa_id}/"
    paginator = s3.get_paginator("list_objects_v2")
    arquivos = []

    for page in paginator.paginate(
        Bucket=bucket_atual(bucket),
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                arquivos.append(obj["Key"])

    return arquivos


def ler_json_s3(s3, key, bucket=None):
    obj = s3.get_object(
        Bucket=bucket_atual(bucket),
        Key=key,
    )

    content = obj["Body"].read().decode(
        "utf-8",
        errors="ignore",
    )

    return json.loads(content)


# =========================================================
# MYSQL
# =========================================================

def get_db_config():
    required = ("DB_HOST", "DB_USER", "DB_PASSWORD", "DB_DATABASE")
    missing = [key for key in required if not os.environ.get(key)]

    if missing:
        raise ValueError(f"Variaveis de banco ausentes: {', '.join(missing)}")

    return {
    "host": os.environ["DB_HOST"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "database": os.environ["DB_DATABASE"],
    "port": int(os.environ.get("DB_PORT", "3306"))
    }

def get_db():
    return mysql.connector.connect(
        **get_db_config(),
        connection_timeout=10,
    )

def obter_empresas():
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT id_empresa, razao_social FROM empresa")
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

def obter_servidores_empresa(id_empresa):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT id_servidor, hostname, status_servidor
            FROM servidor
            WHERE fk_empresa = %s
            """,
            (id_empresa,),
        )
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

def atualizar_status_servidor(servidor_id, novo_status):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT status_servidor FROM servidor WHERE id_servidor = %s",
            (servidor_id,),
        )
        atual = cursor.fetchone()

        if atual and atual[0] != novo_status:
            cursor.execute(
                """
                UPDATE servidor
                SET status_servidor = %s,
                    data_status = CURRENT_TIMESTAMP
                WHERE id_servidor = %s
                """,
                (novo_status, servidor_id),
            )
            conn.commit()
            print(f"[STATUS] {servidor_id}: {atual[0]} -> {novo_status}")
        else:
            print("[STATUS] Status nao mudou")
    finally:
        cursor.close()
        conn.close()

def obter_analistas_por_servidor(empresa_id):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT sa.fk_servidor AS servidor_id, COUNT(*) AS total_analistas
            FROM acesso_servidor sa
            JOIN servidor s ON sa.fk_servidor = s.id_servidor
            WHERE s.fk_empresa = %s
            GROUP BY sa.fk_servidor
            """,
            (empresa_id,),
        )
        rows = cursor.fetchall()
        return {r["servidor_id"]: r["total_analistas"] for r in rows}
    finally:
        cursor.close()
        conn.close()

def obter_limites_servidor(servidor_id, dict_cursor=False):
    conn = get_db()
    cursor = conn.cursor(dictionary=dict_cursor)
    try:
        cursor.execute(
            """
            SELECT c.tipo, sc.limite
            FROM servidor_componente sc
            JOIN componente c ON sc.fk_componente = c.id_componente
            WHERE sc.fk_servidor = %s
            """,
            (servidor_id,),
        )
        rows = cursor.fetchall()
        if dict_cursor:
            return {r["tipo"]: float(r["limite"]) for r in rows}
        return {r[0]: float(r[1]) for r in rows}
    finally:
        cursor.close()
        conn.close()

def obter_limites_batch(servidor_ids):
    servidor_ids = [int(s) for s in servidor_ids if pandas.notna(s)]
    if not servidor_ids:
        return {}

    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    try:
        fmt = ",".join(["%s"] * len(servidor_ids))
        cursor.execute(
            f"""
            SELECT sc.fk_servidor, c.tipo, sc.limite
            FROM servidor_componente sc
            JOIN componente c ON sc.fk_componente = c.id_componente
            WHERE sc.fk_servidor IN ({fmt})
            """,
            servidor_ids,
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    limites = {}
    for r in rows:
        sid = r["fk_servidor"]
        limites.setdefault(sid, {})[r["tipo"]] = float(r["limite"])
    return limites

def obter_servidor_por_mac(mac_address):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT id_servidor, fk_empresa, hostname
            FROM servidor
            WHERE LOWER(mac_address) = LOWER(%s)
            """,
            (mac_address,),
        )
        return cursor.fetchone()
    finally:
        cursor.close()
        conn.close()

# =========================================================
# HELPERS
# =========================================================

def montar_path(tipo, empresa_id, mac_address, nome_arquivo, extensao="json", categoria=None):
    if not empresa_id:
        raise ValueError("empresa_id nao informado para montar path")
    if not mac_address:
        raise ValueError("mac_address nao informado para montar path")

    base = f"{tipo}/empresa_{empresa_id}/{mac_address}"
    if categoria:
        return f"{base}/{categoria}/{nome_arquivo}.{extensao}"
    return f"{base}/{nome_arquivo}.{extensao}"

def montar_path_raw(empresa_id, mac_address, arquivo, tipo="json"):
    return f"raw/empresa_{empresa_id}/{mac_address}/{arquivo}.{tipo}"

def safe_float(value):
    if value is None:
        return 0.0

    if isinstance(value, str):
        value = (
            value.replace("%", "")
            .replace(",", ".")
            .strip()
        )

    try:
        if pandas.isna(value):
            return 0.0
        return float(value)
    except Exception:
        return 0.0

def safe_int(value, default=0):
    try:
        if pandas.isna(value):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)

def extrair_mac_do_path(key):
    if not key:
        return None

    partes = key.strip("/").split("/")
    if len(partes) >= 3 and partes[0] in ("raw", "trusted", "client"):
        return partes[2]

    return None

def resolver_evento(event):
    bucket = event.get("bucket") or event.get("bucket_name") or AWS_CONFIG.get("bucket_name")
    key = event.get("key")
    mac_address = event.get("mac_address") or os.environ.get("MAC_ADDRESS")

    records = event.get("Records") or []
    if records:
        record = records[0].get("s3", {})
        bucket = record.get("bucket", {}).get("name") or bucket
        key = record.get("object", {}).get("key") or key

    if key:
        key = unquote_plus(key)

    mac_address = mac_address or extrair_mac_do_path(key)
    return bucket, key, mac_address

def chave_raw_metrics(key_recebida, empresa_id, mac_address):
    if key_recebida and key_recebida.rsplit("/", 1)[-1] == "raw.csv":
        return key_recebida
    return montar_path("raw", empresa_id, mac_address, "raw", extensao="csv")

def chave_raw_flights(key_recebida, empresa_id, mac_address):
    if key_recebida and key_recebida.rsplit("/", 1)[-1] == "flights_raw.csv":
        return key_recebida
    return montar_path("raw", empresa_id, mac_address, "flights_raw", extensao="csv")

def chave_raw_processos(key_recebida, empresa_id, mac_address):
    if key_recebida and key_recebida.rsplit("/", 1)[-1] == "process_raw.csv":
        return key_recebida

    return montar_path(
        "raw",
        empresa_id,
        mac_address,
        "process_raw",
        extensao="csv"
    )
# =========================================================
# METRICAS / CLASSIFICACAO
# =========================================================

def classificar_metrica(valor, limite):
    valor = safe_float(valor)
    limite = safe_float(limite)

    if limite == 0:
        return "Online", "Normal"
    if valor == 0 and limite > 0:
        return "Offline", "Critico"
    if valor >= limite:
        return "Critico", "Critico"
    if valor >= 0.9 * limite:
        return "Critico", "Alto"
    if valor >= 0.8 * limite:
        return "Atencao", "Medio"
    if valor >= 0.7 * limite:
        return "Online", "Baixo"
    return "Online", "Normal"

def classificar(valor, limite):
    valor = safe_float(valor)
    limite = safe_float(limite)

    if limite == 0:
        return "Normal"
    if valor == 0 and limite > 0:
        return "Critico"
    razao = valor / limite
    if razao >= 1.0:
        return "Critico"
    if razao >= 0.90:
        return "Alto"
    if razao >= 0.80:
        return "Medio"
    if razao >= 0.70:
        return "Baixo"
    return "Normal"

def determinar_status_servidor(severidades):
    if not severidades:
        return "Online"
    pior = max(severidades, key=lambda s: SEVERIDADE.get(s, 0))
    if pior in ("Critico", "Alto"):
        return "Critico"
    if pior == "Medio":
        return "Atencao"
    return "Online"

def classificar_status(score):
    if score >= 90:
        return "Saudavel"
    if score >= 80:
        return "Atencao"
    return "Critico"

def classificar_latencia(valor):
    valor = safe_float(valor)
    if valor > 250:
        return "Critico"
    if valor > 200:
        return "Alto"
    if valor > 150:
        return "Medio"
    if valor > 100:
        return "Baixo"
    return "Normal"

def classificar_pacotes(valor):
    valor = safe_float(valor)
    if valor > 20:
        return "Critico"
    if valor > 15:
        return "Alto"
    if valor > 10:
        return "Medio"
    if valor > 5:
        return "Baixo"
    return "Normal"

# =========================================================
# LIMPEZA DE DADOS
# =========================================================

def limpar_dados(df):
    if df.empty:
        return df

    df = df.copy()
    colunas_numericas = [
        "bytes_recv", "bytes_sent", "pack_recv", "pack_sent",
        "packet_loss_internet", "latency_min_ms", "latency_avg_ms",
        "latency_max_ms", "lat_adsb_rastreamento", "lat_rastreamento_correlacao",
        "lat_correlacao_rotas", "lat_rotas_api", "lat_api_bd", "lat_bd_sync",
        "rastreamento_mbps", "rotas_mbps", "correlacao_mbps", "api_gateway_mbps",
        "bd_mbps", "sync_service_mbps", "rastreamento_loss", "correlacao_loss",
        "rotas_loss", "api_loss", "bd_loss", "sync_loss",
        "total_aeronaves", "avg_adsb_update_seconds",
        "cpu", "ram", "disco", "ram_percent", "ram_mb", "latencia_ms",
    ]

    colunas_existentes = [c for c in colunas_numericas if c in df.columns]
    if colunas_existentes:
        df[colunas_existentes] = df[colunas_existentes].apply(pandas.to_numeric, errors="coerce")
        df[colunas_existentes] = df[colunas_existentes].fillna(0)

    if "timestamp" in df.columns:
        df["timestamp"] = pandas.to_datetime(df["timestamp"], errors="coerce", utc=True)
        
        df["label_24h"] = df["timestamp"].dt.strftime("%H:%M")
        df["label_3d"] = df["timestamp"].dt.strftime("%d/%m %Hh")
        df["label_7d"] = df["timestamp"].dt.strftime("%d/%m")

    if "opensky_timestamp" in df.columns:
        df["opensky_timestamp"] = pandas.to_datetime(df["opensky_timestamp"], errors="coerce", utc=True)

    return df

def limpar_voos(df):
    if df.empty:
        return df

    df = df.copy()
    if "timestamp_coleta" in df.columns:
        df["timestamp_coleta"] = pandas.to_datetime(
            df["timestamp_coleta"],
            errors="coerce",
            utc=True,
        )

    if "delay_origem" in df.columns:
        df["delay_origem"] = pandas.to_numeric(
            df["delay_origem"],
            errors="coerce",
        ).fillna(0)

    if "delay_destino" in df.columns:
        df["delay_destino"] = pandas.to_numeric(
            df["delay_destino"],
            errors="coerce",
        ).fillna(0)

    if "origem" in df.columns:
        df["origem"] = df["origem"].astype(str).str.strip()

    if "destino" in df.columns:
        df["destino"] = df["destino"].astype(str).str.strip()

    if "numero_voo" in df.columns:
        df = df.dropna(subset=["numero_voo"])

    return df

# =========================================================
# ENRIQUECIMENTO DE DADOS
# =========================================================

def severidade_servidor_latencia(linha):
    status = [
        linha.get("status_latency_avg", "Normal"),
        linha.get("status_adsb", "Normal"),
        linha.get("status_correlacao_rotas", "Normal"),
        linha.get("status_rotas_api", "Normal"),
        linha.get("status_api_bd", "Normal"),
        linha.get("status_bd_sync", "Normal"),
    ]
    return max(status, key=lambda x: SEVERIDADE.get(x, 0))

def severidade_servidor_pacotes(linha):
    status = [
        linha.get("status_packet_loss", "Normal"),
        linha.get("status_rastreamento_loss", "Normal"),
        linha.get("status_correlacao_loss", "Normal"),
        linha.get("status_rotas_loss", "Normal"),
        linha.get("status_api_loss", "Normal"),
        linha.get("status_bd_loss", "Normal"),
        linha.get("status_sync_loss", "Normal"),
    ]
    return max(status, key=lambda x: SEVERIDADE.get(x, 0))

def enriquecer_dados(df):
    if df.empty:
        return df.copy()

    df = df.copy()
    df["packet_loss_internet"] = serie_numerica(df, "packet_loss_internet")
    df["rastreamento_loss"] = serie_numerica(df, "rastreamento_loss")
    df["correlacao_loss"] = serie_numerica(df, "correlacao_loss")
    df["rotas_loss"] = serie_numerica(df, "rotas_loss")
    df["api_loss"] = serie_numerica(df, "api_loss")
    df["bd_loss"] = serie_numerica(df, "bd_loss")
    df["sync_loss"] = serie_numerica(df, "sync_loss")

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
            "sync_loss",
        ]
    ].mean(axis=1)

    df["status_servidor_pacotes"] = df.apply(
        severidade_servidor_pacotes,
        axis=1,
    )

    df["latency_avg_ms"] = serie_numerica(df, "latency_avg_ms")
    df["lat_adsb_rastreamento"] = serie_numerica(df, "lat_adsb_rastreamento")
    df["lat_api_bd"] = serie_numerica(df, "lat_api_bd")
    df["lat_bd_sync"] = serie_numerica(df, "lat_bd_sync")
    df["lat_correlacao_rotas"] = serie_numerica(df, "lat_correlacao_rotas")
    df["lat_rotas_api"] = serie_numerica(df, "lat_rotas_api")

    df["status_latency_avg"] = df["latency_avg_ms"].apply(classificar_latencia)
    df["status_adsb"] = df["lat_adsb_rastreamento"].apply(classificar_latencia)
    df["status_api_bd"] = df["lat_api_bd"].apply(classificar_latencia)
    df["status_bd_sync"] = df["lat_bd_sync"].apply(classificar_latencia)
    df["status_correlacao_rotas"] = df["lat_correlacao_rotas"].apply(classificar_latencia)
    df["status_rotas_api"] = df["lat_rotas_api"].apply(classificar_latencia)

    df["status_servidor_latencia"] = df.apply(
        severidade_servidor_latencia,
        axis=1,
    )

    return df

# =========================================================
# SCORE
# =========================================================

def calcular_penalidade(persistencia, peso_maximo):
    if persistencia < 0.20:
        pct = 0
    elif persistencia < 0.40:
        pct = 0.25
    elif persistencia < 0.60:
        pct = 0.50
    elif persistencia < 0.80:
        pct = 0.75
    else:
        pct = 1
    return peso_maximo * pct

def calcular_persistencia_alertas(df, hostname, servidor_id, coluna_tempo="timestamp"):
    if df.empty or coluna_tempo not in df.columns:
        return {}

    obter_limites_bd = obter_limites_servidor(servidor_id)
    limite_cpu = obter_limites_bd.get("CPU", 0)
    limite_ram = obter_limites_bd.get("RAM", 0)
    limite_disco = obter_limites_bd.get("DISCO", 0)
    limite_adsb = 10

    df = df.copy()
    df[coluna_tempo] = pandas.to_datetime(df[coluna_tempo], errors="coerce", utc=True)

    if hostname is not None and "hostname" in df.columns:
        df = df[df["hostname"] == hostname]

    if df.empty:
        return {}

    agora = df[coluna_tempo].max()

    periodos_alerta = {
        "1h": pandas.Timedelta(hours=1),
        "12h": pandas.Timedelta(hours=12),
        "24h": pandas.Timedelta(hours=24),
        "7d": pandas.Timedelta(days=7),
    }

    resultados = {}
    cpu = serie_numerica(df, "cpu")
    ram = serie_numerica(df, "ram")
    disco = serie_numerica(df, "disco")
    adsb = serie_numerica(df, "avg_adsb_update_seconds")

    df["_cpu"] = cpu
    df["_ram"] = ram
    df["_disco"] = disco
    df["_adsb"] = adsb

    for nome_periodo, delta in periodos_alerta.items():
        df_periodo = df[df[coluna_tempo] >= agora - delta]
        total_coletas = len(df_periodo)

        if total_coletas == 0:
            resultados[nome_periodo] = {
                "total_coletas": 0,
                "cpu": {"alertas": 0, "persistencia": 0},
                "ram": {"alertas": 0, "persistencia": 0},
                "disco": {"alertas": 0, "persistencia": 0},
                "adsb": {"alertas": 0, "persistencia": 0},
            }
            continue

        alertas_cpu = (df_periodo["_cpu"] > limite_cpu).sum() if limite_cpu else 0
        alertas_ram = (df_periodo["_ram"] > limite_ram).sum() if limite_ram else 0
        alertas_disco = (df_periodo["_disco"] > limite_disco).sum() if limite_disco else 0
        alertas_adsb = (df_periodo["_adsb"] > limite_adsb).sum()

        resultados[nome_periodo] = {
            "total_coletas": total_coletas,
            "cpu": {"alertas": int(alertas_cpu), "persistencia": float(round(alertas_cpu / total_coletas, 2))},
            "ram": {"alertas": int(alertas_ram), "persistencia": float(round(alertas_ram / total_coletas, 2))},
            "disco": {"alertas": int(alertas_disco), "persistencia": float(round(alertas_disco / total_coletas, 2))},
            "adsb": {"alertas": int(alertas_adsb), "persistencia": float(round(alertas_adsb / total_coletas, 2))},
        }

    return resultados

def calcular_score_servidor(dados, hostname, servidor_id):
    persistencias = calcular_persistencia_alertas(
        dados,
        hostname,
        servidor_id,
    )
    resultados_score = {}

    for periodo, dados_periodo in persistencias.items():
        p_cpu = dados_periodo["cpu"]["persistencia"]
        p_ram = dados_periodo["ram"]["persistencia"]
        p_disco = dados_periodo["disco"]["persistencia"]
        p_adsb = dados_periodo["adsb"]["persistencia"]

        pen_cpu = calcular_penalidade(p_cpu, 30)
        pen_ram = calcular_penalidade(p_ram, 30)
        pen_disco = calcular_penalidade(p_disco, 15)
        pen_adsb = calcular_penalidade(p_adsb, 25)

        score = max(0, min(100, 100 - pen_cpu - pen_ram - pen_disco - pen_adsb))

        resultados_score[periodo] = {
            "score": round(score, 2),
            "status": classificar_status(score),
            "penalidades": {"cpu": pen_cpu, "ram": pen_ram, "disco": pen_disco, "adsb": pen_adsb},
            "persistencias": {"cpu": p_cpu, "ram": p_ram, "disco": p_disco, "adsb": p_adsb},
        }

    return resultados_score

# =========================================================
# TEMPERATURA
# =========================================================

def gerar_alerta(row):
    alertas = []

    status_temp = str(
        row.get(
            "status_temperatura",
            ""
        )
    )

    status_margem = str(
        row.get(
            "status_margem",
            ""
        )
    )

    status_resfriamento = str(
        row.get(
            "status_resfriamento",
            ""
        )
    )

    throttling = str(
        row.get(
            "Throttling",
            ""
        )
    )

    # TEMPERATURA

    if status_temp in [
        "Critico",
        "Medio",
        "Alerta"
    ]:

        alertas.append(
            "Temperatura CPU elevada"
        )

    # MARGEM

    if status_margem in [
        "Critico",
        "Atencao",
        "Throttling"
    ]:

        alertas.append(
            "Margem térmica crítica"
        )

    # RESFRIAMENTO

    if status_resfriamento in [
        "Critico",
        "Atencao"
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

def calcular_dia_mais_alertas(df):
    if "alertas" not in df.columns or "timestamp" not in df.columns:
        return None

    df_alertas = df[df["alertas"].fillna("") != ""]

    if df_alertas.empty:
        return None

    dias = pandas.to_datetime(
        df_alertas["timestamp"],
        errors="coerce",
        utc=True,
    ).dt.strftime("%A")

    contador = Counter(dias.dropna())

    if not contador:
        return None

    return contador.most_common(1)[0][0]

# =========================================================
# PROCESSOS
# =========================================================

def processos_tratados_s3(df_raw):
    print("=== COLUNAS ORIGINAIS ===")
    print(df_raw.columns.tolist())

    print("=== PRIMEIRA LINHA ===")
    print(df_raw.iloc[0].to_dict())
    if df_raw.empty:
        return pandas.DataFrame()

    dados_tratados = []

    for linha in df_raw.to_dict(orient="records"):
        cpu = safe_float(linha.get("cpu"))
        ram_percent = safe_float(linha.get("ram_percent", linha.get("ram")))
        latencia = safe_float(linha.get("latencia_ms"))

        dados_tratados.append({
            "timestamp": linha.get("timestamp"),
            "pid": linha.get("pid"),
            "nome": linha.get("nome"),
            "usuario": linha.get("usuario"),
            "cpu": cpu,
            "ram": ram_percent,
            "ram_percent": ram_percent,
            "ram_mb": safe_float(linha.get("ram_mb")),
            "status": linha.get("status"),
            "tempo_execucao": linha.get("tempo_execucao"),
            "latencia_ms": latencia,
            "criticidade": processos_criticidade(
                cpu,
                ram_percent,
                latencia,
            ),
        })

    return pandas.DataFrame(dados_tratados)

def processos_criticidade(cpu, ram_percent, latencia):

    if (
        cpu >= CPU_CRITICA
        or ram_percent > RAM_CRITICA_PERCENT
        or latencia > LATENCIA_CRITICA
    ):
        return "Critico"

    elif (
        cpu > CPU_ALERTA
        or ram_percent > RAM_ALERTA_PERCENT
        or latencia > LATENCIA_ALERTA
    ):
        return "Alerta"

    return "Estavel"

#-----------------------------------------------------------------------------------------------------
def top5cpu(dfProcessos):

    dfTop5 = dfProcessos.sort_values(
        'cpu',
        ascending=False
    )
    

    cpu5 = {}

    idx = 0
    posicao = 1

    while posicao <= 5 and idx < len(dfTop5):

        nome = dfTop5["nome"].iloc[idx]

        if nome != "System Idle Process":

            cpu5[f"nome_cpu_{posicao}"] = nome
            cpu5[f"cpu_{posicao}"] = float(dfTop5["cpu"].iloc[idx])

            posicao += 1

        idx += 1

    return cpu5
#--------------------------------------------------------------------------------------
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
    total_criticos = (df_processos['criticidade'] == 'Critico').sum()
    return {"totalCriticos": int(total_criticos)}

def maior_latencia(df):
    if df.empty:
        return {
            "nome": None,
            "latencia_ms": 0,
            "pid": None
        }

    linha_maior = df.loc[df["latencia_ms"].idxmax()]

    pid = linha_maior["pid"]

    return {
        "nome": linha_maior["nome"],
        "latencia_ms": float(linha_maior["latencia_ms"]),
        "pid": int(pid) if pandas.notna(pid) else None
    }

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
    df["timestamp"] = pandas.to_datetime(
    df["timestamp"],
        utc=True
    )

    agora = pandas.Timestamp.now(tz="UTC")

    # diferença em horas
    df["horas_atras"] = (
        (agora - df["timestamp"])
        .dt.total_seconds() / 3600
    )

    # apenas críticos
    df_criticos = df[
        df["criticidade"] == "Critico"
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
        df["criticidade"] == "Critico"
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

# =========================================================
# KPIS DE REDE
# =========================================================

def media_segura(df, coluna):
    if coluna not in df.columns:
        return 0

    return round(
        pandas.to_numeric(
            df[coluna],
            errors="coerce",
        ).fillna(0).mean(),
        2,
    )

def perda_pacotes_servico(df):
    return {
        "Rastreamento": media_segura(df, "rastreamento_loss"),
        "Rotas": media_segura(df, "rotas_loss"),
        "Correlação": media_segura(df, "correlacao_loss"),
        "API Gateway": media_segura(df, "api_loss"),
        "Banco de Dados": media_segura(df, "bd_loss"),
        "Sync Service": media_segura(df, "sync_loss"),
    }

def kpi_perda_media(df):
    cols = [
        c for c in [
            "packet_loss_internet",
            "rastreamento_loss",
            "correlacao_loss",
            "rotas_loss",
            "api_loss",
            "bd_loss",
            "sync_loss",
        ]
        if c in df.columns
    ]

    if not cols:
        return 0

    return round(
        df[cols]
        .apply(
            pandas.to_numeric,
            errors="coerce",
        )
        .fillna(0)
        .mean()
        .mean(),
        2,
    )

def kpi_latencia_media(df):
    cols = [
        c for c in [
            "latency_avg_ms",
            "lat_adsb_rastreamento",
            "lat_rastreamento_correlacao",
            "lat_correlacao_rotas",
            "lat_rotas_api",
            "lat_api_bd",
            "lat_bd_sync",
        ]
        if c in df.columns
    ]

    if not cols:
        return 0

    return round(
        df[cols]
        .apply(
            pandas.to_numeric,
            errors="coerce",
        )
        .fillna(0)
        .mean()
        .mean(),
        2,
    )

def kpi_adsb_update(df):
    if "avg_adsb_update_seconds" not in df.columns:
        return 0

    media = safe_float(
        df["avg_adsb_update_seconds"].mean(),
    )

    if media <= 2:
        return round(100 - media * 2, 1)

    return round(
        max(
            96 * math.exp(-(media - 2) / 20),
            0,
        ),
        1,
    )

def rotas_sem_atualizacao(df_voos):
    colunas = [
        "numero_voo",
        "origem",
        "destino",
        "status",
    ]

    if df_voos.empty or not all(c in df_voos.columns for c in colunas):
        return 0

    agrupado = df_voos.groupby(["numero_voo", "origem", "destino", "status"]).size()
    rotas_paradas = agrupado[agrupado >= 3]
    return len(rotas_paradas)

def taxa_transferencia(df):
    if (
        "bytes_recv" not in df.columns
        or "bytes_sent" not in df.columns
    ):
        return 0

    taxa = (
        df["bytes_recv"].fillna(0)
        + df["bytes_sent"].fillna(0)
    ) / (1024 * 1024)

    return round(taxa.mean(), 2)

def consumo_banda_servico(df):
    servicos = {
        "Rastreamento": "rastreamento_mbps",
        "Rotas": "rotas_mbps",
        "Correlacao": "correlacao_mbps",
        "API Gateway": "api_gateway_mbps",
        "Banco de Dados": "bd_mbps",
        "Sync Service": "sync_service_mbps",
    }

    resultado = {}

    for nome, coluna in servicos.items():
        if coluna in df.columns:
            media = (
                pandas.to_numeric(
                    df[coluna],
                    errors="coerce",
                )
                .fillna(0)
                .mean()
            )
            resultado[nome] = round(media, 2)
        else:
            resultado[nome] = 0

    return resultado

# =========================================================
# INCIDENTES
# =========================================================

def detectar_incidentes(df):
    incidentes = []
    if df.empty:
        return incidentes

    ultima = df.iloc[-1]
    hostname = ultima.get("hostname", "desconhecido")
    componentes_criticos = 0

    sev_lat = str(ultima.get("status_servidor_latencia", "Normal")).upper()
    if sev_lat in ("Critico", "Alto"):
        if sev_lat == "Critico":
            componentes_criticos += 1
        incidentes.append({
            "titulo": "Latencia elevada no servidor",
            "criticidade": sev_lat,
            "servidor": hostname,
            "componente": "rede",
            "tipo": "latencia",
            "valor": safe_float(ultima.get("latency_avg_ms")),
            "timestamp": str(ultima.get("timestamp")),
        })

    sev_pkt = str(ultima.get("status_servidor_pacotes", "Normal")).upper()
    if sev_pkt in ("Critico", "Alto"):
        if sev_pkt == "Critico":
            componentes_criticos += 1
        incidentes.append({
            "titulo": "Perda de pacotes elevada",
            "criticidade": sev_pkt,
            "servidor": hostname,
            "componente": "rede",
            "tipo": "packet_loss",
            "valor": safe_float(ultima.get("packet_loss_internet")),
            "timestamp": str(ultima.get("timestamp")),
        })

    if safe_float(ultima.get("avg_adsb_update_seconds")) > 10:
        componentes_criticos += 1
        incidentes.append({
            "titulo": "Delay elevado no ADS-B",
            "criticidade": "Critico",
            "servidor": hostname,
            "componente": "rede",
            "tipo": "adsb",
            "valor": safe_float(ultima.get("avg_adsb_update_seconds")),
            "timestamp": str(ultima.get("timestamp")),
        })

    if componentes_criticos >= 2:
        incidentes.append({
            "titulo": "Multiplos componentes criticos na rede",
            "criticidade": "Critico",
            "servidor": hostname,
            "componente": "rede",
            "tipo": "infraestrutura",
            "valor": componentes_criticos,
            "timestamp": str(ultima.get("timestamp")),
        })

    return incidentes

# =========================================================
# AGRUPAMENTO / DASHBOARD REDE
# =========================================================

def agrupar_periodo(df, periodo, coluna_tempo="timestamp"):
    if df.empty or coluna_tempo not in df.columns:
        return pandas.DataFrame()

    df = df.copy()
    if periodo == "24h":
        df["grupo"] = df[coluna_tempo].dt.strftime("%H:%M")
    elif periodo == "3d":
        df["grupo"] = df[coluna_tempo].dt.strftime("%d/%m %Hh")
    else:
        df["grupo"] = df[coluna_tempo].dt.strftime("%d/%m")

    colunas_agg = [
        "rastreamento_mbps", "rotas_mbps", "correlacao_mbps",
        "api_gateway_mbps", "bd_mbps", "sync_service_mbps",
        "latency_avg_ms", "lat_adsb_rastreamento", "lat_rastreamento_correlacao",
        "lat_correlacao_rotas", "lat_rotas_api", "lat_api_bd", "lat_bd_sync",
        "packet_loss_internet", "rastreamento_loss", "rotas_loss",
        "correlacao_loss", "api_loss", "bd_loss", "sync_loss",
        "avg_adsb_update_seconds",
    ]

    agg = {col: "mean" for col in colunas_agg if col in df.columns}
    if not agg:
        return pandas.DataFrame({"grupo": sorted(df["grupo"].dropna().unique().tolist())})
    return df.groupby("grupo").agg(agg).reset_index()

def lista_coluna(df, coluna):
    if coluna not in df.columns:
        return [0 for _ in range(len(df))]
    return pandas.to_numeric(df[coluna], errors="coerce").fillna(0).round(2).tolist()

def gerar_json_dashboard(df_network, df_flights, periodo):
    df_agrupado = agrupar_periodo(df_network, periodo, "timestamp")

    if df_agrupado.empty:
        return {
            "periodo": periodo,
            "kpis": {},
            "grafico_transferencia": {},
            "grafico_latencia_componentes": {},
            "consumo_banda": {},
            "perda_pacotes_servico": {},
        }

    return {
        "periodo": periodo,
        "kpis": {
            "perda_pacotes": round(kpi_perda_media(df_agrupado), 2),
            "latencia_media": round(kpi_latencia_media(df_agrupado), 2),
            "adsb_update": kpi_adsb_update(df_agrupado),
            "rotas_sem_atualizacao": rotas_sem_atualizacao(df_flights),
        },
        "grafico_transferencia": {
            "labels": df_agrupado["grupo"].tolist(),
            "rastreamento": lista_coluna(df_agrupado, "rastreamento_mbps"),
            "rotas": lista_coluna(df_agrupado, "rotas_mbps"),
            "correlacao": lista_coluna(df_agrupado, "correlacao_mbps"),
        },
        "grafico_latencia_componentes": {
            "ADS-B": media_segura(df_agrupado, "lat_adsb_rastreamento"),
            "Correlação": media_segura(df_agrupado, "lat_rastreamento_correlacao"),
            "Rotas": media_segura(df_agrupado, "lat_rotas_api"),
            "Banco de Dados": media_segura(df_agrupado, "lat_api_bd"),
            "Sync Service": media_segura(df_agrupado, "lat_bd_sync"),
        },
        "consumo_banda": consumo_banda_servico(df_agrupado),
        "perda_pacotes_servico": perda_pacotes_servico(df_agrupado),
    }

# =========================================================
# GESTOR
# =========================================================

def filtrar_periodo(leituras, periodo):
    if periodo not in PERIODOS:
        return []

    delta = PERIODOS[periodo]
    corte = pandas.Timestamp.now(tz="UTC") - delta
    filtradas = []

    for r in leituras:
        if "metricas" not in r or "servidor_id" not in r:
            continue

        data_str = r.get("data_hora")
        if not data_str:
            continue

        data = pandas.to_datetime(data_str, errors="coerce", utc=True)
        if pandas.isna(data):
            continue

        if data >= corte:
            filtradas.append(r)

    return filtradas

def ultimas_leituras_por_servidor(leituras):
    ultimas = {}

    for r in leituras:
        sid = r.get("servidor_id")
        data = pandas.to_datetime(r.get("data_hora"), errors="coerce", utc=True)

        if sid is None or pandas.isna(data):
            continue

        if sid not in ultimas or data > ultimas[sid]["_data"]:
            novo = r.copy()
            novo["_data"] = data
            ultimas[sid] = novo

    return list(ultimas.values())

def classificar_gestor(valor, limite):
    valor = safe_float(valor)
    limite = safe_float(limite)
    if limite == 0 or valor == 0:
        return "Normal"
    razao = valor / limite
    if razao >= 1.0:
        return "Critico"
    if razao >= 0.90:
        return "Alto"
    if razao >= 0.80:
        return "Medio"
    if razao >= 0.70:
        return "Baixo"
    return "Normal"

def calcular_disponibilidade(leituras, limites):
    leituras = ultimas_leituras_por_servidor(leituras)

    if not leituras:
        return 0

    online = 0
    validas = 0

    for r in leituras:
        s = r.get("servidor_id")
        if s is None or s not in limites:
            continue

        m = r.get("metricas", {})
        validas += 1

        cpu_lim   = safe_float(limites[s].get("CPU",   0))
        ram_lim   = safe_float(limites[s].get("RAM",   0))
        disco_lim = safe_float(limites[s].get("DISCO", 0))

        cpu   = safe_float(m.get("cpu",   0))
        ram   = safe_float(m.get("ram",   0))
        disco = safe_float(m.get("disco", 0))

        if (
            classificar_gestor(cpu,   cpu_lim)   != "Critico"
            and classificar_gestor(ram,   ram_lim)   != "Critico"
            and classificar_gestor(disco, disco_lim) != "Critico"
        ):
            online += 1

    if validas == 0:
        return 0
    return round((online / validas) * 100, 2)

def calcular_nivel_risco(leituras, limites):
    leituras = ultimas_leituras_por_servidor(leituras)

    total = 0
    qtd = 0

    for r in leituras:
        s = r.get("servidor_id")
        if s is None or s not in limites:
            continue

        m = r.get("metricas", {})

        for chave in ("CPU", "RAM", "DISCO"):
            lim = safe_float(limites[s].get(chave, 0))
            val = safe_float(m.get(chave.lower(), 0))
            total += SEVERIDADE.get(classificar_gestor(val, lim), 0)

        qtd += 3

    if qtd == 0:
        return 0
    return round(((total / qtd) - 1) / 4 * 100, 2)

def calcular_incidentes_criticos(leituras, limites):
    leituras = ultimas_leituras_por_servidor(leituras)

    criticos = 0
    for r in leituras:
        s = r.get("servidor_id")
        if s is None or s not in limites:
            continue

        m = r.get("metricas", {})
        if (
            classificar_gestor(safe_float(m.get("cpu")),   safe_float(limites[s].get("CPU",   0))) == "Critico"
            or classificar_gestor(safe_float(m.get("ram")),   safe_float(limites[s].get("RAM",   0))) == "Critico"
            or classificar_gestor(safe_float(m.get("disco")), safe_float(limites[s].get("DISCO", 0))) == "Critico"
        ):
            criticos += 1
    return criticos

def calcular_estabilidade_operacional(leituras, limites):
    leituras = ultimas_leituras_por_servidor(leituras)

    if not leituras:
        return 0

    estaveis = 0
    validas = 0

    for r in leituras:
        s = r.get("servidor_id")
        if s is None or s not in limites:
            continue

        cpu_lim   = safe_float(limites[s].get("CPU",   0))
        ram_lim   = safe_float(limites[s].get("RAM",   0))
        disco_lim = safe_float(limites[s].get("DISCO", 0))

        if cpu_lim <= 0 or ram_lim <= 0 or disco_lim <= 0:
            continue

        m = r.get("metricas", {})
        validas += 1

        if (
            safe_float(m.get("cpu"))   / cpu_lim   < 0.80
            and safe_float(m.get("ram"))   / ram_lim   < 0.80
            and safe_float(m.get("disco")) / disco_lim < 0.80
        ):
            estaveis += 1

    if validas == 0:
        return 0
    return round((estaveis / validas) * 100, 2)

def calcular_tendencia(leituras, limites):
    agora = pandas.Timestamp.now(tz="UTC")

    atual = [
        r for r in leituras
        if pandas.to_datetime(
            r.get("data_hora"),
            errors="coerce",
            utc=True,
        ) >= agora - timedelta(hours=1)
    ]

    anterior = [
        r for r in leituras
        if (
            agora - timedelta(hours=2)
            <= pandas.to_datetime(
                r.get("data_hora"),
                errors="coerce",
                utc=True,
            )
            < agora - timedelta(hours=1)
        )
    ]

    ra = calcular_nivel_risco(atual, limites)
    rb = calcular_nivel_risco(anterior, limites)

    if ra > rb:
        return "Subindo"

    if ra < rb:
        return "Caindo"

    return "Estavel"

def grafico_estabilidade(leituras, limites):
    grupos = {}
    for r in leituras:
        data = r.get("data_hora")
        if not data:
            continue

        ts = pandas.to_datetime(data, errors="coerce", utc=True)
        if pandas.isna(ts):
            continue

        hora = ts.strftime("%Y-%m-%d %H")
        grupos.setdefault(hora, []).append(r)

    labels = []
    valores = []
    for hora in sorted(grupos.keys()):
        labels.append(hora[11:] + ":00")
        valores.append(calcular_estabilidade_operacional(ultimas_leituras_por_servidor(grupos[hora]), limites))
    return {"labels": labels[-7:], "valores": valores[-7:]}

# adicionado id do servidor no parametro
def gerar_mensagem(metrica, nivel, previsao, limite, servidor_id):
    if limite <= 0:
        pct = 0
    else:
        pct = round(previsao / limite * 100, 1)

    nivel_map = {
        "Baixo": "baixa",
        "Normalo": "media",
        "Alto": "alta",
        "Critico": "critico",
        "baixa": "baixa",
        "média": "media",
        "media": "media",
        "alta": "alta",
        "crítico": "critico",
        "critico": "critico",
    }
    chave = nivel_map.get(str(nivel), "baixa")

# adicionado servidor id nas mensagens
    mensagens = {
        "CPU": {
            "baixa": f"Servidor {servidor_id}: CPU prevista em {pct}% do limite no servidor - Leve aumento na carga de processamento. Monitore a tendencia.",
            "media": f"Servidor {servidor_id}: CPU prevista em {pct}% do limite - Carga de processamento em elevacao. Verifique rotinas de calculo de rotas e separacao de aeronaves em execucao.",
            "alta": f"Servidor {servidor_id}: CPU prevista em {pct}% do limite - Processamento de dados de radar pode ser impactado. Considere redistribuir carga entre os nos do Sagitario.",
            "critico": f"Servidor {servidor_id}: CPU prevista em {pct}% do limite - Risco de atraso no processamento de dados de voo. Notifique um analista responsavel imediatamente.",
        },
        "RAM": {
            "baixa": f"Servidor {servidor_id}: RAM prevista em {pct}% do limite - Leve crescimento no consumo de memoria. Monitore a tendencia.",
            "media": f"Servidor {servidor_id}: RAM prevista em {pct}% do limite - Consumo de memoria crescente. Verifique buffers de dados de radar e faixas de voo ativas.",
            "alta": f"Servidor {servidor_id}: RAM prevista em {pct}% do limite - Risco de degradacao no gerenciamento de planos de voo. Verifique processos de correlacao de pistas.",
            "critico": f"Servidor {servidor_id}: RAM prevista em {pct}% do limite - Risco de falha no rastreamento de aeronaves. Reinicie processos nao essenciais e acione o sistema de contingencia do Sagitario.",
        },
        "DISCO": {
            "baixa": f"Servidor {servidor_id}: Disco previsto em {pct}% do limite - Leve crescimento no uso de armazenamento. Monitore a tendencia.",
            "media": f"Servidor {servidor_id}: Disco previsto em {pct}% do limite - Crescimento no volume de logs operacionais. Verifique retencao de gravacoes de voz e registros de radar.",
            "alta": f"Servidor {servidor_id}: Disco previsto em {pct}% do limite - Armazenamento de dados de voo pode ser comprometido. Realize purga de arquivos temporarios e logs antigos.",
            "critico": f"Servidor {servidor_id}: Disco previsto em {pct}% do limite - Risco de interrupcao no registro de dados operacionais. Arquive ou remova gravacoes antigas imediatamente e acione o suporte tecnico.",
        },
    }
    return mensagens[metrica][chave]

def calcular_previsao_falhas(leituras, limites):
    por_servidor = {}
    leituras_ordenadas = sorted(
        leituras,
        key=lambda r: pandas.to_datetime(r.get("data_hora"), errors="coerce", utc=True),
    )

    for r in leituras_ordenadas:
        s = r.get("servidor_id")
        if s not in limites:
            continue
        m = r.get("metricas", {})
        por_servidor.setdefault(s, {"cpu": [], "ram": [], "disco": []})
        por_servidor[s]["cpu"].append(safe_float(m.get("cpu")))
        por_servidor[s]["ram"].append(safe_float(m.get("ram")))
        por_servidor[s]["disco"].append(safe_float(m.get("disco")))

    alertas = []
    for servidor_id, series in por_servidor.items():
        for metrica in ("cpu", "ram", "disco"):
            valores = series[metrica]
            if len(valores) < 3:
                continue

            recentes = valores[-JANELA_PREVISAO:]
            limite = safe_float(limites[servidor_id].get(metrica.upper()))
            if limite <= 0:
                continue

            nivel_atual = classificar_gestor(recentes[-1], limite)

            if nivel_atual in ("Critico", "Alto"):
                alertas.append({
                    "servidor_id": servidor_id,
                    "metrica": metrica.upper(),
                    "nivel_previsao": nivel_atual,
                    "mensagem": gerar_mensagem(metrica.upper(), nivel_atual, recentes[-1], limite, servidor_id), #adicionei o id do servidor
                })
                continue

            x = np.arange(len(recentes))
            a, b = np.polyfit(x, recentes, 1)
            previsao = a * len(recentes) + b
            atual = recentes[-1] / limite
            nivel_p = previsao / limite

            if a > 0 and nivel_p > 0.60 and nivel_p > atual:
                nivel_cl = classificar_gestor(previsao, limite)
                if nivel_cl == "Normal":
                    continue
                alertas.append({
                    "servidor_id": servidor_id,
                    "metrica": metrica.upper(),
                    "nivel_previsao": nivel_cl,
                    "mensagem": gerar_mensagem(metrica.upper(), nivel_cl, previsao, limite, servidor_id), # adicionei o id do servidor
                })

    return alertas

def calcular_impacto_componente(leituras, limites):
    leituras = ultimas_leituras_por_servidor(leituras)

    por_servidor = {}
    for r in leituras:
        s = r.get("servidor_id")
        if s not in limites:
            continue

        cpu_lim = safe_float(limites[s].get("CPU"))
        ram_lim = safe_float(limites[s].get("RAM"))
        disco_lim = safe_float(limites[s].get("DISCO"))

        if cpu_lim <= 0 or ram_lim <= 0 or disco_lim <= 0:
            continue

        m = r.get("metricas", {})
        por_servidor.setdefault(s, {"cpu": [], "ram": [], "disco": []})
        por_servidor[s]["cpu"].append(min((safe_float(m.get("cpu")) / cpu_lim) * 100, 100))
        por_servidor[s]["ram"].append(min((safe_float(m.get("ram")) / ram_lim) * 100, 100))
        por_servidor[s]["disco"].append(min((safe_float(m.get("disco")) / disco_lim) * 100, 100))

    medias = {"CPU": [], "RAM": [], "DISCO": []}
    for dados in por_servidor.values():
        if dados["cpu"]:
            medias["CPU"].append(sum(dados["cpu"]) / len(dados["cpu"]))
        if dados["ram"]:
            medias["RAM"].append(sum(dados["ram"]) / len(dados["ram"]))
        if dados["disco"]:
            medias["DISCO"].append(sum(dados["disco"]) / len(dados["disco"]))

    def faixa(v):
        if v >= 80:
            return "Critico"
        if v >= 60:
            return "Alto"
        if v >= 40:
            return "Moderado"
        return "Baixo"

    def media_final(lst):
        return round(sum(lst) / len(lst), 1) if lst else 0

    cpu_f = media_final(medias["CPU"])
    ram_f = media_final(medias["RAM"])
    disco_f = media_final(medias["DISCO"])

    return {
        "CPU": {"valor": cpu_f, "severidade": faixa(cpu_f)},
        "RAM": {"valor": ram_f, "severidade": faixa(ram_f)},
        "DISCO": {"valor": disco_f, "severidade": faixa(disco_f)},
    }

def listar_info_servidores(leituras, limites, servidores, analistas):
    leituras = ultimas_leituras_por_servidor(leituras)
    
    resultado = []
    for srv in servidores:
        sid = srv["id_servidor"]
        if sid not in limites:
            continue

        incidentes = 0
        for r in leituras:
            if r.get("servidor_id") != sid:
                continue
            m = r.get("metricas", {})
            if (
                safe_float(m.get("cpu")) >= safe_float(limites[sid].get("CPU"))
                or safe_float(m.get("ram")) >= safe_float(limites[sid].get("RAM"))
                or safe_float(m.get("disco")) >= safe_float(limites[sid].get("DISCO"))
            ):
                incidentes += 1
        resultado.append({
            "servidor": srv["hostname"],
            "incidentes": incidentes,
            "analistas": analistas.get(sid, 0),
            "status": srv["status_servidor"],
        })
    return resultado

# =========================================================
# PIPELINES
# =========================================================

def preparar_df_metricas(df, empresa_id, servidor_id, hostname):
    df = df.copy()
    if "servidor_id" not in df.columns and "id_servidor" not in df.columns:
        df["servidor_id"] = servidor_id
    if "empresa_id" not in df.columns:
        df["empresa_id"] = empresa_id
    if "hostname" not in df.columns:
        df["hostname"] = hostname
    if "ip" not in df.columns:
        df["ip"] = None
    if "timestamp" in df.columns:
        df = df.sort_values("timestamp")
    return df

def executar_pipeline_metricas(s3, bucket, df, empresa_id, servidor_id_db, hostname, mac_address):
    if df.empty:
        print("CSV metricas vazio.")
        return

    df_metricas = preparar_df_metricas(df, empresa_id, servidor_id_db, hostname)
    col = "servidor_id" if "servidor_id" in df_metricas.columns else "id_servidor"

    trusted_rows = []
    client_json_metricas = []
    alertas = []
    historico_ram = defaultdict(lambda: deque(maxlen=2))
    alertas_por_dia = {}
    alertas_por_servidor = {}
    severidades_detectadas = []

    df_metricas[col] = (
        pandas.to_numeric(df_metricas[col], errors="coerce")
        .fillna(servidor_id_db)
        .astype(int)
    )
    servidores_ids = df_metricas[col].dropna().unique().tolist()
    limites_map = obter_limites_batch(servidores_ids)

    servidor_id_final = None

    for _, row in df_metricas.iterrows():
        row_dict = row.to_dict()
        servidor_id = safe_int(row_dict.get(col), servidor_id_db)
        empresa_row = safe_int(row_dict.get("empresa_id"), empresa_id)
        servidor_id_final = servidor_id

        limites = limites_map.get(servidor_id, {})
        cpu = safe_float(row_dict.get("cpu"))
        ram = safe_float(row_dict.get("ram"))
        disco = safe_float(row_dict.get("disco"))
        ts = timestamp_utc(row_dict.get("timestamp"))
        data_hora = data_hora_str(ts) or data_hora_str(row_dict.get("data_hora")) or str(row_dict.get("timestamp"))

        saude_cpu = 100 - cpu
        saude_ram = 100 - ram
        saude_disco = 100 - disco
        health_score = saude_cpu * 0.40 + saude_ram * 0.40 + saude_disco * 0.20

        criticos = sum(s < 40 for s in [saude_cpu, saude_ram, saude_disco])
        if criticos == 1:
            health_score -= 5
        elif criticos == 2:
            health_score -= 15
        elif criticos == 3:
            health_score -= 25
        health_score = round(max(0, min(100, health_score)), 2)

        status_health = (
            "Estavel" if health_score >= 70 else
            "Atencao" if health_score >= 40 else
            "Critico"
        )

        status_cpu, sev_cpu = classificar_metrica(cpu, limites.get("CPU"))
        status_ram, sev_ram = classificar_metrica(ram, limites.get("RAM"))
        status_disco, sev_disco = classificar_metrica(disco, limites.get("DISCO"))

        trusted_rows.append({
            **row_dict,
            "cpu": f"{cpu}%",
            "ram": f"{ram}%",
            "disco": f"{disco}%",
            "health_score": f"{health_score}%",
            "status_health": status_health,
        })

        historico = historico_ram[servidor_id]
        linha_real = ram
        linha_tendencia = ram
        tendencia_hora = 0

        if len(historico) >= 2 and not pandas.isna(ts):
            ultimo = historico[-1]
            penultimo = historico[-2]
            d_ram = ultimo["ram"] - penultimo["ram"]
            d_tempo = (ultimo["timestamp"] - penultimo["timestamp"]).total_seconds() / 3600
            if d_tempo > 0:
                tendencia_hora = min(max(d_ram / max(d_tempo, 0.1), -100), 100)
                linha_tendencia = round(max(0, min(100, ram + tendencia_hora)), 2)
                tendencia_hora = round(tendencia_hora, 2)

        if not pandas.isna(ts):
            historico.append({"ram": ram, "timestamp": ts})

        client_json_metricas.append({
            "data_hora": data_hora,
            "empresa_id": empresa_row,
            "servidor_id": servidor_id,
            "hostname": row_dict.get("hostname", hostname),
            "ip": row_dict.get("ip"),
            "metricas": {
                "cpu": cpu,
                "ram": ram,
                "disco": disco,
                "heatmap_cpu": classificar(cpu, limites["CPU"]),
                "heatmap_ram": classificar(ram, limites["RAM"]),
                "heatmap_disco": classificar(disco, limites["DISCO"]),
                "health_score": health_score,
                "status_health": status_health,
                "linha_real": linha_real,
                "linha_tendencia": linha_tendencia,
                "tendencia_aumento_hora": tendencia_hora,
            },
            "status_componentes": {
                "cpu": row_dict.get("status_cpu", status_cpu),
                "ram": row_dict.get("status_ram", status_ram),
                "disco": row_dict.get("status_disco", status_disco),
            },
        })

        for componente, valor, status, severidade in [
            ("CPU", cpu, status_cpu, sev_cpu),
            ("RAM", ram, status_ram, sev_ram),
            ("DISCO", disco, status_disco, sev_disco),
        ]:
            if componente in limites:
                limite = limites[componente]
                severidades_detectadas.append(severidade)
                if severidade != "Normal":
                    alertas.append({
                        "data_hora": data_hora,
                        "empresa": empresa_row,
                        "servidor": servidor_id,
                        "componente": componente,
                        "limite": limite,
                        "valor": valor,
                        "status": status,
                        "severidade": severidade,
                    })
                    data = data_hora[:10] if data_hora else "sem_data"
                    alertas_por_dia[data] = alertas_por_dia.get(data, 0) + 1
                    alertas_por_servidor[servidor_id] = alertas_por_servidor.get(servidor_id, 0) + 1

    dia_critico = max(alertas_por_dia, key=alertas_por_dia.get) if alertas_por_dia else None
    servidor_critico = max(alertas_por_servidor, key=alertas_por_servidor.get) if alertas_por_servidor else None

    resumo = {
        "dia_mais_critico": dia_critico,
        "servidor_mais_critico": servidor_critico,
        "total_alertas": len(alertas),
    }

    if servidor_id_final is not None:
        status_final = determinar_status_servidor(severidades_detectadas)
        atualizar_status_servidor(servidor_id_final, status_final)

    df_trusted = pandas.DataFrame(trusted_rows)

    salvar_s3_unificado(
        s3,
        montar_path("trusted", empresa_id, mac_address, "metricas_trusted", extensao="csv"),
        df_trusted,
        formato="csv",
        bucket=bucket,
    )
    salvar_s3_unificado(
        s3,
        montar_path("client", empresa_id, mac_address, "metricas"),
        client_json_metricas,
        formato="json",
        bucket=bucket,
    )
    salvar_s3_unificado(
        s3,
        montar_path("client", empresa_id, mac_address, "metricas", categoria="alertas"),
        alertas,
        formato="json",
        bucket=bucket,
    )
    salvar_s3_unificado(
        s3,
        montar_path("client", empresa_id, mac_address, "metricas", categoria="resumo"),
        resumo,
        formato="json",
        bucket=bucket,
    )

def executar_pipeline_gestor(s3, bucket, empresa_id, mac_address, agora):
    print("\n=== PIPELINE GESTOR ===")
    arquivos = listar_arquivos_client(s3, empresa_id, bucket=bucket)
    
    arquivos_metricas = [
        key for key in arquivos
        if key.endswith("metricas.json")
        and "alertas" not in key
        and "resumo" not in key
    ]

    todas_leituras = []

    for key in arquivos_metricas:
        try:
            dados = ler_json_s3(s3, key, bucket=bucket)
        except Exception as e:
            print(f"[GESTOR] Ignorando {key}: {e}")
            continue

        if isinstance(dados, dict):
            todas_leituras.append(dados)
        elif isinstance(dados, list):
            todas_leituras.extend(dados)

    if not todas_leituras:
        print("[GESTOR] Sem leituras client para consolidar.")
        return
    
    for r in todas_leituras:
        try:
            r["servidor_id"] = int(float(r["servidor_id"]))
        except (ValueError, TypeError, KeyError):
            pass

    servidores_emp = obter_servidores_empresa(empresa_id)
    analistas = obter_analistas_por_servidor(empresa_id)
    limites_emp = {
        int(srv["id_servidor"]): obter_limites_servidor(srv["id_servidor"])
        for srv in servidores_emp
    }

    resultado = {
        "empresa_id": empresa_id,
        "gerado_em": agora.strftime("%Y-%m-%d %H:%M:%S"),
        "periodos": {},
    }

    for periodo in ("24h", "7d", "30d"):
        leituras = filtrar_periodo(todas_leituras, periodo)
        if not leituras:
            resultado["periodos"][periodo] = {"sem_dados": True}
            continue
        resultado["periodos"][periodo] = {
            "kpis": {
                "disponibilidade_global": calcular_disponibilidade(leituras, limites_emp),
                "nivel_risco": calcular_nivel_risco(leituras, limites_emp),
                "incidentes_criticos": calcular_incidentes_criticos(leituras, limites_emp),
                "estabilidade_operacional": calcular_estabilidade_operacional(leituras, limites_emp),
                "tendencia_operacional": calcular_tendencia(leituras, limites_emp),
            },
            "grafico_estabilidade": grafico_estabilidade(leituras, limites_emp),
            "impacto_por_componente": calcular_impacto_componente(leituras, limites_emp),
            "previsao_falhas": calcular_previsao_falhas(leituras, limites_emp),
            "info_servidores": listar_info_servidores(leituras, limites_emp, servidores_emp, analistas),
        }

    salvar_s3_unificado(
        s3,
        f"client/gestor/empresa_{empresa_id}/dashboard_gestor.json",
        resultado,
        formato="json_dashboard",
        bucket=bucket,
    )

# =========================================================
# HANDLER LAMBDA
# =========================================================

def handler(event, context):
    print("Iniciando pipeline unificado (Lambda)...")


    s3_client = boto3.client("s3")
    bucket, key_recebida, mac_address = resolver_evento(event or {})

    if bucket:
        AWS_CONFIG["bucket_name"] = bucket
    bucket = bucket_atual(bucket)

    if not mac_address:
        raise ValueError(
            "mac_address nao informado. Use path raw/empresa_{id}/{mac_address}/arquivo.csv, "
            "event['mac_address'] ou variavel MAC_ADDRESS."
        )

    servidor = obter_servidor_por_mac(mac_address)

    if not servidor:
        raise ValueError(f"Servidor nao encontrado para mac_address={mac_address}")

    empresa_id = servidor["fk_empresa"]
    servidor_id = servidor["id_servidor"]
    hostname = servidor["hostname"]
    agora = pandas.Timestamp.now(tz="UTC")

    raw_key = chave_raw_metrics(
        key_recebida,
        empresa_id,
        mac_address
    )

    flights_key = chave_raw_flights(
        key_recebida,
        empresa_id,
        mac_address
    )

    processos_key = chave_raw_processos(
        key_recebida,
        empresa_id,
        mac_address
    )

    print(f"[ENTRADA] bucket={bucket}")
    print(f"[ENTRADA] key={key_recebida}")
    print(f"[ENTRADA] mac_address={mac_address}")
    print(f"[ENTRADA] raw_metrics={raw_key}")
    print(f"[ENTRADA] raw_flights={flights_key}")
    print(s3_client)
    
    df = limpar_dados(ler_csv(s3_client, raw_key, bucket))
    
    # =====================================================
    # PIPELINE SCORE
    # =====================================================
    print("\n=== PIPELINE SCORE SERVIDOR ===")
    if not df.empty:
        hostname_score = hostname
        if "hostname" in df.columns and hostname not in df["hostname"].astype(str).tolist():
            servidores_score = df["hostname"].dropna().unique().tolist()
            hostname_score = servidores_score[0] if servidores_score else hostname

        client_score_json = calcular_score_servidor(df, hostname_score, servidor_id)
        salvar_s3_unificado(
            s3_client,
            montar_path("client", empresa_id, mac_address, "calcularIndice"),
            client_score_json,
            formato="json_dashboard",
            bucket=bucket,
        )
        print("Score de servidores gerado com sucesso.")
    else:
        print("CSV vazio para score.")

    # =====================================================
    # PIPELINE TEMPERATURA
    # =====================================================
    print("\n=== PIPELINE TEMPERATURA ===")
    if not df.empty:
        df_temp = df.copy()
        df_temp["alertas"] = df_temp.apply(gerar_alerta, axis=1)
        df_temp["quantidade_alertas"] = df_temp["alertas"].apply(
            lambda x: len([a for a in x.split("|") if a.strip()]) if x else 0,
        )
        dia_mais_alertas = calcular_dia_mais_alertas(df_temp)

        client_temp_json = {
            "empresa_id": empresa_id,
            "servidor_id": servidor_id,
            "hostname": hostname,
            "mac_address": mac_address,
            "dia_com_mais_alertas": dia_mais_alertas,
            "total_registros": int(len(df_temp)),
            "total_alertas": int(df_temp["quantidade_alertas"].sum()),
            "dados": df_temp.to_dict(orient="records"),
        }

        salvar_s3_unificado(
            s3_client,
            montar_path("trusted", empresa_id, mac_address, "temperatura_trusted", extensao="csv"),
            df_temp.copy(),
            formato="csv",
            bucket=bucket,
        )
        salvar_s3_unificado(
            s3_client,
            montar_path("client", empresa_id, mac_address, "client_metrics"),
            client_temp_json,
            formato="json_dashboard",
            bucket=bucket,
        )
        print("Temperatura processada com sucesso.")
    else:
        print("CSV temperatura vazio.")

    # =====================================================
    # PIPELINE PROCESSOS
    # =====================================================
    print("\n=== PIPELINE PROCESSOS ===")

    df_processos_raw = limpar_dados(ler_csv(s3_client, processos_key, bucket))
    
    if not df_processos_raw.empty:
        df_processos = processos_tratados_s3(
                df_processos_raw
            )
        print("\n=== DF PROCESSOS ===")
        print(df_processos.columns.tolist())

        print(df_processos[[
            "pid",
            "nome",
            "status",
            "cpu",
            "ram_percent",
            "latencia_ms",
            "criticidade"
        ]].head(10))
        raw_criticos_4h = gerar_raw_criticos_4h(df_processos)

        kpis = {}
        kpis.update(top5cpu(df_processos))
        kpis.update(top5ram(df_processos))
        kpis.update(processos_criticos(df_processos))
        kpis.update(maior_latencia(df_processos))
        kpis.update(limites_processos(df_processos))
        kpis.update(contar_status(df_processos))
        kpis.update(contar_criticos(df_processos))


        total_criticos = 0
        maior_lat = 0
        if not df_processos.empty:
            total_criticos = int((df_processos["criticidade"] == "Critico").sum())
            maior_lat = safe_float(df_processos["latencia_ms"].max())

        client_processos_json = {
            "empresa_id": empresa_id,
            "servidor_id": servidor_id,
            "hostname": hostname,
            "mac_address": mac_address,
            "total_processos": int(len(df_processos)),
            "processos_criticos": total_criticos,
            "maior_latencia": maior_lat,
            "kpis": kpis,
            "dados": df_processos.to_dict(orient="records"),
        }


        salvar_s3_unificado(
            s3_client,
            montar_path("trusted", empresa_id, mac_address, "processos_trusted", extensao="csv"),
            df_processos,
            formato="csv",
            bucket=bucket,
        )
        salvar_s3_unificado(
            s3_client,
            montar_path("client", empresa_id, mac_address, "process_raw_kpis"),
            client_processos_json,
            formato="json_dashboard",
            bucket=bucket,
        )
        salvar_s3_unificado(
            s3_client,
            montar_path("client", empresa_id, mac_address, "raw_criticos_4h"),
            raw_criticos_4h,
            formato="json_dashboard",
            bucket=bucket,
        )
        print("Processos processados com sucesso.")
    else:
        print("CSV processos vazio.")

    # =====================================================
    # PIPELINE RAW -> CLIENT / ALERTAS
    # =====================================================
    print("\n=== PIPELINE RAW -> CLIENT/ALERTAS ===")
    executar_pipeline_metricas(
        s3_client,
        bucket,
        df,
        empresa_id,
        servidor_id,
        hostname,
        mac_address,
    )

    # =====================================================
    # PIPELINE COMUNICACAO
    # =====================================================
    print("\n=== PIPELINE COMUNICACAO ===")
    df_network = limpar_dados(df.copy())
    df_flights = limpar_voos(ler_csv(s3_client, flights_key, bucket=bucket))

    df_n_24h = enriquecer_dados(
        filtrar_df_por_tempo(df_network, "timestamp", agora, pandas.Timedelta(hours=24)),
    )
    df_v_24h = filtrar_df_por_tempo(df_flights, "timestamp_coleta", agora, pandas.Timedelta(hours=24))

    incidentes = detectar_incidentes(df_n_24h)

    salvar_s3_unificado(
        s3_client,
        montar_path("trusted", empresa_id, mac_address, "network_trusted", extensao="csv"),
        df_network,
        formato="csv",
        bucket=bucket,
    )
    salvar_s3_unificado(
        s3_client,
        montar_path("trusted", empresa_id, mac_address, "flights_trusted", extensao="csv"),
        df_flights,
        formato="csv",
        bucket=bucket,
    )

    for periodo in ("24h", "3d", "7d"):
        if periodo == "24h":
            td = pandas.Timedelta(hours=24)
        elif periodo == "3d":
            td = pandas.Timedelta(days=3)
        else:
            td = pandas.Timedelta(days=7)

        df_n = enriquecer_dados(filtrar_df_por_tempo(df_network, "timestamp", agora, td))
        df_v = filtrar_df_por_tempo(df_flights, "timestamp_coleta", agora, td)
        dash = gerar_json_dashboard(df_n, df_v, periodo)
        salvar_s3_unificado(
            s3_client,
            montar_path("client", empresa_id, mac_address, f"dashboard_rede_{periodo}"),
            dash,
            formato="json",
            bucket=bucket,
        )

    salvar_s3_unificado(
        s3_client,
        montar_path("client", empresa_id, mac_address, "incidentes_rede_24h"),
        incidentes,
        formato="json",
        bucket=bucket,
    )

    executar_pipeline_gestor(s3_client, bucket, empresa_id, mac_address, agora)

    print("\nPipeline unificado executado com sucesso.")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Pipeline executado com sucesso",
            "mac_address": mac_address,
            "hostname": hostname,
        }),
    }

def run_etl(bucket=None, key=None, mac_address=None):
    return handler(
        {
            "bucket": bucket,
            "key": key,
            "mac_address": mac_address,
        },
        None,
    )

def lambda_handler(event, context):
    return handler(event, context)
