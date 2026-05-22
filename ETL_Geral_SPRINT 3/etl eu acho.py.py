import math
from datetime import datetime, timezone
import boto3
import mysql.connector
import pandas as pd
from getmac import get_mac_address
from io import StringIO
from collections import Counter
import uuid
import json
import os
import tempfile
from dotenv import load_dotenv

load_dotenv()
env = os.getenv

# ── AWS ───────────────────────────────────────────────────────────────────────
AWS_CONFIG = {
    "aws_access_key_id":     env("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": env("AWS_SECRET_ACCESS_KEY"),
    "aws_session_token":     env("AWS_SESSION_TOKEN"),
    "region_name":           env("AWS_REGION_NAME"),
    "bucket_name":           env("AWS_BUCKET_NAME"),
}

# ── Banco de dados ────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     env("DB_HOST"),
    "user":     env("DB_USER"),
    "password": env("DB_PASSWORD"),
    "database": env("DB_DATABASE"),
}

# ── Limiares de alerta — CPU / RAM / Disco ────────────────────────────────────
LIMITE_CPU_CRITICO   = 90.0
LIMITE_CPU_ALTO      = 80.0
LIMITE_CPU_MEDIO     = 70.0
LIMITE_CPU_BAIXO     = 60.0

LIMITE_RAM_CRITICO   = 90.0
LIMITE_RAM_ALTO      = 80.0
LIMITE_RAM_MEDIO     = 70.0
LIMITE_RAM_BAIXO     = 60.0

LIMITE_DISCO_CRITICO = 90.0
LIMITE_DISCO_ALTO    = 80.0
LIMITE_DISCO_MEDIO   = 70.0
LIMITE_DISCO_BAIXO   = 60.0

# ── Limiares de temperatura ───────────────────────────────────────────────────
LIMITE_TEMP_CRITICO  = 90.0
LIMITE_TEMP_ALTO     = 80.0
LIMITE_TEMP_MEDIO    = 70.0

# ── Limiares de latência (ms) ─────────────────────────────────────────────────
LIMITE_LAT_CRITICO   = 250.0
LIMITE_LAT_ALTO      = 200.0
LIMITE_LAT_MEDIO     = 150.0
LIMITE_LAT_BAIXO     = 100.0

# ── Limiares de perda de pacotes (%) ─────────────────────────────────────────
LIMITE_LOSS_CRITICO  = 20.0
LIMITE_LOSS_ALTO     = 15.0
LIMITE_LOSS_MEDIO    = 10.0
LIMITE_LOSS_BAIXO    = 5.0

# ── Limiares de processos ─────────────────────────────────────────────────────
PROC_CPU_CRITICA      = 80.0
PROC_CPU_ALERTA       = 50.0
PROC_RAM_CRITICA      = 20.0
PROC_RAM_ALERTA       = 10.0
PROC_LATENCIA_CRITICA = 100.0
PROC_LATENCIA_ALERTA  = 50.0

# ── Criticidade padronizada ───────────────────────────────────────────────────
CRITICIDADES   = ("normal", "baixo", "medio", "alto", "critico")
STATUS_ALERTA  = ("ABERTO", "RESOLVIDO")
ORIGENS_VALIDAS = ("CPU", "RAM", "DISCO", "PROCESSO", "REDE", "TEMPERATURA")

PRIORIDADE_SEVERIDADE = {
    "critico": 5,
    "alto":    4,
    "medio":   3,
    "baixo":   2,
    "normal":  1,
}

# ── S3 ────────────────────────────────────────────────────────────────────────

def conectar_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_CONFIG["aws_access_key_id"],
        aws_secret_access_key=AWS_CONFIG["aws_secret_access_key"],
        aws_session_token=AWS_CONFIG["aws_session_token"],
        region_name=AWS_CONFIG["region_name"],
    )


def listar_csvs_s3(s3, prefix="raw/"):
    """Retorna lista de chaves .csv sob o prefix informado."""
    resp = s3.list_objects_v2(Bucket=AWS_CONFIG["bucket_name"], Prefix=prefix)
    return [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".csv")]


def ler_csv_s3(s3, key) -> pd.DataFrame:
    obj = s3.get_object(Bucket=AWS_CONFIG["bucket_name"], Key=key)
    return pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")), on_bad_lines="skip")


def salvar_s3(s3, conteudo: str, key: str):
    s3.put_object(Bucket=AWS_CONFIG["bucket_name"], Key=key, Body=conteudo)


def arquivo_existe_s3(s3, key: str) -> bool:
    try:
        s3.head_object(Bucket=AWS_CONFIG["bucket_name"], Key=key)
        return True
    except Exception:
        return False


# ── MySQL ─────────────────────────────────────────────────────────────────────

def conectar_db():
    return mysql.connector.connect(**DB_CONFIG)


def obter_servidor(mac_address: str) -> dict | None:
    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT id_servidor, hostname, fk_empresa FROM servidor WHERE mac_address = %s",
        (mac_address,),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row


def obter_limites(servidor_id: int) -> dict:
    """Retorna {tipo: limite} para o servidor informado."""
    conn = conectar_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT c.tipo, sc.limite
        FROM servidor_componente sc
        JOIN componente c ON sc.fk_componente = c.id_componente
        WHERE sc.fk_servidor = %s
        """,
        (servidor_id,),
    )
    limites = {row[0]: float(row[1]) for row in cursor.fetchall()}
    cursor.close()
    conn.close()
    return limites


def atualizar_status_servidor(servidor_id: int, novo_status: str):
    conn = conectar_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT status_servidor FROM servidor WHERE id_servidor = %s",
        (servidor_id,),
    )
    atual = cursor.fetchone()
    if atual and atual[0] != novo_status:
        cursor.execute(
            """
            UPDATE servidor
            SET status_servidor = %s, data_status = CURRENT_TIMESTAMP
            WHERE id_servidor = %s
            """,
            (novo_status, servidor_id),
        )
        conn.commit()
        print(f"[DB] Status atualizado: {atual[0]} → {novo_status}")
    cursor.close()
    conn.close()


# ── Identificação local ───────────────────────────────────────────────────────

def coletar_mac() -> str:
    return get_mac_address()

# ── Envelope JSON padrão ──────────────────────────────────────────────────────

def json_envelope(periodo: str, servidor: str, dados: list | dict) -> dict:
    """
    Estrutura obrigatória para todos os JSONs exportados.
    Se `dados` for dict (KPIs escalares), é embrulhado numa lista unitária.
    """
    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "periodo":   periodo,
        "servidor":  servidor,
        "dados":     dados if isinstance(dados, list) else [dados],
    }


def serializar_json(obj) -> str:
    return json.dumps(obj, indent=4, ensure_ascii=False, default=str)


# ── Classificadores de criticidade ───────────────────────────────────────────

def classificar_latencia(valor: float) -> str:
    valor = float(valor)
    if valor > LIMITE_LAT_CRITICO:  return "critico"
    if valor > LIMITE_LAT_ALTO:     return "alto"
    if valor > LIMITE_LAT_MEDIO:    return "medio"
    if valor > LIMITE_LAT_BAIXO:    return "baixo"
    return "normal"


def classificar_pacotes(valor: float) -> str:
    valor = float(valor)
    if valor > LIMITE_LOSS_CRITICO:  return "critico"
    if valor > LIMITE_LOSS_ALTO:     return "alto"
    if valor > LIMITE_LOSS_MEDIO:    return "medio"
    if valor > LIMITE_LOSS_BAIXO:    return "baixo"
    return "normal"


def classificar_cpu(valor: float) -> str:
    valor = float(valor)
    if valor >= LIMITE_CPU_CRITICO:  return "critico"
    if valor >= LIMITE_CPU_ALTO:     return "alto"
    if valor >= LIMITE_CPU_MEDIO:    return "medio"
    if valor >= LIMITE_CPU_BAIXO:    return "baixo"
    return "normal"


def classificar_ram(valor: float) -> str:
    valor = float(valor)
    if valor >= LIMITE_RAM_CRITICO:  return "critico"
    if valor >= LIMITE_RAM_ALTO:     return "alto"
    if valor >= LIMITE_RAM_MEDIO:    return "medio"
    if valor >= LIMITE_RAM_BAIXO:    return "baixo"
    return "normal"


def classificar_disco(valor: float) -> str:
    valor = float(valor)
    if valor >= LIMITE_DISCO_CRITICO:  return "critico"
    if valor >= LIMITE_DISCO_ALTO:     return "alto"
    if valor >= LIMITE_DISCO_MEDIO:    return "medio"
    if valor >= LIMITE_DISCO_BAIXO:    return "baixo"
    return "normal"


def classificar_temperatura(valor: float) -> str:
    valor = float(valor)
    if valor > 90:  return "critico"
    if valor > 80:  return "alto"
    if valor > 70:  return "medio"
    if valor > 60:  return "baixo"
    return "normal"


# ── Determinação de severidade agregada ───────────────────────────────────────

def pior_severidade(severidades: list[str]) -> str:
    """Retorna a severidade mais grave de uma lista."""
    if not severidades:
        return "normal"
    return max(severidades, key=lambda s: PRIORIDADE_SEVERIDADE.get(s, 0))


def determinar_status_servidor(severidades: list[str]) -> str:
    pior = pior_severidade(severidades)
    mapa = {
        "critico": "Crítico",
        "alto":    "Crítico",
        "medio":   "Atenção",
        "baixo":   "Online",
        "normal":  "Online",
    }
    return mapa.get(pior, "Online")


# ── Classificação de score de saúde ──────────────────────────────────────────

def classificar_score(score: float) -> str:
    if score >= 90:  return "Saudável"
    if score >= 80:  return "Atenção"
    return "Crítico"


# ── KPI de atualização ADS-B ──────────────────────────────────────────────────

def kpi_adsb_update(media: float) -> float:
    """Índice percentual de atualização ADS-B (0–100)."""
    if media <= 2:
        return round(100 - media * 2, 1)
    return round(max(96 * math.exp(-(media - 2) / 20), 0), 1)


# ── Faixas de 4h para agrupamento de incidentes ───────────────────────────────

FAIXAS_4H = [
    ("00h-04h", 0,  4),
    ("04h-08h", 4,  8),
    ("08h-12h", 8,  12),
    ("12h-16h", 12, 16),
    ("16h-20h", 16, 20),
    ("20h-24h", 20, 24),
]


def faixa_4h(hora: int) -> str:
    for label, inicio, fim in FAIXAS_4H:
        if inicio <= hora < fim:
            return label
    return "20h-24h"

"""
trusted.py — Camada Trusted: limpeza, tipagem e enriquecimento do raw.csv.

Responsabilidades:
  - Converter tipos numéricos e datas
  - Preencher nulos
  - Adicionar colunas derivadas (status_*, alertas, labels de tempo)
  - Exportar trusted_metrics.csv para S3
"""

# ── Colunas numéricas do raw ──────────────────────────────────────────────────

COLUNAS_NUMERICAS = [
    "cpu", "ram", "disco", "health_score",
    "temp_max_cpu", "margem_termica", "temperatura_ambiente",
    "umidade", "fan_principal_rpm", "indice_resfriamento",
    "bytes_recv", "bytes_sent", "pack_recv", "pack_sent",
    "packet_loss_internet",
    "latency_min_ms", "latency_avg_ms", "latency_max_ms",
    "lat_adsb_rastreamento", "lat_rastreamento_correlacao",
    "lat_correlacao_rotas", "lat_rotas_api", "lat_api_bd", "lat_bd_sync",
    "rastreamento_mbps", "rotas_mbps", "correlacao_mbps",
    "api_gateway_mbps", "bd_mbps", "sync_service_mbps",
    "rastreamento_loss", "correlacao_loss", "rotas_loss",
    "api_loss", "bd_loss", "sync_loss",
    "total_aeronaves", "avg_adsb_update_seconds",
]


# ── Limpeza base ──────────────────────────────────────────────────────────────

def limpar_dados(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
 
    for col in COLUNAS_NUMERICAS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
 
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
 
    if "opensky_timestamp" in df.columns:
        df["opensky_timestamp"] = pd.to_datetime(df["opensky_timestamp"], utc=True)
 
    # Labels de agrupamento temporal
    df["label_24h"] = df["timestamp"].dt.strftime("%H:%M")
    df["label_3d"]  = df["timestamp"].dt.strftime("%d/%m %Hh")
    df["label_7d"]  = df["timestamp"].dt.strftime("%d/%m")
 
    return df


# ── Enriquecimento: colunas de status ─────────────────────────────────────────

def enriquecer_dados(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
 
    # CPU / RAM / Disco
    df["status_cpu"]   = df["cpu"].apply(classificar_cpu)
    df["status_ram"]   = df["ram"].apply(classificar_ram)
    df["status_disco"] = df["disco"].apply(classificar_disco)
 
    # Temperatura
    df["status_temp_max"] = df["temp_max_cpu"].apply(classificar_temperatura)
 
    # Latência por serviço
    df["status_latency_avg"]         = df["latency_avg_ms"].apply(classificar_latencia)
    df["status_adsb"]                = df["lat_adsb_rastreamento"].apply(classificar_latencia)
    df["status_rastreamento_correl"] = df["lat_rastreamento_correlacao"].apply(classificar_latencia)
    df["status_correlacao_rotas"]    = df["lat_correlacao_rotas"].apply(classificar_latencia)
    df["status_rotas_api"]           = df["lat_rotas_api"].apply(classificar_latencia)
    df["status_api_bd"]              = df["lat_api_bd"].apply(classificar_latencia)
    df["status_bd_sync"]             = df["lat_bd_sync"].apply(classificar_latencia)
 
    # Perda de pacotes por serviço
    df["status_packet_loss"]       = df["packet_loss_internet"].apply(classificar_pacotes)
    df["status_rastreamento_loss"] = df["rastreamento_loss"].apply(classificar_pacotes)
    df["status_correlacao_loss"]   = df["correlacao_loss"].apply(classificar_pacotes)
    df["status_rotas_loss"]        = df["rotas_loss"].apply(classificar_pacotes)
    df["status_api_loss"]          = df["api_loss"].apply(classificar_pacotes)
    df["status_bd_loss"]           = df["bd_loss"].apply(classificar_pacotes)
    df["status_sync_loss"]         = df["sync_loss"].apply(classificar_pacotes)
 
    # Severidade agregada por domínio
    cols_lat  = ["status_latency_avg","status_adsb","status_correlacao_rotas",
                 "status_rotas_api","status_api_bd","status_bd_sync"]
    cols_loss = ["status_packet_loss","status_rastreamento_loss","status_correlacao_loss",
                 "status_rotas_loss","status_api_loss","status_bd_loss","status_sync_loss"]
 
    df["status_servidor_latencia"] = df[cols_lat].apply(
        lambda row: pior_severidade(row.tolist()), axis=1
    )
    df["status_servidor_pacotes"] = df[cols_loss].apply(
        lambda row: pior_severidade(row.tolist()), axis=1
    )
 
    # Alertas textuais (mantém compatibilidade com lógica existente)
    df["alertas"]           = df.apply(_gerar_alerta, axis=1)
    df["quantidade_alertas"] = df["alertas"].apply(
        lambda x: len(x.split("|")) if x else 0
    )
 
    return df


def _gerar_alerta(row) -> str:
    alertas = []
    if str(row.get("status_temperatura", "")).lower() in ("critical", "medium", "alert"):
        alertas.append("Temperatura CPU elevada")
    if str(row.get("status_margem", "")).lower() in ("critica", "atencao", "throttling"):
        alertas.append("Margem térmica crítica")
    if str(row.get("status_resfriamento", "")).lower() in ("critica", "atencao"):
        alertas.append("Resfriamento ineficiente")
    if str(row.get("throttling", "")).lower() == "sim":
        alertas.append("CPU em throttling")
    return " | ".join(alertas)
 
 
# ── Exportação para CSV ───────────────────────────────────────────────────────
 
def exportar_trusted_csv(df: pd.DataFrame) -> str:
    buf = StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()
 
 
# ── Pipeline trusted completa ─────────────────────────────────────────────────
 
def processar_trusted(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = limpar_dados(df_raw)
    df = enriquecer_dados(df)
    return df

"""
analytics.py — Geração dos JSONs de CPU / RAM / Disco.

JSONs gerados:
  analytics/recursos_dashboard_24h.json
  analytics/recursos_dashboard_3d.json
  analytics/recursos_dashboard_7d.json
  analytics/disponibilidade_servidor.json
  analytics/painel_saude.json
"""
# ── Filtragem por período ─────────────────────────────────────────────────────

def filtrar_periodo(df: pd.DataFrame, periodo: str) -> pd.DataFrame:
    agora = df["timestamp"].max()
    deltas = {"24h": pd.Timedelta(hours=24),
              "3d":  pd.Timedelta(days=3),
              "7d":  pd.Timedelta(days=7)}
    return df[df["timestamp"] >= agora - deltas[periodo]]


# ── Agrupamento temporal ──────────────────────────────────────────────────────

def agrupar_periodo(df: pd.DataFrame, periodo: str) -> pd.DataFrame:
    df = df.copy()
    fmt = {"24h": "%H:%M", "3d": "%d/%m %Hh", "7d": "%d/%m"}
    df["grupo"] = df["timestamp"].dt.strftime(fmt[periodo])

    colunas_agg = {c: "mean" for c in ["cpu","ram","disco","health_score",
                   "bytes_recv","bytes_sent"] if c in df.columns}
    # swap aproximado = ram disponível acima do limite
    if "ram" in df.columns:
        df["swap_estimado"] = df["ram"].clip(upper=100)
        colunas_agg["swap_estimado"] = "mean"

    return df.groupby("grupo").agg(colunas_agg).reset_index()


# ── Predição RAM linear simples ───────────────────────────────────────────────

def _predicao_ram(df_agr: pd.DataFrame) -> dict:
    if len(df_agr) < 2 or "ram" not in df_agr.columns:
        return {"labels": [], "valores": []}
    valores = df_agr["ram"].tolist()
    labels  = df_agr["grupo"].tolist()
    n       = len(valores)
    delta   = valores[-1] - valores[-2] if n >= 2 else 0
    pred_vals   = [round(min(100, valores[-1] + delta * (i + 1)), 2) for i in range(3)]
    pred_labels = [f"pred+{i+1}" for i in range(3)]
    return {"labels": labels + pred_labels, "valores": valores + pred_vals}


# ── JSON recursos_dashboard_{periodo} ────────────────────────────────────────

def gerar_recursos_dashboard(df: pd.DataFrame, servidor: str, periodo: str) -> dict:
    df_p   = filtrar_periodo(df, periodo)
    df_agr = agrupar_periodo(df_p, periodo)

    cpu_vals   = df_agr["cpu"].tolist()   if "cpu"   in df_agr.columns else []
    ram_vals   = df_agr["ram"].tolist()   if "ram"   in df_agr.columns else []
    disco_vals = df_agr["disco"].tolist() if "disco" in df_agr.columns else []
    swap_vals  = df_agr["swap_estimado"].tolist() if "swap_estimado" in df_agr.columns else []

    cpu_media   = round(df_p["cpu"].mean(),   2) if "cpu"   in df_p.columns else 0
    ram_media   = round(df_p["ram"].mean(),   2) if "ram"   in df_p.columns else 0
    disco_media = round(df_p["disco"].mean(), 2) if "disco" in df_p.columns else 0
    swap_media  = round(df_p["ram"].mean(),   2) if "ram"   in df_p.columns else 0

    cpu_pico   = round(df_p["cpu"].max(),   2) if "cpu"   in df_p.columns else 0
    ram_pico   = round(df_p["ram"].max(),   2) if "ram"   in df_p.columns else 0
    disco_pico = round(df_p["disco"].max(), 2) if "disco" in df_p.columns else 0

    saude_media  = round(df_p["health_score"].mean(), 2) if "health_score" in df_p.columns else 0
    severidades  = (
        df_p[["cpu","ram","disco"]].apply(
            lambda r: pior_severidade([
                classificar_cpu(r["cpu"]),
                classificar_ram(r["ram"]),
                classificar_disco(r["disco"]),
            ]),
            axis=1,
        ).tolist()
        if all(c in df_p.columns for c in ["cpu","ram","disco"])
        else []
    )
    status_srv = determinar_status_servidor(severidades)

    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "periodo":   periodo,
        "servidor":  servidor,
        "kpis": {
            "cpu_media":      cpu_media,
            "cpu_pico":       cpu_pico,
            "ram_media":      ram_media,
            "ram_pico":       ram_pico,
            "disco_media":    disco_media,
            "disco_pico":     disco_pico,
            "swap_media":     swap_media,
            "saude_servidor": saude_media,
            "status_servidor": status_srv,
        },
        "historico": {
            "labels": df_agr["grupo"].tolist(),
            "cpu":    [round(v, 2) for v in cpu_vals],
            "ram":    [round(v, 2) for v in ram_vals],
            "disco":  [round(v, 2) for v in disco_vals],
            "swap":   [round(v, 2) for v in swap_vals],
        },
        "predicao_ram": _predicao_ram(df_agr),
    }


# ── JSON disponibilidade_servidor ─────────────────────────────────────────────

def gerar_disponibilidade_servidor(df: pd.DataFrame, servidor: str) -> dict:
    total = len(df)
    if total == 0:
        return {"servidor": servidor, "uptime_percentual": 0,
                "tempo_online_minutos": 0, "tempo_offline_minutos": 0,
                "ultima_atualizacao": ""}

    # Considera "online" registros com health_score >= 70
    if "health_score" in df.columns:
        online = int((df["health_score"] >= 70).sum())
    else:
        online = total

    offline = total - online
    intervalo_min = 1  # assume coleta a cada 1 min — ajuste se necessário

    return {
        "gerado_em":             datetime.now(timezone.utc).isoformat(),
        "servidor":              servidor,
        "uptime_percentual":     round(online / total * 100, 2),
        "tempo_online_minutos":  online  * intervalo_min,
        "tempo_offline_minutos": offline * intervalo_min,
        "ultima_atualizacao":    str(df["timestamp"].max()),
    }


# ── JSON painel_saude ─────────────────────────────────────────────────────────

def gerar_painel_saude(df: pd.DataFrame) -> dict:
    """
    Classifica cada registro em online / atencao / critico / offline
    e retorna a contagem.
    """
    if "health_score" not in df.columns:
        return {"gerado_em": datetime.now(timezone.utc).isoformat(),
                "online": 0, "atencao": 0, "critico": 0, "offline": 0}

    contagem = {"online": 0, "atencao": 0, "critico": 0, "offline": 0}
    for score in df["health_score"]:
        if score == 0:
            contagem["offline"] += 1
        elif score >= 90:
            contagem["online"]  += 1
        elif score >= 70:
            contagem["atencao"] += 1
        else:
            contagem["critico"] += 1

    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        **contagem,
    }

"""
rede.py — Geração dos JSONs de rede.

JSONs gerados:
  analytics/rede_dashboard_24h.json
  analytics/rede_dashboard_3d.json
  analytics/rede_dashboard_7d.json
  analytics/rede_horarios_criticos.json

Reutiliza agrupar_periodo de analytics.py para não duplicar lógica.
"""

# ── KPIs de rede ──────────────────────────────────────────────────────────────

def _kpi_perda_media(df: pd.DataFrame) -> float:
    colunas = [c for c in [
        "packet_loss_internet","rastreamento_loss","correlacao_loss",
        "rotas_loss","api_loss","bd_loss","sync_loss"
    ] if c in df.columns]
    return round(df[colunas].mean().mean(), 2) if colunas else 0.0


def _kpi_latencia_media(df: pd.DataFrame) -> float:
    colunas = [c for c in [
        "latency_avg_ms","lat_adsb_rastreamento","lat_rastreamento_correlacao",
        "lat_correlacao_rotas","lat_rotas_api","lat_api_bd","lat_bd_sync"
    ] if c in df.columns]
    return round(df[colunas].mean().mean(), 2) if colunas else 0.0


def _taxa_transferencia(df: pd.DataFrame) -> float:
    if "bytes_recv" not in df.columns or "bytes_sent" not in df.columns:
        return 0.0
    return round(((df["bytes_recv"] + df["bytes_sent"]) / (1024 * 1024)).mean(), 2)


def _consumo_banda(df: pd.DataFrame) -> dict:
    mapa = {"Rastreamento": "rastreamento_mbps", "Rotas": "rotas_mbps",
            "Correlacao":   "correlacao_mbps",   "API Gateway": "api_gateway_mbps",
            "Banco de Dados": "bd_mbps",          "Sync Service": "sync_service_mbps"}
    return {k: round(df[v].mean(), 2) for k, v in mapa.items() if v in df.columns}


def _perda_pacotes_servico(df: pd.DataFrame) -> dict:
    mapa = {"Rastreamento": "rastreamento_loss", "Rotas": "rotas_loss",
            "Correlação":   "correlacao_loss",   "API Gateway": "api_loss",
            "Banco de Dados": "bd_loss",          "Sync Service": "sync_loss"}
    return {k: round(df[v].mean(), 2) for k, v in mapa.items() if v in df.columns}


def _latencia_componentes(df: pd.DataFrame) -> dict:
    mapa = {"ADS-B":       "lat_adsb_rastreamento",
            "Correlação":  "lat_rastreamento_correlacao",
            "Rotas":       "lat_rotas_api",
            "Banco de Dados": "lat_api_bd",
            "Sync Service": "lat_bd_sync"}
    return {k: round(df[v].mean(), 2) for k, v in mapa.items() if v in df.columns}


# ── Agrupamento temporal para rede ────────────────────────────────────────────

def _agrupar_rede(df, periodo):
    df = df.copy()

    if periodo == "24h":
        df["grupo"] = df["timestamp"].dt.floor("15min")

    elif periodo == "3d":
        df["grupo"] = df["timestamp"].dt.floor("1h")

    elif periodo == "7d":
        df["grupo"] = df["timestamp"].dt.floor("6h")

    colunas_num = [
        c for c in df.select_dtypes(include="number").columns
        if c not in ("empresa_id", "servidor_id")
    ]

    return df.groupby("grupo")[colunas_num].mean().reset_index()


def _filtrar_periodo(df, periodo):
    df = df.copy()

    agora = df["timestamp"].max()  

    deltas = {
        "24h": pd.Timedelta(hours=24),
        "3d": pd.Timedelta(days=3),
        "7d": pd.Timedelta(days=7)
    }

    return df[df["timestamp"] >= agora - deltas[periodo]]


# ── JSON rede_dashboard_{periodo} ────────────────────────────────────────────

def gerar_rede_dashboard(df: pd.DataFrame, servidor: str, periodo: str) -> dict:
    df_p   = _filtrar_periodo(df, periodo)
    df_agr = _agrupar_rede(df_p, periodo)

    rastreamento = df_agr["rastreamento_mbps"].round(2).tolist() if "rastreamento_mbps" in df_agr.columns else []
    rotas        = df_agr["rotas_mbps"].round(2).tolist()        if "rotas_mbps"        in df_agr.columns else []
    correlacao   = df_agr["correlacao_mbps"].round(2).tolist()   if "correlacao_mbps"   in df_agr.columns else []

    adsb_media = df_p["avg_adsb_update_seconds"].mean() if "avg_adsb_update_seconds" in df_p.columns else 0

    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "periodo":   periodo,
        "servidor":  servidor,
        "kpis": {
            "perda_pacotes":          _kpi_perda_media(df_p),
            "latencia_media":         _kpi_latencia_media(df_p),
            "adsb_update":            kpi_adsb_update(adsb_media),
            "taxa_transferencia":     _taxa_transferencia(df_p),
        },
        "grafico_transferencia": {
            "labels":       df_agr["grupo"].tolist(),
            "rastreamento": rastreamento,
            "rotas":        rotas,
            "correlacao":   correlacao,
        },
        "grafico_latencia_componentes": _latencia_componentes(df_agr),
        "consumo_banda":                _consumo_banda(df_agr),
        "perda_pacotes_servico":        _perda_pacotes_servico(df_agr),
    }


# ── JSON rede_horarios_criticos ───────────────────────────────────────────────

def gerar_rede_horarios_criticos(df: pd.DataFrame, servidor: str) -> dict:
    """
    Identifica horários com maior incidência de alertas de rede nas últimas 24h.
    """
    agora  = df["timestamp"].max()
    df24   = df[df["timestamp"] >= agora - pd.Timedelta(hours=24)].copy()

    colunas_status = [c for c in [
        "status_packet_loss","status_latency_avg","status_adsb",
        "status_correlacao_rotas","status_rotas_api","status_api_bd","status_bd_sync",
    ] if c in df24.columns]

    registros = []
    for _, row in df24.iterrows():
        sevs = [row[c] for c in colunas_status]
        pior = pior_severidade(sevs)
        if pior not in ("normal",):
            qtd = sum(1 for s in sevs if s != "normal")
            registros.append({
                "timestamp":              str(row["timestamp"]),
                "quantidade_incidentes":  qtd,
                "severidade_predominante": pior,
            })

    # Agrupa por faixa de 4h
    faixas: dict[str, dict] = {}
    for r in registros:
        hora  = pd.to_datetime(r["timestamp"], utc=True).hour
        faixa = faixa_4h(hora)
        if faixa not in faixas:
            faixas[faixa] = {"timestamp": faixa, "quantidade_incidentes": 0,
                             "severidade_predominante": "normal"}
        faixas[faixa]["quantidade_incidentes"] += r["quantidade_incidentes"]
        faixas[faixa]["severidade_predominante"] = pior_severidade([
            faixas[faixa]["severidade_predominante"],
            r["severidade_predominante"],
        ])

    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "periodo":   "24h",
        "servidor":  servidor,
        "dados":     list(faixas.values()),
    }

"""
temperatura.py — Geração dos JSONs de temperatura.

JSONs gerados:
  analytics/temperatura_dashboard.json
  analytics/temperatura_estresse_termico.json
  analytics/temperatura_alertas.json
"""
# ── Geração de alertas de temperatura por linha ───────────────────────────────

def _gerar_alertas_linha(row) -> str:
    """Reutiliza a lógica já existente de gerar_alerta (trusted.py)."""
    alertas = []
    if str(row.get("status_temperatura", "")).lower() in ("critical", "medium", "alert"):
        alertas.append("Temperatura CPU elevada")
    if str(row.get("status_margem", "")).lower() in ("critica", "atencao", "throttling"):
        alertas.append("Margem térmica crítica")
    if str(row.get("status_resfriamento", "")).lower() in ("critica", "atencao"):
        alertas.append("Resfriamento ineficiente")
    if str(row.get("throttling", "")).lower() == "sim":
        alertas.append("CPU em throttling")
    return " | ".join(alertas)


# ── Dia com mais alertas ──────────────────────────────────────────────────────

def _dia_mais_critico(df: pd.DataFrame) -> str:
    alertas = df.apply(_gerar_alertas_linha, axis=1)
    df_alr  = df[alertas != ""].copy()
    if df_alr.empty:
        return ""
    dias    = pd.to_datetime(df_alr["timestamp"], utc=True).dt.date.astype(str)
    return Counter(dias).most_common(1)[0][0]
 


# ── JSON temperatura_dashboard ────────────────────────────────────────────────


def gerar_temperatura_dashboard(df: pd.DataFrame, servidor: str) -> dict:
    temp_max = round(df["temp_max_cpu"].max(), 2)       if "temp_max_cpu"           in df.columns else 0
    temp_med = round(df["temp_max_cpu"].mean(), 2)      if "temp_max_cpu"           in df.columns else 0
    margem   = round(df["margem_termica"].mean(), 2)    if "margem_termica"         in df.columns else 0
    efic     = round(df["indice_resfriamento"].mean(), 2) if "indice_resfriamento"  in df.columns else 0
    throttl  = (df["throttling"].astype(str).str.lower() == "sim").any() if "throttling" in df.columns else False
 
    # Histórico agrupado por hora
    df_agr = df.copy()
    df_agr["grupo"] = df_agr["timestamp"].dt.strftime("%H:%M")
    agg_cols = {c: "mean" for c in ["temp_max_cpu","temperatura_ambiente"] if c in df.columns}
    agrupado = df_agr.groupby("grupo").agg(agg_cols).reset_index()
 
    labels     = agrupado["grupo"].tolist()
    core_quent = agrupado["temp_max_cpu"].round(2).tolist()          if "temp_max_cpu"       in agrupado.columns else []
    temp_amb   = agrupado["temperatura_ambiente"].round(2).tolist()  if "temperatura_ambiente" in agrupado.columns else []
 
    # Heatmap: cada registro com temp_max_cpu como "CPU X"
    heatmap = []
    for i, row in df.iterrows():
        heatmap.append({
            "core":        f"CPU {i % 8}",   # simula até 8 núcleos
            "temperatura": round(float(row.get("temp_max_cpu", 0)), 2),
        })
 
    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "servidor":  servidor,
        "kpis": {
            "temperatura_maxima":      temp_max,
            "temperatura_media":       temp_med,
            "margem_termica":          margem,
            "eficiencia_resfriamento": efic,
            "throttling_detectado":    bool(throttl),
        },
        "historico_temperatura": {
            "labels":            labels,
            "core_mais_quente":  core_quent,
            "temperatura_ambiente": temp_amb,
        },
        "heatmap_cores": heatmap,
    }


def gerar_temperatura_estresse_termico(df: pd.DataFrame, servidor: str) -> dict:
    dia = _dia_mais_critico(df)
 
    if dia and "temp_max_cpu" in df.columns:
        df_dia    = df[pd.to_datetime(df["timestamp"], utc=True).dt.date.astype(str) == dia]
        temp_max  = round(df_dia["temp_max_cpu"].max(), 2)
    else:
        temp_max  = round(df["temp_max_cpu"].max(), 2) if "temp_max_cpu" in df.columns else 0
 
    lat_media = round(df["latency_avg_ms"].mean(), 2) if "latency_avg_ms" in df.columns else 0
 
    # Impacto térmico: proporção de registros com temperatura crítica (>80°C)
    if "temp_max_cpu" in df.columns:
        criticos     = (df["temp_max_cpu"] > 80).sum()
        impacto_pct  = round(criticos / len(df) * 100, 2) if len(df) else 0
    else:
        impacto_pct  = 0
 
    return {
        "gerado_em":        datetime.now(timezone.utc).isoformat(),
        "servidor":         servidor,
        "dia_mais_critico": dia,
        "temperatura_maxima": temp_max,
        "media_latencia":   lat_media,
        "impacto_termico":  impacto_pct,
    }
 
 
# ── JSON temperatura_alertas ──────────────────────────────────────────────────
 
def gerar_temperatura_alertas(df: pd.DataFrame, servidor: str) -> dict:
    alertas_por_linha = df.apply(_gerar_alertas_linha, axis=1)
    total   = alertas_por_linha[alertas_por_linha != ""].count()
 
    throttl = (df["throttling"].astype(str).str.lower() == "sim").sum() if "throttling" in df.columns else 0
 
    # Considera "ativo" os registros com temperatura acima de 80°C
    ativos  = (df["temp_max_cpu"] > 80).sum() if "temp_max_cpu" in df.columns else 0
 
    return {
        "gerado_em":            datetime.now(timezone.utc).isoformat(),
        "servidor":             servidor,
        "total_alertas_termicos": int(total),
        "alertas_ativos":       int(ativos),
        "throttling_eventos":   int(throttl),
    }

"""
incidents.py — Geração dos JSONs de incidentes.

JSONs gerados:
  incidents/incidentes_ativos.json
  incidents/incidentes_historico_24h.json
  incidents/incidentes_confiabilidade.json

Consumidos por: Java, Jira, Slack e front-end.
"""

FAIXAS_4H_LABELS = ["00h-04h","04h-08h","08h-12h","12h-16h","16h-20h","20h-24h"]

_MAPA_CRIT = {"normal": "normal", "baixo": "baixo", "medio": "medio",
              "alto": "alto", "critico": "critico"}


# ── Geração de incidentes brutos a partir do trusted ─────────────────────────

def _incidente(componente: str, titulo: str, criticidade: str, descricao: str,
               valor: float, limite: float, servidor: str, ts: str,
               status: str = "ABERTO") -> dict:
    return {
        "incidente_id":  str(uuid.uuid4()),
        "titulo":        titulo,
        "criticidade":   criticidade,
        "descricao":     descricao,
        "componente":    componente,
        "valor":         round(float(valor), 2),
        "limite":        round(float(limite), 2),
        "timestamp":     ts,
        "status_alerta": status,
        "servidor":      servidor,
    }


def _extrair_incidentes(df: pd.DataFrame, servidor: str) -> list[dict]:
    """
    Percorre o DataFrame trusted linha a linha e cria registros de incidente
    para cada coluna que ultrapassa seu limite.  Não duplica lógica de
    classificação — delega para os classificadores de utils.py.
    """
    regras = [
        # (coluna_valor, limite, componente, titulo_template, classificador)
        ("cpu",   LIMITE_CPU_CRITICO,   "CPU",   "CPU elevada",   classificar_cpu),
        ("ram",   LIMITE_RAM_CRITICO,   "RAM",   "RAM elevada",   classificar_ram),
        ("disco", LIMITE_DISCO_CRITICO, "DISCO", "Disco elevado", classificar_disco),
        ("latency_avg_ms", 200, "REDE", "Latência elevada", classificar_latencia),
        ("packet_loss_internet", 15, "REDE", "Perda de pacotes", classificar_pacotes),
        ("temp_max_cpu", 80, "TEMPERATURA", "Temperatura crítica",
         lambda v: "critico" if v > 90 else ("alto" if v > 80 else "medio")),
    ]

    incidentes = []
    for _, row in df.iterrows():
        ts = str(row.get("timestamp", ""))
        for col, limite, comp, titulo, classif in regras:
            if col not in df.columns:
                continue
            valor = float(row.get(col, 0))
            crit  = classif(valor)
            if crit in ("critico", "alto", "medio"):
                incidentes.append(_incidente(
                    componente  = comp,
                    titulo      = titulo,
                    criticidade = crit,
                    descricao   = f"{col} = {valor} (limite: {limite})",
                    valor       = valor,
                    limite      = limite,
                    servidor    = servidor,
                    ts          = ts,
                ))
    return incidentes


# ── JSON 1: incidentes_ativos ─────────────────────────────────────────────────

def gerar_incidentes_ativos(df: pd.DataFrame, servidor: str) -> dict:
    """Incidentes atualmente ABERTOS — consumido por Java/Jira/Slack."""
    incidentes = _extrair_incidentes(df, servidor)
    ativos = [i for i in incidentes if i["status_alerta"] == "ABERTO"]
    return {
        "gerado_em":        datetime.now(timezone.utc).isoformat(),
        "servidor":         servidor,
        "total_incidentes": len(ativos),
        "dados":            ativos,
    }


# ── JSON 2: incidentes_historico_24h ─────────────────────────────────────────

def gerar_incidentes_historico_24h(df: pd.DataFrame, servidor: str) -> dict:
    """Agrupamento de incidentes por janela de 4h nas últimas 24h."""
    agora  = df["timestamp"].max()
    inicio = agora - pd.Timedelta(hours=24)
    df24   = df[df["timestamp"] >= inicio].copy()

    incidentes = _extrair_incidentes(df24, servidor)

    contagem: dict[str, dict] = {
        f: {"faixa": f, "critico": 0, "alto": 0, "medio": 0, "baixo": 0}
        for f in FAIXAS_4H_LABELS
    }

    for inc in incidentes:
        try:
            hora  = pd.to_datetime(inc["timestamp"], utc=True).hour
            faixa = faixa_4h(hora)
            crit  = inc["criticidade"]
            if crit in contagem[faixa]:
                contagem[faixa][crit] += 1
        except Exception:
            pass

    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "servidor":  servidor,
        "periodo":   "24h",
        "intervalos": list(contagem.values()),
    }


# ── Cálculo de persistência de alertas ───────────────────────────────────────

def _calcular_penalidade(persistencia: float, peso_maximo: float) -> float:
    if persistencia < 0.20:   pct = 0
    elif persistencia < 0.40: pct = 0.25
    elif persistencia < 0.60: pct = 0.50
    elif persistencia < 0.80: pct = 0.75
    else:                     pct = 1.0
    return peso_maximo * pct


def _calcular_persistencia(df: pd.DataFrame, limites: dict) -> dict:
    """
    Retorna persistência de alertas para o período completo do DataFrame.
    limites: {"cpu": float, "ram": float, "disco": float}
    """
    total = len(df)
    if total == 0:
        return {"cpu": 0.0, "ram": 0.0, "disco": 0.0, "adsb": 0.0, "total_coletas": 0}

    lim_cpu   = limites.get("CPU",   LIMITE_CPU_CRITICO)
    lim_ram   = limites.get("RAM",   LIMITE_RAM_CRITICO)
    lim_disco = limites.get("DISCO", LIMITE_DISCO_CRITICO)
    lim_adsb  = 10.0

    return {
        "total_coletas": total,
        "cpu":   round((df["cpu"]   > lim_cpu).sum()   / total, 4),
        "ram":   round((df["ram"]   > lim_ram).sum()   / total, 4),
        "disco": round((df["disco"] > lim_disco).sum() / total, 4),
        "adsb":  round((df["avg_adsb_update_seconds"] > lim_adsb).sum() / total, 4),
    }


# ── JSON 3: incidentes_confiabilidade ────────────────────────────────────────

def gerar_incidentes_confiabilidade(df: pd.DataFrame, servidor: str,
                                     limites: dict | None = None) -> dict:
    """Índice operacional, MTTR e disponibilidade do servidor."""
    if limites is None:
        limites = {}

    pers = _calcular_persistencia(df, limites)

    pen_cpu   = _calcular_penalidade(pers["cpu"],   30)
    pen_ram   = _calcular_penalidade(pers["ram"],   30)
    pen_disco = _calcular_penalidade(pers["disco"], 15)
    pen_adsb  = _calcular_penalidade(pers["adsb"],  25)

    indice = round(max(0, min(100, 100 - pen_cpu - pen_ram - pen_disco - pen_adsb)), 2)

    # Disponibilidade baseada em health_score
    total_registros = len(df)
    online_count    = (df.get("health_score", pd.Series(dtype=float)) >= 70).sum() \
                      if "health_score" in df.columns else total_registros

    disponibilidade = round(online_count / total_registros * 100, 2) if total_registros else 0

    # MTTR estimado (minutos entre registros com status degradado)
    incidentes_total  = _extrair_incidentes(df, servidor)
    alertas_criticos  = sum(1 for i in incidentes_total if i["criticidade"] == "critico")
    mttr              = round(60 * (1 - indice / 100), 1)

    return {
        "gerado_em":          datetime.now(timezone.utc).isoformat(),
        "servidor":           servidor,
        "indice_confiabilidade": indice,
        "mttr_minutos":       mttr,
        "total_alertas":      len(incidentes_total),
        "alertas_criticos":   alertas_criticos,
        "disponibilidade":    disponibilidade,
    }

"""
client.py — Geração do JSON de alertas padronizados universal.

JSON gerado:
  client/alertas_padronizados.json

Consumido por: Java, Jira, Slack e front-end.
Criticidade: normal | baixo | medio | alto | critico
Status:       ABERTO | RESOLVIDO
Origens:      CPU | RAM | DISCO | PROCESSO | REDE | TEMPERATURA
"""


# ── Regras de alerta — cada regra gera um registro de alerta se o valor
#    ultrapassar o limite e a criticidade for >= medio. ──────────────────────

_REGRAS = [
    # (coluna, limite, origem, titulo_template, classificador)
    ("cpu",   LIMITE_CPU_CRITICO,   "CPU",
     "CPU elevada no servidor", classificar_cpu),

    ("ram",   LIMITE_RAM_CRITICO,   "RAM",
     "Consumo de RAM elevado", classificar_ram),

    ("disco", LIMITE_DISCO_CRITICO, "DISCO",
     "Uso de disco elevado", classificar_disco),

    ("latency_avg_ms", 200.0, "REDE",
     "Latência de rede elevada", classificar_latencia),

    ("packet_loss_internet", 15.0, "REDE",
     "Perda de pacotes detectada", classificar_pacotes),

    ("temp_max_cpu", 80.0, "TEMPERATURA",
     "Temperatura crítica da CPU",
     lambda v: "critico" if v > 90 else ("alto" if v > 80 else ("medio" if v > 70 else "normal"))),

    ("avg_adsb_update_seconds", 10.0, "REDE",
     "Atraso na atualização ADS-B",
     lambda v: "critico" if v > 30 else ("alto" if v > 20 else ("medio" if v > 10 else "normal"))),
]

_CRITICIDADES_RELEVANTES = {"medio", "alto", "critico"}


def _status_alerta(criticidade: str) -> str:
    """Mapeia criticidade para status de alerta inicial."""
    return "ABERTO" if criticidade in _CRITICIDADES_RELEVANTES else "RESOLVIDO"


# ── Geração dos alertas padronizados ─────────────────────────────────────────

def gerar_alertas_padronizados(df: pd.DataFrame, servidor: str) -> dict:
    """
    Percorre o DataFrame trusted e gera um alerta padronizado para cada
    métrica que ultrapassa seu limiar.  Cada alerta é único (uuid) e compatível
    com Java, Jira, Slack e front-end.
    """
    alertas = []

    for _, row in df.iterrows():
        ts = str(row.get("timestamp", ""))

        for col, limite, origem, titulo, classif in _REGRAS:
            if col not in df.columns:
                continue

            valor     = float(row.get(col, 0))
            crit      = classif(valor)

            if crit not in _CRITICIDADES_RELEVANTES:
                continue

            alertas.append({
                "incidente_id":  str(uuid.uuid4()),
                "titulo":        titulo,
                "criticidade":   crit,
                "descricao":     f"{col} = {round(valor, 2)} (limite: {limite})",
                "componente":    origem,
                "servidor":      servidor,
                "valor":         round(valor, 2),
                "limite":        float(limite),
                "status_alerta": _status_alerta(crit),
                "timestamp":     ts,
                "origem":        origem,
            })

    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "servidor":  servidor,
        "dados":     alertas,
    }

"""
processos.py — Geração dos JSONs de processos.

JSONs gerados:
  analytics/processos_lista.json
  analytics/processos_kpis.json
  analytics/processos_criticos_24h.json

Nota: este módulo espera um DataFrame com as colunas do raw_processos.csv
(pid, nome, usuario, cpu, ram_percent, ram_mb, status, tempo_execucao, latencia_ms).
O raw.csv principal não contém essas colunas; elas vêm de um CSV separado de
processos coletado pelo agente de coleta.
"""

# ── Criticidade de processo ───────────────────────────────────────────────────

def criticidade_processo(cpu: float, ram_percent: float, latencia: float) -> str:
    if cpu >= PROC_CPU_CRITICA or ram_percent > PROC_RAM_CRITICA or latencia > PROC_LATENCIA_CRITICA:
        return "Crítico"
    if cpu > PROC_CPU_ALERTA or ram_percent > PROC_RAM_ALERTA or latencia > PROC_LATENCIA_ALERTA:
        return "Alerta"
    return "Estável"


# ── Limpeza/enriquecimento do CSV de processos ────────────────────────────────

def processar_processos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["cpu"]         = pd.to_numeric(df["cpu"], errors="coerce").fillna(0)
    df["ram_percent"] = pd.to_numeric(df["ram_percent"], errors="coerce").fillna(0)
    df["latencia_ms"] = pd.to_numeric(df["latencia_ms"], errors="coerce").fillna(0)
    df["timestamp"]   = pd.to_datetime(df["timestamp"], utc=True)
    df["criticidade"] = df.apply(
        lambda r: criticidade_processo(r["cpu"], r["ram_percent"], r["latencia_ms"]),
        axis=1,
    )
    return df


# ── JSON 1: processos_lista ───────────────────────────────────────────────────

def gerar_processos_lista(df: pd.DataFrame, servidor: str) -> dict:
    """Lista completa de processos com todos os campos relevantes."""
    colunas = ["timestamp","pid","nome","usuario","cpu","ram_percent",
               "ram_mb","status","tempo_execucao","latencia_ms","criticidade"]
    registros = df[colunas].to_dict(orient="records")
    return json_envelope("snapshot", servidor, registros)


# ── JSON 2: processos_kpis ────────────────────────────────────────────────────

def _top5(df: pd.DataFrame, coluna: str, alias: str) -> dict:
    top = df.nlargest(5, coluna)[["nome", coluna]].reset_index(drop=True)
    resultado = {}
    for i, row in top.iterrows():
        resultado[f"nome-{i+1}"]    = row["nome"]
        resultado[f"{alias}-{i+1}"] = float(row[coluna])
    return resultado


def gerar_processos_kpis(df: pd.DataFrame, servidor: str) -> dict:
    status_count = df["status"].str.lower().value_counts().to_dict()
    maior_lat    = df.loc[df["latencia_ms"].idxmax()]

    kpis = {
        "total_processos":    len(df),
        "processos_running":  status_count.get("running", 0),
        "processos_sleeping": status_count.get("sleeping", 0),
        "processos_stopped":  status_count.get("stopped", 0),
        "top5_cpu":           _top5(df, "cpu", "cpu"),
        "top5_ram":           _top5(df, "ram_percent", "ram"),
        "maior_latencia": {
            "pid":        str(maior_lat["pid"]),
            "nome":       maior_lat["nome"],
            "latencia_ms": float(maior_lat["latencia_ms"]),
        },
        "media_cpu": round(float(df["cpu"].mean()), 2),
        "media_ram": round(float(df["ram_percent"].mean()), 2),
    }
    return json_envelope("snapshot", servidor, kpis)


# ── JSON 3: processos_criticos_24h ───────────────────────────────────────────

def gerar_processos_criticos_24h(df: pd.DataFrame, servidor: str) -> dict:
    """Agrupa processos críticos em janelas de 4h nas últimas 24h."""
    agora   = df["timestamp"].max()
    inicio  = agora - pd.Timedelta(hours=24)
    df_24h  = df[df["timestamp"] >= inicio].copy()
    df_crit = df_24h[df_24h["criticidade"] == "Crítico"].copy()

    df_crit["faixa"] = df_crit["timestamp"].dt.hour.apply(faixa_4h)

    intervalos = []
    for label, _, _ in [
        ("00h-04h", 0,  4), ("04h-08h", 4,  8),
        ("08h-12h", 8, 12), ("12h-16h", 12, 16),
        ("16h-20h", 16, 20), ("20h-24h", 20, 24),
    ]:
        bloco = df_crit[df_crit["faixa"] == label]
        qty   = len(bloco)

        # Severidade predominante no intervalo
        if qty == 0:
            sev = "normal"
        elif (bloco["cpu"] >= PROC_CPU_CRITICA).any():
            sev = "critico"
        else:
            sev = "alto"

        intervalos.append({
            "faixa":               label,
            "quantidade_criticos": qty,
            "severidade":          sev,
            "timestamp":           str(agora),
        })

    return json_envelope("24h", servidor, intervalos)

"""
pipeline.py — Orquestrador principal da pipeline ETL modular.

Fluxo:
  1. Identificação do servidor (MAC → MySQL)
  2. Download do raw.csv do S3
  3. Trusted: limpeza + enriquecimento → upload CSV
  4. Analytics: CPU/RAM/Disco, Rede, Temperatura → upload JSONs
  5. Incidents: ativos, histórico, confiabilidade → upload JSONs
  6. Client: alertas padronizados → upload JSON
  7. Processos: lista, KPIs, críticos 24h → upload JSONs
     (requer raw_processos.csv separado no S3)

Todos os JSONs respeitam o envelope padrão definido em utils.json_envelope
e as estruturas obrigatórias dos requisitos do projeto.
"""

# ── Helpers de upload ─────────────────────────────────────────────────────────

def _upload_json(s3, bucket: str, obj: dict, key: str):
    salvar_s3(s3, serializar_json(obj), key)
    print(f"  ✓ {key}")


def _prefixo(empresa_id, mac) -> tuple[str, str, str, str]:
    """Retorna (raw_prefix, trusted_prefix, analytics_prefix, incidents_prefix, client_prefix)"""
    base = f"empresa_{empresa_id}/{mac}"
    return (
        f"raw/{base}",
        f"trusted/{base}",
        f"analytics/{base}",
        f"incidents/{base}",
        f"client/{base}",
    )


# ── Pipeline principal ────────────────────────────────────────────────────────

def main():
    bucket = AWS_CONFIG["bucket_name"]
    s3     = conectar_s3()

    # ── 1. Identificação do servidor ─────────────────────────────────────────
    mac = coletar_mac()
    mac_win = mac.replace(":", "-")  # compatibilidade Windows

    servidor_info = obter_servidor(mac)
    if not servidor_info:
        print("[ERRO] Servidor não encontrado no banco de dados.")
        return

    empresa_id  = servidor_info["fk_empresa"]
    servidor_id = servidor_info["id_servidor"]
    hostname    = servidor_info["hostname"]

    print(f"[INFO] Servidor: {hostname} | MAC: {mac} | Empresa: {empresa_id}")

    raw_pfx, trusted_pfx, analytics_pfx, incidents_pfx, client_pfx = _prefixo(empresa_id, mac)

    # ── 2. Download raw.csv ───────────────────────────────────────────────────
    raw_key = f"{raw_pfx}/raw.csv"
    if not arquivo_existe_s3(s3, raw_key):
        print(f"[ERRO] {raw_key} não encontrado no S3.")
        return

    df_raw = ler_csv_s3(s3, raw_key)
    if df_raw.empty:
        print("[ERRO] raw.csv vazio.")
        return

    # ── 3. Trusted ────────────────────────────────────────────────────────────
    print("\n[Trusted]")
    df = processar_trusted(df_raw)
    salvar_s3(s3, exportar_trusted_csv(df), f"{trusted_pfx}/trusted_metrics.csv")
    print(f"  ✓ {trusted_pfx}/trusted_metrics.csv")

    # Limites do banco de dados para cálculos de incidentes
    limites_bd = obter_limites(servidor_id)

    # ── 4. Analytics: CPU/RAM/Disco ───────────────────────────────────────────
    print("\n[Analytics — Recursos]")
    for periodo in ("24h", "3d", "7d"):
        obj = gerar_recursos_dashboard(df, hostname, periodo)
        _upload_json(s3, bucket, obj, f"{analytics_pfx}/recursos_dashboard_{periodo}.json")

    _upload_json(s3, bucket,
                 gerar_disponibilidade_servidor(df, hostname),
                 f"{analytics_pfx}/disponibilidade_servidor.json")

    _upload_json(s3, bucket,
                 gerar_painel_saude(df),
                 f"{analytics_pfx}/painel_saude.json")

    # ── 5. Analytics: Rede ───────────────────────────────────────────────────
    print("\n[Analytics — Rede]")
    print(df["timestamp"].dtype)
    print(df["timestamp"].head())
    for periodo in ("24h", "3d", "7d"):
        obj = gerar_rede_dashboard(df, hostname, periodo)
        _upload_json(s3, bucket, obj, f"{analytics_pfx}/rede_dashboard_{periodo}.json")

    _upload_json(s3, bucket,
                 gerar_rede_horarios_criticos(df, hostname),
                 f"{analytics_pfx}/rede_horarios_criticos.json")

    # ── 6. Analytics: Temperatura ────────────────────────────────────────────
    print("\n[Analytics — Temperatura]")
    _upload_json(s3, bucket,
                 gerar_temperatura_dashboard(df, hostname),
                 f"{analytics_pfx}/temperatura_dashboard.json")
    _upload_json(s3, bucket,
                 gerar_temperatura_estresse_termico(df, hostname),
                 f"{analytics_pfx}/temperatura_estresse_termico.json")
    _upload_json(s3, bucket,
                 gerar_temperatura_alertas(df, hostname),
                 f"{analytics_pfx}/temperatura_alertas.json")

    # ── 7. Incidents ─────────────────────────────────────────────────────────
    print("\n[Incidents]")
    _upload_json(s3, bucket,
                 gerar_incidentes_ativos(df, hostname),
                 f"{incidents_pfx}/incidentes_ativos.json")
    _upload_json(s3, bucket,
                 gerar_incidentes_historico_24h(df, hostname),
                 f"{incidents_pfx}/incidentes_historico_24h.json")
    _upload_json(s3, bucket,
                 gerar_incidentes_confiabilidade(df, hostname, limites_bd),
                 f"{incidents_pfx}/incidentes_confiabilidade.json")

    # ── 8. Client: alertas padronizados ──────────────────────────────────────
    print("\n[Client]")
    _upload_json(s3, bucket,
                 gerar_alertas_padronizados(df, hostname),
                 f"{client_pfx}/alertas_padronizados.json")

    # ── 9. Atualização de status no banco ─────────────────────────────────────
    severidades = df[["status_cpu","status_ram","status_disco"]].values.flatten().tolist() \
                  if all(c in df.columns for c in ["status_cpu","status_ram","status_disco"]) else []
    novo_status = determinar_status_servidor(severidades)
    atualizar_status_servidor(servidor_id, novo_status)

    # ── 10. Processos (CSV separado, opcional) ────────────────────────────────
    proc_key = f"{raw_pfx}/raw_processos.csv"
    if arquivo_existe_s3(s3, proc_key):
        print("\n[Analytics — Processos]")
        df_proc_raw = ler_csv_s3(s3, proc_key)
        df_proc     = processar_processos(df_proc_raw)

        salvar_s3(s3,
                  df_proc.to_csv(index=False),
                  f"{trusted_pfx}/processos_trusted.csv")

        _upload_json(s3, bucket,
                     gerar_processos_lista(df_proc, hostname),
                     f"{analytics_pfx}/processos_lista.json")
        _upload_json(s3, bucket,
                     gerar_processos_kpis(df_proc, hostname),
                     f"{analytics_pfx}/processos_kpis.json")
        _upload_json(s3, bucket,
                     gerar_processos_criticos_24h(df_proc, hostname),
                     f"{analytics_pfx}/processos_criticos_24h.json")
    else:
        print(f"\n[INFO] {proc_key} não encontrado — JSONs de processos ignorados.")

    print("\n✅ Pipeline ETL finalizado com sucesso.")


if __name__ == "__main__":
    main()