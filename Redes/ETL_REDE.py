import boto3
import json
import mysql.connector
from datetime import datetime
from io import StringIO
import pandas
import math

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


#Informações da S3
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_CONFIG["aws_access_key_id"],
    aws_secret_access_key=AWS_CONFIG["aws_secret_access_key"],
    aws_session_token=AWS_CONFIG["aws_session_token"],
    region_name=AWS_CONFIG["region_name"]
)

def verificar_csv():
    response = s3.list_objects_v2(
        Bucket=AWS_CONFIG["bucket_name"],
        Prefix="raw/"
    )
    return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]

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
       df[coluna] = pandas.to_numeric(df[coluna], errors="coerce")

    df["timestamp"] = pandas.to_datetime(df["timestamp"])
    df["label_24h"] = df["timestamp"].dt.strftime("%H:%M")
    df["label_3d"] = df["timestamp"].dt.strftime("%d/%m %Hh")
    df["label_7d"] = df["timestamp"].dt.strftime("%d/%m")
    df["opensky_timestamp"] = pandas.to_datetime(df["opensky_timestamp"]) 
    df = df.fillna(0)

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

#Classificação e tratamento de dados
def classificar_latencia(valor):
    valor = float(valor)

    if valor > 250:
        return "critico"
    elif valor > 200:
        return "alto"
    elif valor > 150:
        return "medio"
    elif valor > 100:
        return "baixo"
    else:
        return "normal"
    
def severidade_servidor_latencia(linha):
    status = [
        linha["status_latency_avg"],
        linha["status_adsb"],
        linha["status_api_bd"],
        linha["status_bd_sync"]
    ]

    prioridade = {
        "critico": 4,
        "alto": 3,
        "medio": 2,
        "baixo": 1,
        "normal": 0
    }

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

    prioridade = {
        "critico": 4,
        "alto": 3,
        "medio": 2,
        "baixo": 1,
        "normal": 0
    }

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

    return df

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
    df["taxa_total_mb"] = (
        df["bytes_recv"] + df["bytes_sent"]
    ) / (1024 * 1024) #Byte para MegaByte

    return df["taxa_total_mb"].mean()

def consumo_banda_servico(df):
    return {
        "Rastreamento": df["rastreamento_mbps"].mean(),
        "Rotas": df["rotas_mbps"].mean(),
        "Correlacao": df["correlacao_mbps"].mean(),
        "API Gateway": df["api_gateway_mbps"].mean(),
        "Banco de Dados": df["bd_mbps"].mean(),
        "Sync Service": df["sync_service_mbps"].mean()
    }

#Geração de JSON para o Client
def gerar_json_dashboard(df_network, df_flights):

    dashboard = {
        "kpis": {
            "perda_pacotes": round(kpi_perda_media(df_network) ,2),
            "latencia_media": round(kpi_latencia_media(df_network)),
            "adsb_update": kpi_adsb_update(df_network),
            "rotas_sem_atualizacao": rotas_sem_atualizacao(df_flights)
        },

        "grafico_transferencia": {
            "hora": df_network["label_24h"].astype(str).tolist(),
            "rastreamento": df_network["rastreamento_mbps"].tolist(),
            "rotas": df_network["rotas_mbps"].tolist(),
            "correlacao": df_network["correlacao_mbps"].tolist()
        },

        "grafico_latencia_componentes": {
            "ADS-B": df_network["lat_adsb_rastreamento"].mean(),
            "Correlação": df_network["lat_rastreamento_correlacao"].mean(),
            "Banco de Dados": df_network["lat_api_bd"].mean(),
            "Rotas": round(df_network["lat_rotas_api"].mean(), 2),
            "Banco de Dados": round(df_network["lat_api_bd"].mean(), 2),
            "Sync Service": round(df_network["lat_bd_sync"].mean(), 2)
        },

        "consumo_banda": consumo_banda_servico(df_network),

        "perda_pacotes_servico": perda_pacotes_servico(df_network)
    }

    return dashboard

def salvar_trusted(df, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    salvar_s3(
        csv_buffer.getvalue(),
        key
    )

def salvar_json_dashboard(dashboard, key):
    salvar_s3(
        json.dumps(dashboard, indent=4),
        key
    )

def main():

    network_key = "raw/empresa_1/c0:35:32:c7:0b:59/network_raw.csv"
    flights_key = "raw/empresa_1/c0:35:32:c7:0b:59/flights_raw.csv"

    print("Lendo arquivos raw...")
    df_network = ler_csv_s3(network_key)
    df_flights = ler_csv_s3(flights_key)

    print("Limpando dados...")
    df_network = limpar_dados(df_network)
    df_flights = limpar_voos(df_flights)

    print("Enriquecendo dados...")
    df_network = enriquecer_dados(df_network)

    print("Salvando trusted...")
    salvar_trusted(
        df_network,
        "trusted/empresa_1/c0:35:32:c7:0b:59/network_trusted.csv"
    )

    salvar_trusted(
        df_flights,
        "trusted/empresa_1/c0:35:32:c7:0b:59/flights_trusted.csv"
    )

    print("Gerando JSON dashboard...")
    dashboard = gerar_json_dashboard(
        df_network,
        df_flights
    )

    salvar_json_dashboard(
        dashboard,
        "client/empresa_1/c0:35:32:c7:0b:59/dashboard.json"
    )

    print("Pipeline executado com sucesso.")

if __name__ == "__main__":
    main()
