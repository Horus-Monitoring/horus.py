
import pandas as pd 
import boto3 
import os
from datetime import datetime
import json
from io import StringIO
from botocore.exceptions import NoCredentialsError

S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "seu-bucket-s3") # Substitua pelo nome do seu bucket
S3_RAW_FILE_KEY = "raw/all_metrics.csv" # Caminho para o CSV geral na camada RAW
S3_TRUSTED_PREFIX = "trusted/"
S3_CLIENT_PREFIX = "client/"

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

def download_s3_file_content(bucket_name, s3_key):
    """Baixa o conteúdo de um arquivo do S3 e retorna como string."""
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
    return obj["Body"].read().decode("utf-8")

def upload_to_s3(data, bucket_name, s3_key):
    """Faz o upload de dados para o S3."""
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=data)
    print(f"Dados enviados com sucesso para s3://{bucket_name}/{s3_key}")
    return True

def processar_dados():
    """Processa o arquivo CSV geral da camada Raw, trata e envia para Trusted e Client."""
    print("Iniciando processamento de dados da camada RAW...")
    raw_content = download_s3_file_content(S3_BUCKET_NAME, S3_RAW_FILE_KEY)

    if not raw_content:
        raise ValueError("Nenhum conteúdo encontrado no arquivo RAW para processar.")

    df_raw = pd.read_csv(StringIO(raw_content))

    if df_raw.empty:
        raise ValueError("O arquivo RAW está vazio ou contém apenas o cabeçalho. Nada para processar.")

    df_trusted = df_raw.copy()
    df_trusted["data_hora"] = pd.to_datetime(df_trusted["data_hora"])
    df_trusted["valor"] = pd.to_numeric(df_trusted["valor"], errors="coerce")
    df_trusted["limite"] = pd.to_numeric(df_trusted["limite"], errors="coerce")
    df_trusted.dropna(subset=["valor"], inplace=True) 

    trusted_file_name = f"{S3_TRUSTED_PREFIX}all_metrics_trusted_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv"
    trusted_csv_buffer = StringIO()
    df_trusted.to_csv(trusted_csv_buffer, index=False)
    upload_to_s3(trusted_csv_buffer.getvalue(), S3_BUCKET_NAME, trusted_file_name)

    for servidor_id in df_trusted["servidor_id"].unique():
        df_server = df_trusted[df_trusted["servidor_id"] == servidor_id].copy()
        df_server = df_server.sort_values(by="data_hora", ascending=False)

        metrics_data = df_server[[
            "data_hora", "empresa_id", "empresa_nome", "servidor_id", "servidor_nome",
            "componente_id", "componente_v", "tipo", "valor", "limite"
        ]].to_dict(orient="records")
        metrics_json_key = f"{S3_CLIENT_PREFIX}servidor_{servidor_id}/metrics_{datetime.now().strftime("%Y%m%d%H%M%S")}.json"
        upload_to_s3(json.dumps(metrics_data, indent=2, default=str), S3_BUCKET_NAME, metrics_json_key)

        latest_process_entry = df_server.iloc[0]["top_5_cpu_processes_json"]
        top_processes_data = json.loads(latest_process_entry) if pd.notna(latest_process_entry) and latest_process_entry else []
        top_processes_json_key = f"{S3_CLIENT_PREFIX}servidor_{servidor_id}/top_processes_{datetime.now().strftime("%Y%m%d%H%M%S")}.json"
        upload_to_s3(json.dumps(top_processes_data, indent=2, default=str), S3_BUCKET_NAME, top_processes_json_key)

        alerts_data = df_server[df_server["valor"] > df_server["limite"]][[
            "data_hora", "empresa_id", "empresa_nome", "servidor_id", "servidor_nome",
            "componente_id", "componente_v", "tipo", "valor", "limite"
        ]].to_dict(orient="records")
        alerts_json_key = f"{S3_CLIENT_PREFIX}servidor_{servidor_id}/alerts_{datetime.now().strftime("%Y%m%d%H%M%S")}.json"
        upload_to_s3(json.dumps(alerts_data, indent=2, default=str), S3_BUCKET_NAME, alerts_json_key)

    print("Processamento de dados concluído.")

if __name__ == "__main__":
    print("Iniciando o script de processamento de dados para as camadas Trusted e Client do S3...")
    print(f"Certifique-se de que as variáveis de ambiente AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY e S3_BUCKET_NAME estão configuradas.")
    processar_dados()
