import os
import json
import boto3
import tempfile
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
from getmac import get_mac_address
from collections import Counter
from botocore.exceptions import ClientError

load_dotenv()

AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "aws_session_token": os.getenv("AWS_SESSION_TOKEN"),
    "region_name": os.getenv("AWS_REGION_NAME"),
    "bucket_name": os.getenv("AWS_BUCKET_NAME")
}

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE")
}

def conectar_s3():

    return boto3.client(
        "s3",
        aws_access_key_id=AWS_CONFIG["aws_access_key_id"],
        aws_secret_access_key=AWS_CONFIG["aws_secret_access_key"],
        aws_session_token=AWS_CONFIG["aws_session_token"],
        region_name=AWS_CONFIG["region_name"]
    )

def arquivo_existe_s3(
    s3,
    bucket,
    key
):

    try:

        s3.head_object(
            Bucket=bucket,
            Key=key
        )

        return True

    except ClientError:

        return False

# DOWNLOAD S3

def baixar_s3(
    s3,
    bucket,
    key,
    destino
):

    try:

        s3.download_file(
            bucket,
            key,
            destino
        )

        print(
            f"Download concluído: {key}"
        )

    except Exception as e:

        print(
            f"Erro download S3: {e}"
        )

# UPLOAD S3

def upload_s3(
    s3,
    arquivo,
    bucket,
    key
):

    try:

        s3.upload_file(
            arquivo,
            bucket,
            key
        )

        print(
            f"Upload concluído: {key}"
        )

    except Exception as e:

        print(
            f"Erro upload S3: {e}"
        )

# MAC ADDRESS

def coletar_mac():

    mac = get_mac_address()

    if not mac:

        raise Exception(
            "MAC Address não encontrado."
        )

    return (
        mac
        .lower()
        .replace("-", ":")
    )

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

# ALERTAS

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

    if throttling == "sim":

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

    dias = pd.to_datetime(
        df_alertas["timestamp"]
    ).dt.day_name()

    contador = Counter(dias)

    return contador.most_common(1)[0][0]

# MAIN

def main():

    # MAC ADDRESS

    MAC_ADDRESS = coletar_mac()

    # WINDOWS SAFE

    MAC_ADDRESS_LOCAL = (
        MAC_ADDRESS
        .replace(":", "-")
    )

    # MYSQL

    servidor = obter_servidor(
        MAC_ADDRESS
    )

    if not servidor:

        print(
            "Servidor não encontrado."
        )

        return

    EMPRESA_ID = servidor["fk_empresa"]

    SERVIDOR_ID = servidor["id_servidor"]

    HOSTNAME = servidor["hostname"]

    print(
        f"MAC Address: {MAC_ADDRESS}"
    )

    print(
        f"Empresa ID: {EMPRESA_ID}"
    )

    print(
        f"Servidor ID: {SERVIDOR_ID}"
    )

    # DIRETÓRIO RAW

    RAW_DIR = os.path.join(
        "raw",
        f"empresa_{EMPRESA_ID}",
        MAC_ADDRESS_LOCAL
    )

    os.makedirs(
        RAW_DIR,
        exist_ok=True
    )

    # RAW LOCAL

    RAW_FILE = os.path.join(
        RAW_DIR,
        "raw.csv"
    )

    # TEMP TRUSTED

    trusted_temp = tempfile.NamedTemporaryFile(
        delete=False,
        suffix=".csv"
    )

    TRUSTED_FILE = trusted_temp.name

    trusted_temp.close()

    # TEMP CLIENT

    client_temp = tempfile.NamedTemporaryFile(
        delete=False,
        suffix=".json"
    )

    CLIENT_FILE = client_temp.name

    client_temp.close()

    # KEYS S3

    RAW_KEY = (
        f"raw/empresa_{EMPRESA_ID}/"
        f"{MAC_ADDRESS}/raw.csv"
    )

    TRUSTED_KEY = (
        f"trusted/empresa_{EMPRESA_ID}/"
        f"{MAC_ADDRESS}/trusted_metrics.csv"
    )

    CLIENT_KEY = (
        f"client/empresa_{EMPRESA_ID}/"
        f"{MAC_ADDRESS}/client_metrics.json"
    )

    # S3

    s3 = conectar_s3()

    bucket = AWS_CONFIG["bucket_name"]

    # VALIDAR RAW S3

    if not arquivo_existe_s3(
        s3,
        bucket,
        RAW_KEY
    ):

        print(
            "raw.csv não encontrado na S3."
        )

        return

    # DOWNLOAD RAW

    baixar_s3(
        s3,
        bucket,
        RAW_KEY,
        RAW_FILE
    )

    # VALIDAR RAW

    if not os.path.exists(
        RAW_FILE
    ):

        print(
            "raw.csv não encontrado localmente."
        )

        return

    # LEITURA CSV

    df = pd.read_csv(
        RAW_FILE
    )

    if df.empty:

        print(
            "CSV vazio."
        )

        return

    # ALERTAS

    df["alertas"] = df.apply(
        gerar_alerta,
        axis=1
    )

    # TOTAL ALERTAS

    df["quantidade_alertas"] = (
        df["alertas"]
        .apply(
            lambda x:
            len(x.split("|"))
            if x else 0
        )
    )

    # DIA COM MAIS ALERTAS

    dia_mais_alertas = (
        calcular_dia_mais_alertas(df)
    )

    # TRUSTED CSV

    df.to_csv(
        TRUSTED_FILE,
        index=False,
        encoding="utf-8"
    )

    print(
        "Trusted gerado."
    )

    # CLIENT JSON

    client_json = {

        "empresa_id":
            EMPRESA_ID,

        "servidor_id":
            SERVIDOR_ID,

        "hostname":
            HOSTNAME,

        "mac_address":
            MAC_ADDRESS,

        "dia_com_mais_alertas":
            dia_mais_alertas,

        "total_registros":
            int(len(df)),

        "total_alertas":
            int(
                df[
                    "quantidade_alertas"
                ].sum()
            ),

        "dados":
            df.to_dict(
                orient="records"
            )
    }

    with open(
        CLIENT_FILE,
        "w",
        encoding="utf-8"
    ) as json_file:

        json.dump(
            client_json,
            json_file,
            indent=4,
            ensure_ascii=False,
            default=str
        )

    print(
        "Client JSON gerado."
    )

    # UPLOAD TRUSTED

    upload_s3(
        s3,
        TRUSTED_FILE,
        bucket,
        TRUSTED_KEY
    )

    # UPLOAD CLIENT

    upload_s3(
        s3,
        CLIENT_FILE,
        bucket,
        CLIENT_KEY
    )

    # REMOVE TEMP

    try:

        os.remove(
            TRUSTED_FILE
        )

    except Exception as e:

        print(
            f"Erro removendo trusted temp: {e}"
        )

    try:

        os.remove(
            CLIENT_FILE
        )

    except Exception as e:

        print(
            f"Erro removendo client temp: {e}"
        )

    # FINAL

    print(
        "ETL finalizado com sucesso."
    )

# START

if __name__ == "__main__":

    main()