import mysql.connector
import psutil
import speedtest
import time
import csv
import os
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Nm.05/08/03",
    "database": "horus_db"
}

ID_EMPRESA = 2

def conectar():
    return mysql.connector.connect(**DB_CONFIG)

def obter_nome_csv(id_empresa, id_servidor):
    pasta = f"empresa_{id_empresa}"
    if not os.path.exists(pasta):
        os.makedirs(pasta)
    return f"{pasta}/servidor_{id_servidor}.csv"

def inicializar_csv(nome_arquivo):
    if not os.path.exists(nome_arquivo):
        with open(nome_arquivo, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow([
                "data_hora",
                "empresa",
                "servidor",
                "componente_id",
                "componente_v",
                "tipo",
                "valor",
                "limite"
            ])

def salvar_csv(nome_arquivo, dado):
    with open(nome_arquivo, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([
            datetime.now(),
            dado["empresa"],
            dado["servidor"],
            dado["componente_id"],
            dado["componente_v"],
            dado["tipo"],
            dado["valor"],
            dado["limite"]
        ])

def buscar_componentes():
    conn = conectar()
    cursor = conn.cursor(dictionary=True)

    query = """
    SELECT 
        s.idServidor,
        c.idComponentes,
        c.nome_componente,
        c.tipo_componente,
        cs.limite,
        cs.id_componente_v
    FROM Servidor s
    JOIN CompServidor cs ON cs.fk_servidor = s.idServidor
    JOIN Componentes c ON c.idComponentes = cs.fk_componente
    WHERE s.fk_empresa = %s
    AND cs.ativo = TRUE
    """

    cursor.execute(query, (ID_EMPRESA,))
    dados = cursor.fetchall()

    cursor.close()
    conn.close()

    return dados

def coletar_valor(tipo):
    tipo = tipo.upper()

    if tipo == "CPU":
        return psutil.cpu_percent(interval=1)
    elif tipo == "RAM":
        return psutil.virtual_memory().percent
    elif tipo == "DISCO":
        return psutil.disk_usage('/').percent
    elif tipo == "REDE":
        st = speedtest.Speedtest()
        return round(st.download() / 10**6, 2)

    return None

def gerar_alerta(dado):
    if dado["valor"] is None or dado["limite"] is None:
        return

    if float(dado["valor"]) > float(dado["limite"]):
        conn = conectar()
        cursor = conn.cursor()

        query = """
        INSERT INTO Registro_Alerta 
        (data_alerta, criticidade, fk_servidor_componentes, valor)
        VALUES (%s, %s, %s, %s)
        """

        cursor.execute(query, (
            datetime.now(),
            "ALTA",
            dado["componente_v"],
            dado["valor"]
        ))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"ALERTA {dado}")

def monitorar():
    while True:
        try:
            componentes = buscar_componentes()

            for item in componentes:
                valor = coletar_valor(item["tipo_componente"])

                dado = {
                    "empresa": ID_EMPRESA,
                    "servidor": item["idServidor"],
                    "componente_id": item["idComponentes"],
                    "componente_v": item["id_componente_v"],
                    "tipo": item["tipo_componente"],
                    "valor": valor,
                    "limite": item["limite"]
                }

                nome_csv = obter_nome_csv(ID_EMPRESA, item["idServidor"])

                inicializar_csv(nome_csv)

                salvar_csv(nome_csv, dado)
                gerar_alerta(dado)

            time.sleep(5)

        except Exception as e:
            print("Erro:", e)
            time.sleep(5)

if __name__ == "__main__":
    monitorar()