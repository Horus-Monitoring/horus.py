import boto3
import csv
import json
import os
import mysql.connector
from datetime import datetime, timedelta
from io import StringIO
from collections import defaultdict

AWS_CONFIG = {
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "aws_session_token": "",
    "region_name": "us-east-1",
    "bucket_name": "bucket-teste-sprint-3-2026"
}

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "horus_db"
}

def get_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_CONFIG["aws_access_key_id"],
        aws_secret_access_key=AWS_CONFIG["aws_secret_access_key"],
        aws_session_token=AWS_CONFIG["aws_session_token"],
        region_name=AWS_CONFIG["region_name"],
    )

def get_db():
    return mysql.connector.connect(**DB_CONFIG)

def listar_arquivos_client(s3, empresa_id):
    prefix = f"client/empresa_{empresa_id}/"
    res = s3.list_objects_v2(Bucket=AWS_CONFIG["bucket_name"], Prefix=prefix)
    return [
        obj["Key"]
        for obj in res.get("Contents", [])
        if obj["Key"].endswith(".json")
        and "/alertas/" not in obj["Key"]
        and "/resumo/" not in obj["Key"]
    ]

def ler_json_s3(s3, key):
    obj = s3.get_object(Bucket=AWS_CONFIG["bucket_name"], Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))

def salvar_json_s3(s3, key, dados):
    s3.put_object(
        Bucket=AWS_CONFIG["bucket_name"],
        Key=key,
        Body=json.dumps(dados, indent=2, ensure_ascii=False),
        ContentType="application/json",
    )

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

def obter_limites_servidor(servidor_id):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT c.tipo, sc.limite
        FROM servidor_componente sc
        JOIN componente c ON sc.fk_componente = c.id_componente
        WHERE sc.fk_servidor = %s
    """, (servidor_id,))

    rows = cursor.fetchall()
    cursor.close(); conn.close()
    return {r["tipo"]: float(r["limite"]) for r in rows}

PERIODOS = {
    "24h": timedelta(hours=24),
    "7d":  timedelta(days=7),
    "30d": timedelta(days=30),
}

def filtrar_periodo(leituras, periodo):
    delta = PERIODOS[periodo]
    agora = datetime.now()
    corte = agora - delta
    return [
        r for r in leituras
        if datetime.strptime(r["data_hora"], "%Y-%m-%d %H:%M:%S") >= corte
    ]

SEVERIDADE = {
    "crítico": 5,
    "alta": 4, 
    "média": 3, 
    "baixa": 2, 
    "normal": 1
    }

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

    for r in leituras:
        hora = r["data_hora"][:13]

        if hora not in grupos:
            grupos[hora] = []

        grupos[hora].append(r)

    for hora in grupos:
        estabilidade = calcular_estabilidade_operacional(
            grupos[hora],
            limites
        )

        valores.append(estabilidade)

    return valores

def calcular_impacto_componente(leituras, limites):
    cpu = []
    ram = []
    disco = []

    for r in leituras:
        s = r["servidor_id"]
        m = r["metricas"]

        cpu.append(
            m["cpu"] / limites[s]["CPU"] * 100
        )

        ram.append(
            m["ram"] / limites[s]["RAM"] * 100
        )

        disco.append(
            m["disco"] / limites[s]["DISCO"] * 100
        )

    return {
        "CPU": sum(cpu) / len(cpu),
        "RAM": sum(ram) / len(ram),
        "DISCO": sum(disco) / len(disco)
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

# def calcular_previsao_falhas(leituras, limites):

def processar():
    s3 = get_s3()
    empresas = obter_empresas()

    for empresa in empresas:
        empresa_id = empresa["id_empresa"]
        print(f"\n── Empresa {empresa_id}: {empresa['razao_social']}")

        arquivos = listar_arquivos_client(s3, empresa_id)
        todas_leituras = []
        for key in arquivos:
            dados = ler_json_s3(s3, key)
            todas_leituras.extend(dados)

        if not todas_leituras:
            print("Sem dados disponíveis")
            continue

        servidores = obter_servidores_empresa(empresa_id)
        analistas = obter_analistas_por_servidor(empresa_id)

        limites = {
            srv["id_servidor"]: obter_limites_servidor(srv["id_servidor"])
            for srv in servidores
        }

        resultado = {"empresa_id": empresa_id, "gerado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "periodos": {}}

        for periodo in ["24h", "7d", "30d"]:
            leituras = filtrar_periodo(todas_leituras, periodo)
            print(f"[{periodo}] {len(leituras)} leituras")

            if not leituras:
                resultado["periodos"][periodo] = {"sem_dados": True}
                continue

            # mttr = calcular_mttr(leituras, limites)

            resultado["periodos"][periodo] = {
                "kpis": {
                    "disponibilidade_global": {
                        "valor": calcular_disponibilidade(leituras, limites),
                        "meta": 99.5
                    },
                    "nivel_risco": {
                        "valor": calcular_nivel_risco(leituras, limites)
                    },
                    "incidentes_criticos": {
                        "valor": calcular_incidentes_criticos(leituras, limites)
                    },
                    "estabilidade_operacional": {
                        "valor": calcular_estabilidade_operacional(leituras, limites)
                    },
                    "tendencia_operacional": calcular_tendencia(leituras, limites)
                },

                "grafico_estabilidade": grafico_estabilidade(leituras, limites),
                "impacto_por_componente": calcular_impacto_componente(leituras, limites),

                "info_servidores": listar_info_servidores(
                    leituras, limites, servidores, analistas
                )
            }

        key_destino = f"client/gestor/empresa_{empresa_id}/dashboard_gestor.json"
        salvar_json_s3(s3, key_destino, resultado)
        print(f"Salvo em s3://{AWS_CONFIG['bucket_name']}/{key_destino}")

    print("\nETL do gestor executada")

if __name__ == "__main__":
    processar()