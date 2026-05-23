import boto3
import json
import mysql.connector
from datetime import datetime, timedelta
import numpy as np

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
        if obj["Key"].endswith("metricas.json")
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
        return "offline"
    
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

# alteração para agrupar por servidores na hora de realizar o calculo
def calcular_disponibilidade(leituras, limites):
    por_servidor = {}

    for r in leituras:
        servidor = r["servidor_id"]
        if servidor not in por_servidor:
            por_servidor[servidor] = {"total": 0, "online": 0}

        metricas = r["metricas"]
        cpu = classificar(metricas["cpu"], limites[servidor]["CPU"])
        ram = classificar(metricas["ram"], limites[servidor]["RAM"])
        disco = classificar(metricas["disco"], limites[servidor]["DISCO"])

        por_servidor[servidor]["total"] += 1
        if cpu not in ["crítico", "offline"] and ram not in ["crítico", "offline"] and disco not in ["crítico", "offline"]:
            por_servidor[servidor]["online"] += 1

    disponibilidade = [
        servidor["online"] / servidor["total"] * 100
        for servidor in por_servidor.values() if servidor["total"] > 0
    ]

    return sum(disponibilidade) / len(disponibilidade)

def calcular_nivel_risco(leituras, limites):
    total = 0
    quantidade = 0

    for r in leituras:
        servidor = r["servidor_id"]
        metricas = r["metricas"]

        cpu = classificar(metricas["cpu"], limites[servidor]["CPU"])
        ram = classificar(metricas["ram"], limites[servidor]["RAM"])
        disco = classificar(metricas["disco"], limites[servidor]["DISCO"])

        total += SEVERIDADE[cpu]
        total += SEVERIDADE[ram]
        total += SEVERIDADE[disco]

        quantidade += 3
    # correção no valor da divisão pro calculo
    if quantidade > 0:
        media = total / quantidade
        return (media / 5) * 100
    
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

# agrupamento por servidor na hora de realizar o calculo
def calcular_estabilidade_operacional(leituras, limites):
    por_servidor = {}

    for r in leituras:
        servidor = r["servidor_id"]
        
        if servidor not in por_servidor:
            por_servidor[servidor] = {"total": 0, "estaveis": 0}

        metricas = r["metricas"]
        cpu = metricas["cpu"] / limites[servidor]["CPU"]
        ram = metricas["ram"] / limites[servidor]["RAM"]
        disco = metricas["disco"] / limites[servidor]["DISCO"]

        por_servidor[servidor]["total"] += 1
        if cpu < 0.80 and ram < 0.80 and disco < 0.80:
            por_servidor[servidor]["estaveis"] += 1

    estabilidade = [
        servidor["estaveis"] / servidor["total"] * 100
        for servidor in por_servidor.values()
    ]

    return sum(estabilidade) / len(estabilidade)

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
    grupos = {}

    for r in leituras:
        hora = r["data_hora"][:13]
        if hora not in grupos:
            grupos[hora] = []
        grupos[hora].append(r)

    labels = []
    valores = []

    for hora in sorted(grupos.keys()):
        estabilidade = calcular_estabilidade_operacional(grupos[hora], limites)
        labels.append(hora[11:] + ":00")
        valores.append(estabilidade)

    return {"labels": labels[-7:], "valores": valores[-7:]}

# adiocionado peso para os componentes pro calculo de impacto
PESOS_COMPONENTES = {
    "CPU": 0.8,
    "RAM": 1.0,
    "DISCO": 1.3
}

# alteração no calculo de impacto para utilizar uma faixa de severidade 
def calcular_impacto_componente(leituras, limites):
    por_servidor = {}

    for r in leituras:
        servidor = r["servidor_id"]

        if servidor not in por_servidor:
            por_servidor[servidor] = {
                "cpu": [],
                "ram": [],
                "disco": []
            }

        metricas = r["metricas"]
        
        impacto_cpu = min((metricas["cpu"] / limites[servidor]["CPU"]) * 100 * PESOS_COMPONENTES["CPU"], 100)
        impacto_ram = min((metricas["ram"] / limites[servidor]["RAM"]) * 100 * PESOS_COMPONENTES["RAM"], 100)
        impacto_disco = min((metricas["disco"] / limites[servidor]["DISCO"]) * 100 * PESOS_COMPONENTES["DISCO"], 100)

        por_servidor[servidor]["cpu"].append(impacto_cpu)
        por_servidor[servidor]["ram"].append(impacto_ram)
        por_servidor[servidor]["disco"].append(impacto_disco)

    medias = {
        "CPU": [],
        "RAM": [],
        "DISCO": []
    }

    for servidor, dados in por_servidor.items():

        medias["CPU"].append(sum(dados["cpu"]) / len(dados["cpu"]))
        medias["RAM"].append(sum(dados["ram"]) / len(dados["ram"]))
        medias["DISCO"].append(sum(dados["disco"]) / len(dados["disco"]))

    cpu_final = round(sum(medias["CPU"]) / len(medias["CPU"]), 1) if medias["CPU"] else 0
    ram_final = round(sum(medias["RAM"]) / len(medias["RAM"]), 1) if medias["RAM"] else 0
    disco_final = round(sum(medias["DISCO"]) / len(medias["DISCO"]), 1) if medias["DISCO"] else 0

# adiocionada função para calcular a faixa de severidade
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
        servidor = srv["id_servidor"]
        incidentes = 0

        for r in leituras:
            if r["servidor_id"] != servidor:
                continue

            metricas = r["metricas"]

            if (
                metricas["cpu"] >= limites[servidor]["CPU"]
                or
                metricas["ram"] >= limites[servidor]["RAM"]
                or
                metricas["disco"] >= limites[servidor]["DISCO"]
            ):
                incidentes += 1

        qtd_analistas = analistas.get(servidor, 0)

        status = srv["status_servidor"]

        resultado.append({
            "servidor": srv["hostname"],
            "incidentes": incidentes,
            "analistas": qtd_analistas,
            "status": status
        })

    return resultado

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

            x = np.arange(len(valores_recentes))
            a, b = np.polyfit(x, valores_recentes, 1)
            previsao = a * len(valores_recentes) + b

            limite = limites[servidor_id][metrica.upper()]

            atual = valores_recentes[-1] / limite
            nivel_previsao = previsao / limite

            print(
                    servidor_id,
                    metrica,
                    "inclinação:", round(a, 2),
                    "atual:", round(atual * 100, 1),
                    "previsto:", round((previsao / limite) * 100, 1)
                )
            
            if a > 0 and nivel_previsao > 0.60 and nivel_previsao > atual:
                nivel_previsao = classificar(previsao, limite)

                if nivel_previsao == "normal":
                    continue

                alertas_previsao.append({
                    "servidor_id": servidor_id,
                    "metrica": metrica.upper(),
                    "nivel_previsao": nivel_previsao,
                    "mensagem": gerar_mensagem(metrica.upper(), nivel_previsao, previsao, limite)
                })

    return alertas_previsao

def calcular_mttr(leituras, limites):
    tempos_recuperacao = []
    inicio_incidente = None

    for r in sorted(leituras, key=lambda r: r["data_hora"]):
        servidor = r["servidor_id"]
        metricas = r["metricas"]

        cpu = classificar(metricas["cpu"], limites[servidor]["CPU"])
        ram = classificar(metricas["ram"], limites[servidor]["RAM"])
        disco = classificar(metricas["disco"], limites[servidor]["DISCO"])

        chamado_aberto = cpu == "crítico" or ram == "crítico" or disco == "crítico"
        horario = datetime.strptime(r["data_hora"], "%Y-%m-%d %H:%M:%S")

        if chamado_aberto and inicio_incidente is None:
            inicio_incidente = horario

        elif not chamado_aberto and inicio_incidente is not None:
            tempo = (horario - inicio_incidente).total_seconds() / 60
            tempos_recuperacao.append(tempo)
            inicio_incidente = None

    if not tempos_recuperacao:
        return None

    return round(sum(tempos_recuperacao) / len(tempos_recuperacao), 1)

def processar():
    s3 = get_s3()
    empresas = obter_empresas()

    for empresa in empresas:
        empresa_id = empresa["id_empresa"]
        print(f"\nEmpresa {empresa_id}: {empresa['razao_social']}")

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

            resultado["periodos"][periodo] = {
                "kpis": {
                    "disponibilidade_global": calcular_disponibilidade(leituras, limites),
                    "nivel_risco": calcular_nivel_risco(leituras, limites),
                    "incidentes_criticos": calcular_incidentes_criticos(leituras, limites),
                    "estabilidade_operacional": calcular_estabilidade_operacional(leituras, limites),
                    "tendencia_operacional": calcular_tendencia(leituras, limites)
                },
                "gráficos": {
                    "grafico_estabilidade": grafico_estabilidade(leituras, limites),
                    "impacto_por_componente": calcular_impacto_componente(leituras, limites),
                },
                "predicoes": calcular_previsao_falhas(leituras, limites),
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