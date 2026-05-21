import random
import requests
import csv
import time
import os
import subprocess
from datetime import datetime

# CONFIGURAÇÕES
# http://localhost:8085/data.json rodar no gulu gulu

ARQUIVO_CSV = r"C:\Users\morai\Downloads\script-py\temperatura_servidor.csv"

URL_SENSOR = "http://localhost:8085/data.json"

API_KEY = "5d23ab006847e5fa09a310ef424cf172" #open wheather
CIDADE = "Sao Paulo"

URL_CLIMA = (
    f"https://api.openweathermap.org/data/2.5/weather"
    f"?q={CIDADE}"
    f"&appid={API_KEY}"
    f"&units=metric"
    f"&lang=pt_br"
)

INTERVALO = 5

# Ryzen normalmente 90
# Intel normalmente 95

TEMP_MAX_CPU = 90

# CLASSIFICAÇÃO TEMPERATURA

def classificar(temp_max):

    if temp_max < 65:
        return "normal"

    elif temp_max < 75:
        return "alert"

    elif temp_max < 85:
        return "medium"

    return "critical"

# MARGEM TÉRMICA

def calcular_margem_termica(temp_max):

    return round(
        TEMP_MAX_CPU - temp_max,
        1
    )

# STATUS MARGEM

def classificar_margem(margem):

    if margem > 30:
        return "excelente"

    elif margem > 20:
        return "boa"

    elif margem > 10:
        return "atencao"

    elif margem > 0:
        return "critica"

    return "throttling"

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

        response = requests.get(URL_CLIMA)

        data = response.json()

        return {

            "temperatura_ambiente":
                data["main"]["temp"],

            "descricao":
                data["weather"][0]["description"],

            "umidade":
                data["main"]["humidity"]

        }

    except Exception as erro:

        print("Erro clima:", erro)

        return {

            "temperatura_ambiente": 0,

            "descricao": "indisponível",

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
        return "excelente"

    elif ier > 50:
        return "boa"

    elif ier > 30:
        return "atencao"

    return "critica"

# CRIAR CSV

def criar_csv(qtd_cores):

    if os.path.exists(ARQUIVO_CSV):
        return

    cabecalho = [

        "timestamp"

    ]

    for i in range(1, qtd_cores + 1):

        cabecalho.append(
            f"core_{i}"
        )

    cabecalho.extend([

        "temp_max",

        "status",

        "margem_termica",

        "status_margem",

        "temperatura_ambiente",

        "clima",

        "umidade",

        "fan_principal_rpm",

        "indice_resfriamento",

        "status_resfriamento",

        "latencia_ms",

        "throttling"

    ])

    with open(
        ARQUIVO_CSV,
        "w",
        newline="",
        encoding="utf-8"
    ) as arquivo:

        writer = csv.writer(
            arquivo,
            delimiter=";"
        )

        writer.writerow(cabecalho)

# SALVAR CSV

def salvar():

    temperaturas = buscar_temperaturas()

    if len(temperaturas) == 0:

        print(
            "Nenhuma temperatura encontrada."
        )

        return

    if not os.path.exists(ARQUIVO_CSV):

        criar_csv(
            len(temperaturas)
        )

    clima = obter_clima()

    latencia = obter_latencia()

    temp_max = max(
        temperaturas.values()
    )

    fan_principal = simular_fan_cpu(
        temp_max
    )

    ier = calcular_ier(

        fan_principal,

        temp_max,

        clima["temperatura_ambiente"]

    )

    status_ier = classificar_ier(
        ier
    )

    status = classificar(
        temp_max
    )

    margem_termica = calcular_margem_termica(
        temp_max
    )

    status_margem = classificar_margem(
        margem_termica
    )

    throttling = verificar_throttling(
        temp_max
    )

    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    linha = [

        timestamp

    ]

    for valor in temperaturas.values():

        linha.append(valor)

    linha.extend([

        temp_max,

        status,

        margem_termica,

        status_margem,

        clima["temperatura_ambiente"],

        clima["descricao"],

        clima["umidade"],

        fan_principal,

        ier,

        status_ier,

        latencia,

        throttling

    ])

    with open(
        ARQUIVO_CSV,
        "a",
        newline="",
        encoding="utf-8"
    ) as arquivo:

        writer = csv.writer(
            arquivo,
            delimiter=";"
        )

        writer.writerow(linha)


    # TERMINAL


    print("\n============================")

    print(f"Horário: {timestamp}")

    for core, temp in temperaturas.items():

        print(
            f"{core.upper()}: "
            f"{temp}°C"
        )

    print(f"\nTemp Máxima: {temp_max}°C")

    print(
        f"Margem Térmica: "
        f"{margem_termica}°C"
    )

    print(
        f"Status Margem: "
        f"{status_margem}"
    )

    print(
        f"Fan Principal: "
        f"{fan_principal} RPM"
    )

    print(
        f"IER: "
        f"{ier}"
    )

    print(
        f"Status Resfriamento: "
        f"{status_ier}"
    )

    print(
        f"Latência: "
        f"{latencia}ms"
    )

    print(
        f"Throttling: "
        f"{throttling}"
    )

# LOOP

print("Captura iniciada.")

while True:

    salvar()

    time.sleep(INTERVALO)