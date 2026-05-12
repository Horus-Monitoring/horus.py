import requests
import random

def dados_opensky():
    url = "https://opensky-network.org/api/states/all"
    response = requests.get(url) #Response é um objeto HTTP
    response_json = response.json()
    if response.status_code == 200: #Requisição bem sucedida
        return response_json
    else:
        print("Erro na requisição à API OpenSky Network.")
        return None

def opensky_timestamp(response_json):
    return response_json["time"]

def opensky_aeronaves(response_json):
    total_flights = 0
    for r in response_json["states"]:
        if r[2] == "Brazil" or r[2] == "Brasil" or r[2] == "BR":
            total_flights += 1
    return total_flights
    

def coletar_banda_processos(total_aeronaves):
    return {
        "rastreamento_mbps": round(total_aeronaves * random.uniform(0.4,0.8),2),
        "rotas_mbps": round(total_aeronaves * random.uniform(0.2,0.5),2),
        "correlacao_mbps": round(total_aeronaves * random.uniform(0.3,0.7),2),
        "api_gateway_mbps": round(total_aeronaves * random.uniform(0.1,0.4),2),
        "bd_mbps": round(total_aeronaves * random.uniform(0.2,0.6),2),
        "sync_service_mbps": round(total_aeronaves * random.uniform(0.15,0.5),2)
    }

def contato_adsb(response_json):
    tempo_atual = response_json["time"]
    atualizacao = []
    for r in response_json["states"]:
        if r[2] == "Brazil" or r[2] == "Brasil" or r[2] == "BR":
            ultima_atualizacao = r[4]
            atualizacao.append(tempo_atual - ultima_atualizacao)
    return atualizacao 

banda = contato_adsb(dados_opensky())
print(banda)