import requests
import random



def perda_pacotes_componentes():

    return {
        "rastreamento_loss": round(random.expovariate(3),2),
        "correlacao_loss": round(random.expovariate(3),2),
        "rotas_loss": round(random.expovariate(3),2),
        "api_loss": round(random.expovariate(3),2),
        "bd_loss": round(random.expovariate(3),2),
        "sync_loss": round(random.expovariate(3),2)
    }
    #Uso de variação exponencial para tornar a perda mais próxima de 1%

def dados_opensky():
    url = "https://opensky-network.org/api/states/all"
    response = requests.get(url) #Response é um objeto HTTP

    if response.status_code == 200: #Requisição bem sucedida
        response_json = response.json()
        total_flights = 0
        for r in response_json["states"]:
            if r[2] == "Brazil" or r[2] == "Brasil" or r[2] == "BR":
                print (r)
                total_flights += 1
        return total_flights
    else:
        print("Erro na requisição à API OpenSky Network.")
        return None
    
def coletar_banda_processos(total_aeronaves = dados_opensky()):
    print(total_aeronaves)
    return {
        "rastreamento_mbps": round(total_aeronaves * random.uniform(0.4,0.8),2),
        "rotas_mbps": round(total_aeronaves * random.uniform(0.2,0.5),2),
        "correlacao_mbps": round(total_aeronaves * random.uniform(0.3,0.7),2),
        "api_gateway_mbps": round(total_aeronaves * random.uniform(0.1,0.4),2),
        "bd_mbps": round(total_aeronaves * random.uniform(0.2,0.6),2),
        "sync_service_mbps": round(total_aeronaves * random.uniform(0.15,0.5),2)
    }

banda = coletar_banda_processos()
print(banda)