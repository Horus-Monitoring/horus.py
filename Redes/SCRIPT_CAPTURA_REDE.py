import psutil 
import random
import time
from datetime import datetime
from getmac import get_mac_adress #Função específica para MAC Adress
import subprocess #Permite executar comandos no sistema operacional
import re #Manipulação de strings

def tempo_atual(): #Coleta a data-hora
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def coletar_mac_adress(): #Coleta o MAC Adress
    return get_mac_adress()

def coletar_dados_rede(): #Coleta dados para métricas de fluxo de rede e pacotes
    network = psutil.net_io_counters();
    return{
        "bytes_recv": network.bytes_recv,
        "bytes_sent": network.bytes_sent,
        "pack_recv": network.packets_recv,
        "pack_sent": network.packets_sent
    }
def ping_shell():
    cmd = ["ping", "-n", "10", "8.8.8.8" ]

    try:
        cmd = ["ping", "-n", "10", "8.8.8.8" ] #-n para Windowns e -c para Ubuntu

        resultado = subprocess.run(cmd, capture_output=True, text=True, check=True) #Executa o comando no shell
        saida = resultado.stdout #Captura a saída (Standard Output)
        
        saida = " ".join(saida.split()) #remove quebras de linha para facilitar o regex

        return saida

    except subprocess.CalledProcessError: #Chama uma "exception", como no Java
        print("Erro ao executar o comando.")
        return None

def coletar_pacotes():
    saida = ping_shell()
    
    padrao = r"\((\d+)% de perda\)" #Verificar a saída padrão no ubuntu para modificar
    #Manipulando string onde \d+ recebe qualquer número, \(\) busca por parênteses e \s considera quebra de linha 
    match = re.search(padrao, saida) #Buscando a string na saida do shell
        
    if match:
        perda = match.group(1) #captura o primeiro resultado obtido na expressão regular na var padrao
        return int(perda)
    else:
        return None
        
def coletar_latencia():
    saida = ping_shell()

    padrao_tempo = r"tempo=(\d+)ms" 
    tempos = re.findall(padrao_tempo, saida)

    if tempos:
        return tempos
    else:
        print("Erro ao capturar o tempo de latência.")
        return None
    
def coletar_latencia_componentes(): #simulação da latencia entre os diferentes componentes do SAGITARIO
    return {
        "lat_adsb_rastreamento": round(random.uniform(20,50),2),
        "lat_rastreamento_correlacao": round(random.uniform(30,80),2),
        "lat_correlacao_rotas": round(random.uniform(40,100),2),
        "lat_rotas_api": round(random.uniform(10,40),2),
        "lat_api_bd": round(random.uniform(50,150),2),
        "lat_bd_sync": round(random.uniform(30,90),2)
    }

def coletar_banda_processos(total_aeronaves):
    return {
        "rastreamento_mbps": round(total_aeronaves * random.uniform(0.4,0.8),2),
        "rotas_mbps": round(total_aeronaves * random.uniform(0.2,0.5),2),
        "correlacao_mbps": round(total_aeronaves * random.uniform(0.3,0.7),2),
        "api_gateway_mbps": round(total_aeronaves * random.uniform(0.1,0.4),2),
        "bd_mbps": round(total_aeronaves * random.uniform(0.2,0.6),2),
        "sync_service_mbps": round(total_aeronaves * random.uniform(0.15,0.5),2)
    }

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
