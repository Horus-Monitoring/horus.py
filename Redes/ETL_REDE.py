import requests

def dados_aviationstack():
    
    params = {
    'access_key': 'e2326bc56d7d29aab7be45590b9c1aa1',
    'dep_iata': 'GRU',
    'limit': 100
    }

    response = requests.get('https://api.aviationstack.com/v1/flights', params = params)

    data = response.json()
    data_aviationstack = data['data']
    data_api = [["Número do voo", "Status", "Origem", "Destino", "Delay de Partida", "Delay de Chegada"]]


    for voo in data_aviationstack:
        numero_voo = voo.get('flight', {}).get('iata') #IATA é um código composto pela companhia + numero do voo
        status = voo.get('flight_status') 
        origem = voo.get('departure', {}).get('airport')
        destino = voo.get('arrival', {}).get('airport')
        delay_origem = voo.get('departure', {}).get('delay')
        delay_destino = voo.get('arrival', {}).get('delay')
        data_api.append([numero_voo, status, origem, destino, delay_origem, delay_destino])

    return(data_api)

dados_aviationstack()