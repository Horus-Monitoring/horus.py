import psutil
import pandas as pd
from datetime import timedelta
import json
import time 
import csv


caminho_csv = 'dados-brutos_maquina.csv'

while True: 


    dados_brutos = pd.read_csv(caminho_csv, sep=';')




    dados_brutos['DATA_HORA'] = pd.to_datetime(dados_brutos['DATA_HORA'])

    limite_tempo = pd.Timestamp.now() - timedelta(minutes=5)




    qtd_linha_atual = len(dados_brutos) - 1


    # criando as variaveis pico calcular a média
    mediaram = 0
    mediadisco = 0 

    mediapercentram = 0
    mediapercentcpu = 0
    mediapercentdisco = 0 

    # criando as variaveis de pico
    picoram = dados_brutos['RAM_USADA'][qtd_linha_atual]
    picodisco = dados_brutos['DISCO_USADO'][qtd_linha_atual]

    picopercentram =  dados_brutos['RAM_PERCENT'][qtd_linha_atual]
    picopercentdisco = dados_brutos['DISCO_PERCENT'][qtd_linha_atual]
    picopercentcpu = dados_brutos['CPU'][qtd_linha_atual]

    # está variavel serve para contar quantas linhas tem que passam no limite de 5 minutos
    cont_linhas_soma = 0
    
    while qtd_linha_atual >= 0:
        if(dados_brutos['DATA_HORA'][qtd_linha_atual] >= limite_tempo ):
            
            
            mediaram = mediaram + dados_brutos['RAM_USADA'][qtd_linha_atual]
            mediapercentram = mediapercentram + dados_brutos['RAM_PERCENT'][qtd_linha_atual]
        
            mediadisco = mediadisco + dados_brutos['DISCO_USADO'][qtd_linha_atual]
            mediapercentdisco = mediapercentdisco + dados_brutos['DISCO_PERCENT'][qtd_linha_atual]

            mediapercentcpu = mediapercentcpu + dados_brutos['CPU'][qtd_linha_atual]
            cont_linhas_soma = cont_linhas_soma + 1
        
            # pegando os picos do uso de ram

            momento_atual_uso_ram = dados_brutos['RAM_USADA'][qtd_linha_atual]
            momento_atual_percent_ram = dados_brutos['RAM_PERCENT'][qtd_linha_atual]

            if momento_atual_uso_ram > picoram:
                picoram = momento_atual_uso_ram

            if momento_atual_percent_ram > picopercentram:
                picopercentram = momento_atual_percent_ram


            # pegando os picos do uso de disco

            momento_atual_uso_disco = dados_brutos['DISCO_USADO'][qtd_linha_atual]
            momento_atual_percent_disco = dados_brutos['DISCO_PERCENT'][qtd_linha_atual]

            if momento_atual_uso_disco > picodisco:
                picodisco = momento_atual_uso_disco

            if momento_atual_percent_disco > picopercentdisco:
                picopercentdisco = momento_atual_percent_disco

            # pegando os picos do uso de cpu

            momento_atual_percent_cpu = dados_brutos['CPU'][qtd_linha_atual]

            if momento_atual_percent_cpu > picopercentcpu:
                picopercentcpu = momento_atual_percent_cpu

         
        
        qtd_linha_atual -= 1

    

    # deixando as médias que estão em byte para gb

    mediaram = mediaram / (1024 ** 3)
    mediadisco = mediadisco / (1024 ** 3)

    # calculando a média da porcentagem e dos dados com a quantidade de linhas 

    mediaram = round(mediaram / cont_linhas_soma, 2)
    mediadisco = round(mediadisco / cont_linhas_soma, 2)

    mediapercentcpu = round(mediapercentcpu / cont_linhas_soma, 2)
    mediapercentdisco = round(mediapercentdisco / cont_linhas_soma,2)
    mediapercentram = round(mediapercentram / cont_linhas_soma,2)


    # deixando os picos que estão em byte para gb

    picodisco = round(picodisco / (1024 ** 3), 2)
    picoram = round(picoram / (1024 ** 3),2 )


    # pegando o usuario atual

    usuario_atual = dados_brutos['USER'][qtd_linha_atual + 1]

    # criando o csv com os dados tratados

    arquivo_csv = 'dados_tratados_maquina.csv'

    with open(arquivo_csv, 'a', newline='') as csvfile:
        colunas = ['USER', 'MEDIA_CPU','PICO_CPU', 'MEDIA_RAM', 'PICO_RAM','PORCENTAGEM_RAM', 'PICO_P_RAM', 'MEDIA_DISCO', 'PICO_DISCO', 'PORCENTAGEM_DISCO', 'PICO_P_DISCO']
        CSV_DIC_WRITER = csv.DictWriter(csvfile, fieldnames=colunas, delimiter=';')

        if csvfile.tell() == 0:
            CSV_DIC_WRITER.writeheader()

        print(f"""
        Escrevendo os dados tratados dos ultimos 10 minutos
        
        Média uso cpu: {mediapercentcpu}
        Pico uso cpu: {picopercentcpu}

        Média uso ram: {mediaram}
        Média uso ram porcentagem: {mediapercentram}
        Pico uso ram: {picoram}
        Pico uso ram porcentagem {picopercentram}

        Média uso de disco: {mediadisco}
        Média uso de disco porcentagem: {mediapercentdisco}
        Pico uso de disco: {picodisco}
        Pico uso de disco porcentagem {picopercentdisco}

        """)

        escrita_dados = {'USER': usuario_atual, 'MEDIA_CPU': mediapercentcpu, 'PICO_CPU': picopercentcpu, 'MEDIA_RAM': mediaram, 'PICO_RAM': picoram, 'PORCENTAGEM_RAM': mediapercentram, 'PICO_P_RAM': picopercentram, 
                         'MEDIA_DISCO': mediadisco, 'PICO_DISCO': picodisco, 'PORCENTAGEM_DISCO': mediapercentdisco, 'PICO_P_DISCO': picopercentdisco  }
    
        CSV_DIC_WRITER.writerow(escrita_dados)
        csvfile.flush()
    time.sleep(150)

    


