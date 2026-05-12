import subprocess #Permite executar comandos no sistema operacional
import re #Manipulação de strings

cmd = ["ping", "-n", "10", "8.8.8.8" ]

resultado = subprocess.run(cmd, capture_output=True, text=True, check=True) #Executa o comando no shell
saida = resultado.stdout #Captura a saída (Standard Output)
print(saida)
saida = " ".join(saida.split()) #remove quebras de linha para facilitar o regex
print(saida)

padrao = r"\((\d+)% de perda\)"
#Manipulando string onde \d+ recebe qualquer número, \(\) busca por parênteses e \s considera quebra de linha 
match = re.search(padrao, saida) #Buscando a string na saida do shell
perda = match.group(1) #captura o primeiro resultado obtido na expressão regular na var padrao
print(f"Perda de pacotes estimada em {perda}%")