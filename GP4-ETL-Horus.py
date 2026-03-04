import psutil
import pandas as pd
from datetime import timedelta
import json



caminho_csv = 'dados-brutos.csv'

dados_brutos = pd.read_csv(caminho_csv)