import random

base = {
        "rastreamento_loss": round(random.expovariate(3),2),
        "correlacao_loss": round(random.expovariate(3),2),
        "rotas_loss": round(random.expovariate(3),2),
        "api_loss": round(random.expovariate(3),2),
        "bd_loss": round(random.expovariate(3),2),
        "sync_loss": round(random.expovariate(3),2)
    }

print(base)