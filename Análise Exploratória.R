# Análise das Métricas do Projeto

# Paleta de Cores (Criar Variáveis)
# 78c0e0
# 449dd1
# 192bc2
# 150578
# 0e0e52

# Dados importantes para análise:
# Análise de feriados e finais de semana (com pesquisas)
# Análise da relação entre os componentes e processos
# Adicionar coluna para setores (Airspace Management(CPU) e 
# Track Correlation / Flight Plan(Armazenamento))
# Pesquisa dos componentes priorizados em cada setor 
# Transformar bytes em MB

df_horus <- data.frame(sagitario)

# remover o símbolo '%' da coluna 'cpu_percent'
df_horus$cpu_percent <- gsub("\\%", "", df_horus$cpu_percent)

# transformar uma coluna no rstudio para numérica
df_horus$cpu_percent <- as.numeric(df_horus$cpu_percent)
mean(df_horus$cpu_percent)

hist(df_horus$cpu_percent,
     main = c("Relação entre Uso de CPU durante o Mês"),
     col = ("#78c0e0"),
     xlab = "CPU(%)",
     ylab = "frequência")

# Track Correlation / Flight Plan
 