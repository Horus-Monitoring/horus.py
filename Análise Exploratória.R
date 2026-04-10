# Análise das Métricas do Projeto

# Paleta de Cores (Criar Variáveis)
azul1 <- "#78c0e0"
azul2 <- "#449dd1"
azul3 <- "#192bc2"
azul4 <- "#150578"
azul5 <- "#0e0e52"

# Dados importantes para análise:
# Análise de feriados e finais de semana (com pesquisas)
# Análise da relação entre os componentes e processos
# Adicionar coluna para setores (Airspace Management(CPU) e 
# Track Correlation / Flight Plan(Armazenamento))
# Pesquisa dos componentes priorizados em cada setor 
# Transformar bytes em MB
# Track Correlation / Flight Plan

df_horus <- data.frame(sagitario)

# remover o símbolo '%' da coluna 'cpu_percent'
df_horus$cpu_percent <- gsub("\\%", "", df_horus$cpu_percent)

# remover o 'GB' da coluna 'memory_available_gb'
df_horus$memory_available_gb <- gsub("\\GB", "", df_horus$memory_available_gb)

# remover o símbolo '%' da coluna 'disk_usage_percent'
df_horus$disk_usage_percent <- gsub("\\%", "", df_horus$disk_usage_percent)

# transformando as colunas para tipo numérico
df_horus$cpu_percent <- as.numeric(df_horus$cpu_percent)
mean(df_horus$cpu_percent)

df_horus$memory_available_gb <- as.numeric(df_horus$memory_available_gb)
mean(df_horus$memory_available_gb)

df_horus$disk_usage_percent <- as.numeric(df_horus$disk_usage_percent)
mean(df_horus$disk_usage_percent)

hist(df_horus$cpu_percent,
     main = c("Relação entre Uso de CPU durante o Mês"),
     col = (azul1),
     xlab = "CPU(%)",
     ylab = "frequência")

hist(df_horus$memory_available_gb,
     main = c("Relação entre Uso de Memória durante o Mês"),
     col = (azul2),
     xlab = "Memória(GB)",
     ylab = "frequência")

hist(df_horus$disk_usage_percent,
     main = c("Relação entre Uso de Disco durante o Mês"),
     col = (azul3),
     xlab = "Disco(%)",
     ylab = "frequência")

# Instalando o pacote lubridate para manipulação de datas
install.packages("lubridate")
library(lubridate)

# Adicionando coluna do dia da semana de acordo com a data
df_horus$dia_semana <- wday(df_horus$timestamp, label = TRUE, abbr = TRUE)

# OBS: transformar em numerico para fazer o histograma!
hist(df_horus$dia_semana) 
