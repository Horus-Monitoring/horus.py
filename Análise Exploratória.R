# Análise das Métricas do Projeto

# Paleta de Cores (Criar Variáveis)
azul1 <- "#78c0e0"
azul2 <- "#449dd1"
azul3 <- "#192bc2"
azul4 <- "#150578"
azul5 <- "#0e0e52"

# Dados importantes para análise:
# Pesquisa dos componentes priorizados em cada setor 
# Adicionar coluna para setores (Airspace Management(CPU) e 
# Track Correlation / Flight Plan(Armazenamento))
# Análise de feriados e finais de semana (com pesquisas)
# Análise da relação entre os componentes e processos
# IOWAIT(Percentual do Tempo que a CPU fica esperando operações I/O) - Disco
# SWAP(Memória Virtual do Disco) - RAM
# mean(df_horus$proc_asm_cpu) 
# mean(df_horus$proc_correlation_cpu) 
# mean(df_horus$proc_db_cpu)
# mediana, desvio padrão e moda
# variavel qualitativa ordinal
# ggplot
# variavel limite
# coluna de incidentes
# coluna custo
# coluna data e hora de resolução do incidente
# coluna com localização
# coluna com o nome do servidor
# coluna com ip
# coluna com so
# coluna status

df_horus <- data.frame(sagitario)

# Tratamento de Dados 

# remover o símbolo '%' da coluna 'cpu_percent'
df_horus$cpu_percent <- gsub("\\%", "", df_horus$cpu_percent)

# remover o 'GB' da coluna 'memory_available_gb'
df_horus$memory_available_gb <- gsub("\\GB", "", df_horus$memory_available_gb)

# remover o símbolo '%' da coluna 'disk_usage_percent'
df_horus$disk_usage_percent <- gsub("\\%", "", df_horus$disk_usage_percent)

# remover o símbolo '%' da coluna 'proc_asm_cpu'
df_horus$proc_asm_cpu <- gsub("\\%", "", df_horus$proc_asm_cpu)

# remover o símbolo '%' da coluna 'proc_correlation_cpu'
df_horus$proc_correlation_cpu <- gsub("\\%", "", df_horus$proc_correlation_cpu)

# remover o símbolo '%' da coluna 'proc_db_cpu'
df_horus$proc_db_cpu <- gsub("\\%", "", df_horus$proc_db_cpu)

# transformando as colunas para tipo numérico
df_horus$cpu_percent <- as.numeric(df_horus$cpu_percent)
df_horus$memory_available_gb <- as.numeric(df_horus$memory_available_gb)
df_horus$disk_usage_percent <- as.numeric(df_horus$disk_usage_percent)
df_horus$proc_asm_cpu <- as.numeric(df_horus$proc_asm_cpu)
df_horus$proc_correlation_cpu <- as.numeric(df_horus$proc_correlation_cpu)
df_horus$proc_db_cpu <- as.numeric(df_horus$proc_db_cpu)

# Transformando Bytes para KiloBytes
df_horus$net_bytes_sent <- df_horus$net_bytes_sent/1000
df_horus$net_bytes_recv <- df_horus$net_bytes_recv/1000

# Médias dos Componentes - Geral 
media_cpu <- mean(df_horus$cpu_percent)
media_memoria <- mean(df_horus$memory_available_gb)
media_disco <- mean(df_horus$disk_usage_percent)
media_bytes_env <- mean(df_horus$net_bytes_sent)
media_bytes_recv <- mean(df_horus$net_bytes_recv)
media_latencia <- mean(df_horus$latency_ms) 
media_processos <- mean(df_horus$active_processes) 
media_iowait <- mean(df_horus$iowait_percent)  
media_swap <- mean(df_horus$swap_percent) 

# Classificação das Colunas 
# Qualitativa Nominal: dia_semana
# Quantitativa Contínua: cpu_percent, memory_available_gb, disk_usage_percent,
# proc_asm_cpu, proc_correlation_cpu, proc_db_cpu, latency_ms, iowait_percent, swap_percent 
# Quantitativa Discreta: net_bytes_sent, net_bytes_recv, active_processes

# Analisando o comportamento e distribuição de cada uma das colunas e suas médias
hist(df_horus$cpu_percent,
     main = c("Relação entre Uso de CPU durante o Mês"),
     col = (azul1),
     xlab = "CPU(%)",
     ylab = "Frequência")
abline(v = media_cpu, col = azul5, lwd = 2)

hist(df_horus$memory_available_gb,
     main = c("Relação entre Uso de Memória durante o Mês"),
     col = (azul2),
     xlab = "Memória(GB)",
     ylab = "Frequência")
abline(v = media_memoria, col = azul5, lwd = 2)

hist(df_horus$disk_usage_percent,
     main = c("Relação entre Uso de Disco durante o Mês"),
     col = (azul3),
     xlab = "Disco(%)",
     ylab = "Frequência")
abline(v = media_disco, col = azul1, lwd = 2)

hist(df_horus$latency_ms,
     main = c("Distribuição do tempo de latência durante o Mês"),
     col = (azul4),
     xlab = "Latência(m/s)",
     ylab = "Frequência")
abline(v = media_disco, col = azul1, lwd = 2)

hist(df_horus$iowait_percent,
     main = c("Distribuição de I/O Wait (%) durante o Mês"),
     col = (azul5),
     xlab = "I/O Wait (%)",
     ylab = "Frequência")
abline(v = media_iowait, col = azul1, lwd = 2)

hist(df_horus$net_bytes_sent,
     main = c("Distribuição da Quantidade de Bytes Enviados Durante o Mês"),
     col = (azul1),
     xlab = "KiloBytes(KB)",
     ylab = "Frequência")
abline(v = media_bytes_env, col = azul5, lwd = 2)

hist(df_horus$net_bytes_recv,
     main = c("Distribuição da Quantidade de Bytes Recebidos Durante o Mês"),
     col = (azul2),
     xlab = "KiloBytes(KB)",
     ylab = "Frequência")
abline(v = media_bytes_recv, col = azul4, lwd = 2)

hist(df_horus$active_processes,
     main = c("Distribuição da Quantidade de Processos Ativos Durante o Mês"),
     col = (azul3),
     xlab = "Processos Ativos",
     ylab = "Frequência")
abline(v = media_processos, col = azul1, lwd = 2)

# Instalando oactive_processes# Instalando o pacote lubridate para manipulação de datas
install.packages("lubridate")
library(lubridate)

# Adicionando coluna do dia da semana de acordo com a data
df_horus$dia_semana <- wday(df_horus$timestamp, label = TRUE, abbr = TRUE)


