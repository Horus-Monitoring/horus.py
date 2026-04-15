# Análise das Métricas do Projeto

# Paleta de Cores (Criar Variáveis)
azul1 <- "#78c0e0"
azul2 <- "#449dd1"
azul3 <- "#192bc2"
azul4 <- "#150578"
azul5 <- "#0e0e52"

# Dados importantes para análise:
# Pesquisa dos componentes priorizados em cada setor 

# Quando tiver um dataframe completo:
# Análise de feriados e finais de semana (com pesquisas)
# Análise da relação entre os componentes e processos
# IOWAIT(Percentual do Tempo que a CPU fica esperando operações I/O) - Disco
# SWAP(Memória Virtual do Disco) - RAM
# Fontes, Considerações Finais e Análise

# Ideias para adicionar
# mediana e desvio padrão
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

# Removendo colunas que não serão utilizadas
df_horus$proc_asm_cpu <- NULL
df_horus$proc_correlation_cpu <- NULL
df_horus$proc_db_cpu <- NULL

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

# Assumindo que a memoria total é de 128GB (transformar em porcentagem)
memoria_total <- 128
df_horus$memory_available_gb <- df_horus$memory_available_gb * 8
df_horus$memory_percent <- round((df_horus$memory_available_gb * 100) / memoria_total, 1)

# Criando uma coluna de memoria usada em % de forma aproximada 
df_horus$memory_used <- 100 - df_horus$memory_percent


# Média, Mediana e Desvio Padrão dos Componentes
media_cpu <- mean(df_horus$cpu_percent)
mediana_cpu <- median(df_horus$cpu_percent)
desvio_cpu <- sd(df_horus$cpu_percent)

media_memoria <- mean(df_horus$memory_used)
mediana_memoria <- median(df_horus$memory_used)
desvio_memoria <- sd(df_horus$memory_used)

media_disco <- mean(df_horus$disk_usage_percent)
mediana_disco <- median(df_horus$disk_usage_percent)
desvio_disco <- sd(df_horus$disk_usage_percent)

# media_bytes_env <- mean(df_horus$net_bytes_sent)
# media_bytes_recv <- mean(df_horus$net_bytes_recv)
# media_latencia <- mean(df_horus$latency_ms) 
# media_processos <- mean(df_horus$active_processes) 
# media_iowait <- mean(df_horus$iowait_percent)  
# media_swap <- mean(df_horus$swap_percent) 

# Classificação das Colunas 
# Qualitativa Nominal: dia_semana
# Quantitativa Contínua: cpu_percent, memory_available_gb, disk_usage_percent,
# latency_ms, iowait_percent, swap_percent, memory_percent 
# Quantitativa Discreta: net_bytes_sent, net_bytes_recv, active_processes

# Identificar o comportamento dos servidores de cada "setor" do Sagiário
# Track Correlation / Flight Plan(Armazenamento)


# Analisando o comportamento e distribuição de CPU, RAM e DISCO

# CPU 
hist(df_horus$cpu_percent,
     main = c("Relação entre Uso de CPU durante o Mês"),
     col = (azul1),
     xlab = "CPU(%)",
     ylab = "Frequência")
abline(v = media_cpu, col = azul5, lwd = 2)

# Convertendo a coluna timestamp para um formato de data e hora, possibilitando a manipulação de datas
df_horus$timestamp <- as.POSIXct(
  df_horus$timestamp,
  format = "%Y-%m-%d %H:%M:%S"
)

plot(df_horus$timestamp, df_horus$cpu_percent,
     main = "Distribuição do Uso de CPU Durante o Mês",
     ylab = "CPU(%)",
     xlab = "Dias do Mês",
     col = (azul2))

print(paste("Média CPU:", round(media_cpu, 1)))
print(paste("Mediana CPU:", round(mediana_cpu, 1)))
print(paste("Desvio CPU:", round(desvio_cpu, 1)))

# Através da análise do histograma e do plot, é possível notar que durante o mês
# o uso da CPU ficou concentrado entre o intervalo de 20% a 40%, apresentando 
# alguns outliers (casos isolados - incidentes, tendo como foco o dia 16 de março). 
# A média e a mediana do uso da cpu durante esse período possuem valor, 
# o que indica simetria e equilibrio na distribuição dos dados. 


# RAM 
hist(df_horus$memory_used,
     main = c("Distribuição do Uso de Memória durante o Mês"),
     col = (azul2),
     xlab = "Memória(%)",
     ylab = "Frequência",
     xlim = c(20,100))
abline(v = media_memoria, col = azul5, lwd = 2)

plot(df_horus$timestamp, df_horus$memory_used,
     main = "Distribuição do Uso de RAM Durante o Mês",
     ylab = "RAM(%)",
     xlab = "Dias do Mês",
     col = (azul4))

plot(df_horus$active_processes, df_horus$memory_used,
     col = (azul1),
     xlab = "Quantidade de Processos Ativos",
     ylab = "Memória(%)",
     main = "Relação entre Quantidade de Processos Ativos e Memória Usada")

print(paste("Média RAM:", round(media_memoria, 1)))
print(paste("Mediana RAM:", round(mediana_memoria, 1)))
print(paste("Desvio RAM:", round(desvio_memoria, 1)))

# Buscando compreender melhor as causas para o incidente, foi feita a análise do
# uso de memória durante o mês. Com isso, por meio do histograma, conseguimos observar
# que o uso da RAM ficou na faixa de 30% a 50%, apresentando média e mediana com valores 
# aproximados. Apesar da simetria constante na maior parte dos dados, é notório casos específicos,
# onde o percentual de memória usada chegou no casa dos 90%. Essa discrepância fica ainda
# mais evidente no plot entre o uso da RAM durante os dias do mês de março. Por meio dele,
# nota-se que a partir do dia 11 até o dia 15, o percentual de ram usado teve um crescimento
# contínuo, o que ocasionou no seu ápice no dia 16 de março (durante às 00:00 até 10:00),
# até a resolução do incidente. Dessa forma, tendo como base o segundo plot, é possível 
# identificar que durante o período do uso extremo de RAM, a quantidade de processos ativos
# ficou entre 300 e ultrapassou o número de 400 em determinados momentos. Assim, ressalta-se
# a importância do monitoramento contínuo do uso da memória, sendo essencial para identificar
# anomalias, evitando assim possíveis conflitos e prejuízos.


# DISCO (23 - 30)
hist(df_horus$disk_usage_percent,
     main = c("Relação entre Uso de Disco durante o Mês"),
     col = (azul3),
     xlab = "Disco(%)",
     ylab = "Frequência")
abline(v = media_disco, col = azul1, lwd = 2)

plot(df_horus$timestamp, df_horus$disk_usage_percent,
     main = "Distribuição do Uso de CPU Durante o Mês",
     ylab = "CPU(%)",
     xlab = "Dias do Mês",
     col = (azul2))

plot(df_horus$timestamp, df_horus$iowait_percent,
     main = "Distribuição do Uso de CPU Durante o Mês",
     ylab = "CPU(%)",
     xlab = "Dias do Mês",
     col = (azul2))

hist(df_horus$iowait_percent,
     main = c("Distribuição de I/O Wait (%) durante o Mês"),
     col = (azul5),
     xlab = "I/O Wait (%)",
     ylab = "Frequência")
abline(v = media_iowait, col = azul1, lwd = 2)


# ================================================================================

# Outros Graficos
hist(df_horus$latency_ms,
     main = c("Distribuição do tempo de latência durante o Mês"),
     col = (azul4),
     xlab = "Latência(m/s)",
     ylab = "Frequência")
abline(v = media_disco, col = azul1, lwd = 2)

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

# Justificar o setor - RELACIONAR COM O SETOR, O QUAL TEM FOCO NO ARMAZENAMENTO
df_horus$setor <- "Flight Plan / Track Correlation"

# (Airspace Management(CPU))

# Dataframe completo:

# Instalando oactive_processes# Instalando o pacote lubridate para manipulação de datas
# DIAS
install.packages("lubridate")
library(lubridate)

# Adicionando coluna do dia da semana de acordo com a data
df_horus$dia_semana <- wday(df_horus$timestamp, label = TRUE, abbr = TRUE)

