# Análise das Métricas do Projeto
library("ggplot2")
install.packages("ggplot2")

# Paleta de Cores (Criar Variáveis)
azul1 <- "#78c0e0"
azul2 <- "#449dd1"
azul3 <- "#192bc2"
azul4 <- "#150578"
azul5 <- "#0e0e52"

# Ideias para adicionar
# Análise de feriados e finais de semana (com pesquisas)
# IOWAIT(Percentual do Tempo que a CPU fica esperando operações I/O) - Disco
# Fontes, Considerações Finais e Análise
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
# nova kpi analista

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
media_latencia <- mean(df_horus$latency_ms) 
# media_processos <- mean(df_horus$active_processes) 
# media_iowait <- mean(df_horus$iowait_percent)  

# Classificação das Colunas 
# Qualitativa Nominal: dia_semana
# Quantitativa Contínua: cpu_percent, memory_available_gb, disk_usage_percent,
# latency_ms, iowait_percent, swap_percent, memory_percent 
# Quantitativa Discreta: net_bytes_sent, net_bytes_recv, active_processes

# Analisando o comportamento e distribuição de CPU, RAM e DISCO

# DESVIO PADRÃO
# 23 - 30 (CPU, Latência, Disco) / aumento de outliers

# CPU 

cores <- ifelse(
  df_horus$periodo == "Durante",
  "red", azul2)

hist(df_horus$cpu_percent,
     main = c("Relação entre Uso de CPU durante o Mês"),
     col = (azul1),
     xlab = "CPU(%)",
     ylab = "Frequência")
abline(v = media_cpu, col = azul5, lwd = 2)

print(paste("Média CPU:", round(media_cpu, 1)))
print(paste("Mediana CPU:", round(mediana_cpu, 1)))
print(paste("Desvio CPU:", round(desvio_cpu, 1)))

# Convertendo a coluna timestamp para um formato de data e hora, possibilitando a manipulação de datas
df_horus$timestamp <- as.POSIXct(
  df_horus$timestamp,
  format = "%Y-%m-%d %H:%M:%S"
)

plot(df_horus$timestamp, df_horus$cpu_percent,
     main = "Distribuição do Uso de CPU Durante o Mês",
     ylab = "CPU(%)",
     xlab = "Dias do Mês",
     col = cores)

legend("topleft",
       legend = c("Normal", "Incidente"),
       col = c(azul2, "red"),
       pch = 16)

# Através da análise do histograma e do plot, é possível notar que durante o mês
# o uso da CPU ficou concentrado entre o intervalo de 20% a 40%, apresentando 
# alguns outliers (casos isolados - incidentes), tendo como foco o dia 16 de 
# março, no qual o uso percentual chegou a 90% durante algumas horas, voltando 
# ao seu comportamento usual logo em seguida. Ademais, vale destacar que 
# a média e a mediana do uso da cpu durante o mês de março possuem valor 
# aproximado, o que indica simetria e equilibrio na distribuição dos dados durante
# o período de análise. 


# IOWAIT
# RAM

hist(df_horus$memory_used,
     main = c("Distribuição do Uso de Memória durante o Mês"),
     col = (azul1),
     xlab = "Memória(%)",
     ylab = "Frequência",
     xlim = c(20,100))
abline(v = media_memoria, col = azul5, lwd = 2)

print(paste("Média RAM:", round(media_memoria, 1)))
print(paste("Mediana RAM:", round(mediana_memoria, 1)))
print(paste("Desvio RAM:", round(desvio_memoria, 1)))

plot(df_horus$timestamp, df_horus$memory_used,
     main = "Distribuição do Uso de RAM Durante o Mês",
     ylab = "RAM(%)",
     xlab = "Dias do Mês",
     col = cores)

legend("topleft",
       legend = c("Normal", "Incidente"),
       col = c(azul2, "red"),
       pch = 16)

# correlação - regressão linear
df_antesIncidente <- subset(df_horus,
                     as.Date(timestamp) >= "2026-03-11" &
                       as.Date(timestamp) <= "2026-03-15"
)

cor(as.numeric(df_antesIncidente$timestamp),
    df_antesIncidente$memory_used)

ggplot(data = df_antesIncidente, aes(timestamp, memory_used)) + 
  geom_point() +
  geom_smooth(method = "lm",
              se = FALSE) + theme_gray()

plot(df_semana$timestamp, df_semana$memory_used, type = "l",
     main = "Uso de Memória RAM na semana do incidente",
     xlab = "Tempo", ylab = "RAM(%)",
     col = (azul3),
     lwd = 2)

plot(df_horus$active_processes, df_horus$memory_used,
     col = cores,
     xlab = "Quantidade de Processos Ativos",
     ylab = "Memória(%)",
     main = "Relação entre Quantidade de Processos Ativos e Memória Usada")

legend("topleft",
       legend = c("Normal", "Incidente"),
       col = c(azul2, "red"),
       pch = 16)

plot(df_horus$timestamp, df_horus$swap_percent, 
     type = "l",
     lwd = 3,
     col = (azul1),
     xlab = "Março 2026",
     ylab = "Swap(%)",
     main = "Uso de Swap ao Longo do Mês")
grid()

abline(v = as.POSIXct("2026-03-16"), col = (azul5), lty = 3, lwd = 2)

hist(df_horus$swap_percent,
     main = "Distribuição do Percentual de Swap",
     xlab = "Swap(%)",
     ylab = "Frequência",
     col = c(azul3))

df_horus$periodo <- ifelse(df_horus$timestamp < as.POSIXct("2026-03-16"),
                           "Antes",
                    ifelse(df_horus$timestamp < as.POSIXct("2026-03-16 11:00:00"),
                           "Durante", "Depois"))
df_horus$periodo <- factor(df_horus$periodo, levels = c("Antes", "Durante", "Depois"))

boxplot(swap_percent ~ periodo, data = df_horus,
        col = (azul1),
        main = "Swap Antes, Durante e Depois do Incidente",
        xlab = "",
        ylab = "SWAP(%)")

# relação entre swap e memória
# df_horus$swap_faixa <- cut(df_horus$swap_percent, breaks = c(0, 20, 40, 80, 100))
# boxplot(memory_used ~ swap_faixa, data = df_horus)

# latência

plot(df_horus$timestamp, df_horus$latency_ms,
     main = "Distribuição da latência em ms durante o mês",
     ylab = "Latência(ms)",
     xlab = "Dias do Mês",
     col = cores)

legend("topright",
       legend = c("Normal", "Incidente"),
       col = c(azul2, "red"),
       pch = 16)

plot(df_horus$timestamp, df_horus$latency_ms, type = "l",
     main = "Latência ao longo do tempo",
     xlab = "Tempo", ylab = "Latência (ms)",
     col = (azul2))


boxplot(latency_ms ~ periodo, data = df_horus,
        main = "Latência antes, durante e depois do incidente",
        ylab = "Latência (ms)",
        xlab = "",
        col = (azul4),
        medcol = "#f1faee")

plot(df_horus$memory_used, df_horus$latency_ms,
     main = "Relação entre Uso Percentual de Memória RAM e Latência",
     xlab = "RAM(%)",
     ylab = "Latência (ms)",
     col = cores)

legend("topleft",
       legend = c("Normal", "Incidente"),
       col = c(azul2, "red"),
       pch = 16)

dia_escolhido <- as.Date("2026-03-16")

df_dia <- subset(df_horus, as.Date(timestamp) == dia_escolhido)

plot(df_dia$timestamp, df_dia$latency_ms, type = "l",
     main = "Latência no dia 16",
     xlab = "Tempo", ylab = "Latência (ms)",
     col = (azul3),
     lwd = 3, xaxt = "n")

axis.POSIXct(1,
             at = seq(min(df_dia$timestamp),
                      max(df_dia$timestamp),
                      by = "1 hour"),
             format = "%H:%M")

inicio <- as.POSIXct("2026-03-10")
fim    <- as.POSIXct("2026-03-17 00:00:00")
df_semana <- subset(df_horus, timestamp >= inicio & timestamp <= fim)

plot(df_semana$timestamp, df_semana$latency_ms, type = "l",
     main = "Latência na semana do incidente",
     xlab = "Tempo", ylab = "Latência (ms)",
     col = (azul1),
     lwd = 2, xaxt = "n")

axis.POSIXct(1,
             at = seq(min(df_semana$timestamp),
                      max(df_semana$timestamp),
                      by = "1 day"),
             format = "%d/%m")

dia_antes   <- as.Date("2026-03-10")
dia_depois  <- as.Date("2026-03-20")

df_antes   <- df_horus[as.Date(df_horus$timestamp) == dia_antes, ]
df_durante <- df_horus[as.Date(df_horus$timestamp) == dia_durante, ]
df_depois  <- df_horus[as.Date(df_horus$timestamp) == dia_depois, ]

df_antes$hora   <- format(df_antes$timestamp, "%H:%M")
df_dia$hora <- format(df_dia$timestamp, "%H:%M")
df_depois$hora  <- format(df_depois$timestamp, "%H:%M")

# range() garante que o eixo Y vai do menor valor entre os 3 dias até o maior valor entre os 3 dias
plot(df_antes$latency_ms, type = "l", col = "blue", lwd = 2,
     ylim = range(c(df_antes$latency_ms,
                    df_dia$latency_ms,
                    df_depois$latency_ms), na.rm = TRUE),
     xaxt = "n",
     xlab = "Hora do dia", ylab = "Latência (ms)",
     main = "Comparação de Latência Antes, Durante, Depois do Incidente")

lines(df_dia$latency_ms, col = "red", lwd = 2)
lines(df_depois$latency_ms, col = "darkgreen", lwd = 2)

# 1 - horizontal
# nrow() número de linhas
axis(1,
     at = seq(1, nrow(df_antes), length.out = 8),
     labels = df_antes$hora[seq(1, nrow(df_antes), length.out = 8)])

legend("topright",
       legend = c("Antes", "Durante", "Depois"),
       col = c("blue", "red", "darkgreen"),
       lty = 1,
       lwd = 2)


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
# ficou entre 300 e ultrapassou o número de 400 em determinados momentos. Além disso, foi
# realizada a observação do uso da memória swap ao longo do mês, na qual é possível notar
# o crescimento lento e contínuo, até chegar no dia 16 de março, onde ocorreu um salto
# abrupto, representando um evento (incidente) no servidor. Após isso, o uso de swap 
# permaneceu em 100%, não retornando ao comportamento anterior. A diferença de comportamento
# da memória swap pode ser analisada também por meio de um boxplot, que indica a variação entre
# antes, durante e depois do dia 16. Antes: o intervalo da swap teve um crescimento no intervalo de 5% a
# 23%, apresentando baixa variabilidade (caixa pequena), tendo um valor máximo por volta dos 25%, sem 
# outliers, demonstrando estabilidade e previsibilidade. Durante: em um intervalo de 7 horas o uso de swap 
# foi de 37% até 100%, apresentando alta variação entre os dados (caixa grande). Depois: permanence 
# neste valor até o final da análise não tendo variação, o que pode ser observado por meio da caixa 
# "achatada". Entre uma das explicações, podemos citar o fato de que a memória swap não é "auto limpante", 
# ou seja, ela só é liberada quando os processos terminam ou quando os dados armazenados sejam necessários 
# para algum processo. Caso o contrário, eles permanecem na swap ocupando espaço. 

# Relação entre memória swap e memória ram. 
# Latência

# Assim, ressalta-se
# a importância do monitoramento contínuo do uso da memória, sendo essencial para identificar
# anomalias, evitando possíveis conflitos e prejuízos.


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

# Ordena os dados e captura os 5 processos com maior uso de memória
top5 <- processos[order(processos$Porcentagem.de.Memoria), ][1:5, ]

# Instalando o pacote lubridate para manipulação de datas
# DIAS
install.packages("lubridate")
library(lubridate)

# Adicionando coluna do dia da semana de acordo com a data
df_horus$dia_semana <- wday(df_horus$timestamp, label = TRUE, abbr = TRUE)

