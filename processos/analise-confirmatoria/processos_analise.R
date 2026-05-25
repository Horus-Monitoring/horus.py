# Análise Confirmatória - Dashboard Processos

install.packages("tidyverse")
library(tidyverse)

df_processos <- data.frame(processos_incidente_narrativa)

# Convertendo a coluna timestamp para um formato de data e hora, possibilitando a manipulação de datas
df_processos$timestamp <- as.POSIXct(
  df_processos$timestamp,
  format = "%Y-%m-%d %H:%M:%S"
)

# Justificativa das KPI's e Dashboards

# (Status do Sistema)

# O monitoramento dos estados dos processos (running, sleeping e stopped) 
# é essencial para a infraestrutura do Sagitário, pois permite acompanhar o 
# comportamento operacional do sistema em tempo real e identificar sinais 
# de instabilidade ou sobrecarga. Os processos running indicam atividades em 
# execução e ajudam a detectar aumento de carga e consumo de recursos. Os 
# sleeping representam processos ativos, porém ociosos. Sendo assim, monitorá-los 
# é vital para entender por que o sistema não flui, permitindo observar mudanças 
# durante incidentes. Já os stopped representam processos interrompidos, sendo 
# importantes para identificar falhas ou indisponibilidades, servindo como segurança
# operacional, uma vez que se processos vitais estiverem interrompidos, a 
# segurança do espaço aéreo é diretamente comprometida. Assim, o acompanhamento 
# desses estados oferece uma visão mais completa da saúde do sistema, apoiando a
# detecção de incidentes, a análise de desempenho e a tomada de decisão sobre a 
# estabilidade da infraestrutura do Sagitário.

# Contagem de processos por timestamp e status
processos_status <- df_processos %>%
  group_by(timestamp, status) %>%
  summarise(qtd_processos = n(), .groups = "drop")

dados_plot <- processos_status %>%
  pivot_wider(
    names_from = status,
    values_from = qtd_processos,
    values_fill = 0
  )

plot(
  dados_plot$timestamp,
  dados_plot$running,
  type = "l",       
  col = "#38b000",
  lwd = 3,          
  ylim = c(0, max(
    dados_plot$running,
    dados_plot$sleeping,
    dados_plot$stopped
  )),
  xlab = "Horário do incidente",
  ylab = "Quantidade de processos",
  main = "Variação dos Estados dos Processos Durante o Incidente",
  xaxt = "n"
)

axis(1,
  at = dados_plot$timestamp[seq(1, nrow(dados_plot), by = 5)],
  labels = format(
    dados_plot$timestamp[seq(1, nrow(dados_plot), by = 5)],
    "%H:%M"),las = 0)


lines(
  dados_plot$timestamp,
  dados_plot$sleeping,
  col = "#2196f3",
  lwd = 3
)


lines(
  dados_plot$timestamp,
  dados_plot$stopped,
  col = "#ffd60a",
  lwd = 3
)

legend(
  "topleft",
  legend = c("Running", "Sleeping", "Stopped"),
  col = c("#38b000", "#2196f3", "#ffd60a"),
  lwd = 3,
  bty = "n"
)

# O gráfico de linhas dos estados dos processos demonstra como o comportamento 
# operacional do Sagitário varia ao longo do incidente, evidenciando a relação 
# entre atividade do sistema e utilização dos recursos. Observa-se um aumento 
# dos processos em estado running durante o pico do incidente, acompanhado da 
# redução dos processos sleeping, indicando que processos antes ociosos passaram
# a executar para atender à maior demanda operacional. Além disso, os processos 
# stopped permanecem relativamente baixos e estáveis, sugerindo que, apesar do 
# aumento de carga, não houve indisponibilidade significativa de serviços. 
# Dessa forma, o gráfico confirma a importância do monitoramento dos estados dos
# processos para identificar mudanças operacionais, compreender o impacto de 
# incidentes e apoiar decisões relacionadas ao desempenho e à estabilidade da 
# infraestrutura do Sagitário.


# (Processos Críticos)

# O monitoramento da quantidade de processos críticos ao longo do tempo é importante
# porque permite identificar padrões de instabilidade do sistema, mostrando não apenas
# quantos processos críticos existem, mas quando e em quais momentos eles aumentam. 
# Isso ajuda a detectar picos de carga e prever possíveis incidentes antes que se 
# tornem falhas maiores. Já o limite de 30% funciona como um indicador de alerta. 
# Ele define um ponto objetivo a partir do qual o sistema passa a ser considerado 
# em risco. Quando esse valor é ultrapassado, indica que uma parte relevante dos 
# processos está em condição crítica, comprometendo a saúde do servidor. 

df_processos <- df_processos %>%
  mutate(
    criticidade = ifelse(
      cpu_percent > 80 |
        ram_percent > 20 |
        latency_ms > 100,
      "critico",
      "estável"
    )
  )

df_criticos <- df_processos %>%
  filter(criticidade == "critico")

criticos_por_horario <- df_criticos %>%
  group_by(timestamp) %>%
  summarise(qtd_criticos = n(), .groups = "drop") %>%
  arrange(timestamp)

plot(
  criticos_por_horario$timestamp,
  criticos_por_horario$qtd_criticos,
  type = "l",
  col = "#e63946",
  lwd = 3,
  xlab = "Hora do dia",
  ylab = "Quantidade de processos críticos",
  main = "Processos Críticos ao Longo das Horas",
  xaxt = "n"
)

axis(1,
     at = dados_plot$timestamp[seq(1, nrow(dados_plot), by = 3)],
     labels = format(
       dados_plot$timestamp[seq(1, nrow(dados_plot), by = 3)],
       "%H:%M"),las = 0)

df_processos_unicos <- df_processos %>%
  distinct(nome, .keep_all = TRUE)

limite <- 0.3 * nrow(df_processos_unicos)

abline(
  h = limite,
  col = "black",
  lwd = 2,
  lty = 2
)

legend(
  "topleft",
  legend = c("Críticos", "30% do Total"),
  col = c("#e63946", "black"),
  lwd = 3,
  lty = c(1, 2),
  bty = "n",
  seg.len = 3
)

# O gráfico mostra um comportamento evidente de instabilidade ao longo do periodo 
# analisado. No início, entre aproximadamente 06:45 e 07:00, a quantidade de 
# processos críticos se mantém baixa e relativamente estável, sempre abaixo do 
# limite de 30%, indicando um cenário controlado, com algumas pequenas oscilações,
# mas sem impacto relevante na saúde do sistema. A partir de cerca de 07:00, 
# ocorre um crescimento abrupto, com os processos críticos ultrapassando o limite 
# definido. Esse pico se mantém por alguns minutos, chegando ao ponto mais alto do 
# período, o que indica uma fase de forte degradação do sistema e possível sobrecarga.
# Logo depois, por volta de 07:09, há uma queda brusca, com os valores retornando 
# para níveis baixos e novamente estáveis, sugerindo que o sistema se recuperou 
# ou que a carga crítica foi reduzida. Através do gráfico é possivel observar se 
# sistema está piorando de forma contínua ou só teve picos isolados, sendo fundamental
# para maior controle da infraestrutura do sagitário.


# (Utilização de CPU, RAM e Latência em processos críticos)

# O monitoramento de CPU, RAM e latência dos processos é essencial para garantir 
# a estabilidade e o desempenho do sistema, pois permite identificar rapidamente 
# sinais de sobrecarga e degradação. A CPU indica o nível de processamento exigido, 
# a RAM mostra o uso de memória e possíveis acúmulos, e a latência revela o tempo 
# de resposta do processo, sendo um dos principais indicadores de impacto na 
# experiência do sistema. Ao acompanhar essas três métricas em conjunto, é 
# possível detectar padrões de falha, antecipar incidentes e entender a causa 
# raiz de comportamentos críticos, tornando a gestão da infraestrutura mais 
# eficiente e preventiva.

df_processos %>%
  filter(criticidade == "critico") %>%
  count(nome, sort = TRUE)

df_radar <- df_processos %>%
  filter(nome == "radar_data_ingestion") %>%
  arrange(timestamp)

# normalização dos valores (CPU, RAM e latência ficam na mesma escala)
# quantos desvios padrão esse valor está acima ou abaixo da média
df_radar$cpu_n <- scale(df_radar$cpu_percent)
df_radar$ram_n <- scale(df_radar$ram_percent)
df_radar$lat_n <- scale(df_radar$latency_ms)

plot(
  df_radar$timestamp,
  df_radar$cpu_n,
  type = "l",
  col = "#e63946",
  lwd = 3,
  ylim = range(c(df_radar$cpu_n, df_radar$ram_n, df_radar$lat_n)),
  xlab = "Hora do Dia",
  ylab = "Variação normalizada",
  main = "Variação de CPU, RAM e Latência - Ingestão de Dados de Radar",
  xaxt = "n"
)

axis(
  1,
  at = df_radar$timestamp[seq(1, nrow(df_radar), by = 5)],
  labels = format(df_radar$timestamp[seq(1, nrow(df_radar), by = 5)], "%H:%M")
)

lines(df_radar$timestamp, df_radar$ram_n, col = "#2196f3", lwd = 3)
lines(df_radar$timestamp, df_radar$lat_n, col = "#2a9d8f", lwd = 3)

legend(
  "topleft",
  legend = c("Latência", "CPU", "RAM"),
  col = c("#2a9d8f", "#e63946", "#2196f3"),
  lwd = 3,
  bty = "n"
)

# O monitoramento de CPU, RAM e latência dos processos é essencial para garantir 
# a estabilidade e o desempenho do sistema, pois permite identificar rapidamente 
# sinais de sobrecarga e degradação. A CPU indica o nível de processamento exigido, 
# a RAM mostra o uso de memória e possíveis acúmulos, e a latência revela o tempo 
# de resposta do processo, sendo um dos principais indicadores de impacto na 
# experiência do sistema. Ao acompanhar essas três métricas em conjunto, é 
# possível detectar padrões de falha, antecipar incidentes e entender a causa raiz 
# de comportamentos críticos, tornando a gestão da infraestrutura mais eficiente e preventiva.

