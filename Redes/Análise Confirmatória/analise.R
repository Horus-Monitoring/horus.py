#Análise de Dados - Horus

df <- read.csv("C:/Users/ricar/Documents/SP Tech/CCO/2 Semestre/Projeto/horus_full_2months.csv")

#Tratamento do formado DATETIME
Sys.setlocale("LC_TIME", "C") 
  df$timestamp <- as.POSIXct(  
    df$timestamp,
    format = "%Y-%m-%d  %H:%M:%S"
)
  
df$estimated_revenue_loss_brl <- df$estimated_revenue_loss_brl/10^6

library(ggplot2)
  
hist(df$cpu_percent)
hist(df$memory_available_gb)
hist(df$latency_ms)
ggplot(df, aes(x = timestamp, y = latency_ms)) +
      geom_point(color = "blue", size = 1) +  
      labels(
         title = "Latência ao longo do tempo",
         x = "Data",
         y = "Latência (ms)")

ggplot(df, aes(x = timestamp, y = packet_loss_percent)) +
      geom_point(color = "blue", size = 1) +
      labs(
        title = "Perda de pacotes ao longo do tempo",
        x = "Data",
        y = "Pacotes"
      )

#Matriz de correlação
library(corrplot)
df_cor <- df
df_cor$incident_severity = NULL
df_cor$timestamp = NULL

matriz_cor <- cor(df_cor, method = "pearson")
corrplot(matriz_cor, method = "circle", type = "upper")

#Filtro de matriz
matriz_cor_filtrada <- matriz_cor
matriz_cor_filtrada[abs(matriz_cor_filtrada) < 0.7] <- 0
corrplot(matriz_cor_filtrada, method = "circle", type ="upper")

#A taxa de atualização do ADS-B depende diretamente dos pacotes perdidos 
#e das falhas de conexão
#Com a queda da taxa de atualização, há falhas na transmissão de aeronaves
#e rotas sem atualização

#Regressão Linear utilizando ADS-B com pacotes perdidos e falha de conexão
#Consequências operacionais
lm_adsb_causa <- lm(df$adsb_update_rate ~ 
                      df$packet_loss_percent +
                      df$failed_connections)

summary(lm_adsb_causa)
adsb_causa <- plot_ly(df,
                      x = ~adsb_update_rate,
                      y = ~packet_loss_percent,
                      z = ~failed_connections,
                      type = "scatter3d",
                      mode = "markers",
                      marker = list(
                        color = df$adsb_update_rate,
                        colorscale = "Magma"
                      ))

adsb_causa <- adsb_causa %>%
  layout(
    title = "Atualização do sensor ADS-B com base na Perda de Pacotes e Falhas de Conexão",
    scene = list(
      xaxis = list(title = "Taxa de Atualização do ADS-B (%)"),
      yaxis = list(title = "Pacotes perdidos"),
      zaxis = list(title = "Falhas na conexão")
    )
  )

adsb_causa

#Consequências de negócio
lm_adsb_neg <- lm(df$adsb_update_rate ~ df$estimated_revenue_loss_brl)

summary(lm_adsb_neg)
ggplot(df, aes(x = adsb_update_rate, y = estimated_revenue_loss_brl)) +
  geom_point(color = "blue", size = 1) +
  labs(
    title = "Perdas (milhões/h) devido à Incidentes com ADS-B",
    x = "Taxa de Atualização do ADS-B (%)",
    y = "Perdas estimadas em milhões de reais por hora"
  ) +
  geom_smooth(method = 'lm', color = "darkblue", se = FALSE)

#Regressão linear para evidenciar que problemas de conexão no ADS-B
#levam a perda de rotas e localização de aeronaves
lm_adsb_cons <- lm(df$adsb_update_rate ~ 
                      df$routes_without_update +
                      df$aircraft_broadcast_failures)

summary(lm_adsb_cons)
adsb_cons <- plot_ly(df,
                      x = ~adsb_update_rate,
                      y = ~routes_without_update,
                      z = ~aircraft_broadcast_failures,
                      type = "scatter3d",
                      mode = "markers",
                      marker = list(
                        color = df$aircraft_broadcast_failures,
                        colorscale = "Plasma"
                      ))


adsb_cons <- adsb_cons %>%
  layout(
    title = "Impacto da falta de atualização do sensor ADS-B",
    scene = list(
      xaxis = list(title = "Taxa de Atualização do ADS-B (%)"),
      yaxis = list(title = "Rotas Desatualizadas"),
      zaxis = list(title = "Falhas de Conexão com Aeronaves")
    )
  )

adsb_cons

#O uso de CPU é um indicador importante, visto que é impactado diretamente pelo
#numero de aeronaves conectadas
ggplot(df, aes(x = cpu_percent, y = aircraft_connected)) +
    geom_point(color = "red", size = 1) +
    labels(
      title = "Relação entre uso de CPU e Aeronaves Conectadas",
      x = "Uso de CPU (%)",
      y = "Aeronaves conectadas"
    )

library(plotly)
lat_rotas <- plot_ly(df,
              x = ~routes_without_update,
              y = ~timestamp,
              z = ~latency_ms,
              type = "scatter3d",
              mode = "markers",
              marker = list(
                color = df$routes_without_update,
                colorscale = "Magma"
              )) #Problemas de latência só interferiram
                                # nas rotas no incidente 
                                #entre 23 e 24 de abril e levemente 
                                #entre 18 e 19 de março.

lat_rotas <- lat_rotas %>%
      layout(
        title = "Latência vs Rotas sem Atualização ao Longo do Tempo",
        scene = list(
          xaxis = list(title = "Rotas Desatualizadas"),
          yaxis = list(title = "Data"),
          zaxis = list(title = "Latência")
        )
      )
lat_rotas



