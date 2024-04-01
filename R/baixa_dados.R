library(tidyverse)
library(janitor)
library(readr)
library(microdatasus)

# Lendo data.frames auxiliares (criados em cria_dfs_auxiliares.R)
df_cid10 <- read.csv("R/databases/df_cid10.csv") |>
  clean_names()
df_aux_municipios <- read.csv("R/databases/df_aux_municipios.csv") |>
  clean_names() |>
  mutate_if(is.numeric, as.character)

# Para os óbitos fetais: baixando os dados do SIM-DOFET de 2012 a 2022
df_sim_dofet_aux <- fetch_datasus(
  year_start = 2012,
  year_end = 2022,
  information_system = "SIM-DOFET"
) |>
  clean_names()

## Criando a variável de ano, limitando a variável 'causabas' a três caracteres e filtrando apenas pelos óbitos fetais que consideramos
df_fetais <- df_sim_dofet_aux |>
  mutate(
    ano = as.numeric(substr(dtobito, nchar(dtobito) - 3, nchar(dtobito))),
    causabas = substr(causabas, 1, 3),
    semagestac = as.numeric(semagestac),
    peso = as.numeric(peso)
  ) |>
  filter(
    ((gestacao != "1" & !is.na(gestacao) & gestacao != "9") | (semagestac >= 22 & semagestac != 99)) | (peso >= 500)
  ) |>
  mutate(
    tipo_do_obito = "Fetais",
    faixa_de_peso = case_when(
      is.na(peso) ~ "Sem informação",
      peso < 1500 ~ "< 1500 g",
      peso >= 1500 & peso < 2000 ~ "1500 a 1999 g",
      peso >= 2000 & peso < 2500 ~ "2000 a 2499 g",
      peso >= 2500 ~ "\U2265 2500 g"
    ),
    obitos = 1
  ) |>
  left_join(df_aux_municipios) |>
  left_join(df_cid10) |>
  select(codigo = codmunres, ano, municipio, uf, regiao, tipo_do_obito, capitulo_cid10, grupo_cid10, causabas_categoria, faixa_de_peso, obitos) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup()

##Exportando os dados 
write.table(df_fetais, gzfile('R/databases/dados_obitos_fetais_2012_2022.csv.gz'), sep = ",", dec = ".", row.names = FALSE, fileEncoding = "utf-8")


# Para os óbitos neonatais: baixando os dados do SIM de 2012 a 2022
df_sim_aux <- fetch_datasus(
  year_start = 2012,
  year_end = 2022,
  information_system = "SIM-DO"
) |>
  clean_names()

## Criando a variável de ano, limitando a variável 'causabas' a três caracteres, filtrando pelos óbitos neonatais e pós-neonatais e criando algumas variáveis
df_neonatais <- df_sim_aux |>
  mutate(
    ano = as.numeric(substr(dtobito, nchar(dtobito) - 3, nchar(dtobito))),
    causabas = substr(causabas, 1, 3),
    peso = as.numeric(peso),
    idade = as.numeric(idade)
  ) |>
  filter(
    idade <= 400
  ) |>
  mutate(
    tipo_do_obito = case_when(
      idade < 207 ~ "Neonatais precoces",
      idade >= 207 & idade < 228 ~ "Neonatais tardios",
      idade >= 228 ~ "Pós-neonatais"
    ),
    faixa_de_peso = case_when(
      is.na(peso) ~ "Sem informação",
      peso < 1500 ~ "< 1500 g",
      peso >= 1500 & peso < 2000 ~ "1500 a 1999 g",
      peso >= 2000 & peso < 2500 ~ "2000 a 2499 g",
      peso >= 2500 ~ "\U2265 2500 g"
    ),
    obitos = 1
  ) |>
  left_join(df_aux_municipios) |>
  left_join(df_cid10) |>
  select(codigo = codmunres, ano, municipio, uf, regiao, tipo_do_obito, capitulo_cid10, grupo_cid10, causabas_categoria, faixa_de_peso, obitos) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup()

## Exportando os dados 
write.table(df_neonatais, gzfile('R/databases/dados_obitos_neonatais_2012_2022.csv.gz'), sep = ",", dec = ".", row.names = FALSE, fileEncoding = "utf-8")

# #Juntando as bases de óbitos fetais e neonatais
# df_obitos_fetais_neonatais <- rbind(df_obitos_fetais, df_obitos_neonatais)
# 
# ##Exportando os dados 
# write.table(df_obitos_fetais_neonatais, 'R/databases/dados_obitos_fetais_neonatais.csv', sep = ",", dec = ".", row.names = FALSE, fileEncoding = "utf-8")














