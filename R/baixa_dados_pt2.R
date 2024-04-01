library(tidyverse)
library(janitor)
library(readr)

# Lendo data.frames auxiliares (criados em cria_dfs_auxiliares.R)
df_cid10 <- read.csv("R/databases/df_cid10.csv") |>
  clean_names()
df_aux_municipios <- read.csv("R/databases/df_aux_municipios.csv") |>
  clean_names() |>
  mutate_if(is.numeric, as.character)

# Baixando os dados preliminares do SIM de 2023
download.file("https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIM/Mortalidade_Geral_2023.csv", "R/databases/DO23OPEN.csv", mode = "wb")
write.csv2(read.csv2("R/databases/DO23OPEN.csv"), gzfile("R/databases/DO23OPEN.csv.gz"), row.names = FALSE)
file.remove("R/databases/DO23OPEN.csv")

df_sim_preliminares <- read.csv2(gzfile("R/databases/DO23OPEN.csv.gz")) |>
  clean_names()


# Para os óbitos fetais ---------------------------------------------------
## Criando a variável de ano, limitando a variável 'causabas' a três caracteres e filtrando apenas pelos óbitos fetais que consideramos
df_fetais_preliminares <- df_sim_preliminares |>
  mutate(
    ano = as.numeric(substr(dtobito, nchar(dtobito) - 3, nchar(dtobito))),
    causabas = substr(causabas, 1, 3),
    tipobito = as.character(tipobito),
    gestacao = as.character(gestacao),
    semagestac = as.numeric(semagestac),
    peso = as.numeric(peso),
    codmunres = as.character(codmunres)
  ) |>
  filter(
    tipobito == "1",
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

sum(df_fetais_preliminares$obitos)

## Juntando os dados preliminares com os dados de 2012 a 2021
df_fetais_2022 <- read.csv(gzfile("R/databases/dados_obitos_fetais_2012_2022.csv.gz")) |>
  mutate(codigo = as.character(codigo))

df_fetais_completo <- full_join(df_fetais_2022, df_fetais_preliminares) |>
  arrange(codigo, ano)

##Exportando os dados 
write.table(df_fetais_completo, gzfile('dados_oobr_obitos_fetais_2012_2023.csv.gz'), sep = ",", dec = ".", row.names = FALSE, fileEncoding = "utf-8")


# Para os óbitos neonatais ------------------------------------------------
## Criando a variável de ano, limitando a variável 'causabas' a três caracteres, filtrando pelos óbitos neonatais e pós-neonatais e criando algumas variáveis
df_neonatais_preliminares <- df_sim_preliminares |>
  mutate(
    ano = as.numeric(substr(dtobito, nchar(dtobito) - 3, nchar(dtobito))),
    codmunres = as.character(codmunres),
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

## Juntando os dados preliminares com os dados de 2012 a 2021
df_neonatais_2022 <- read.csv(gzfile("R/databases/dados_obitos_neonatais_2012_2022.csv.gz")) |>
  mutate(codigo = as.character(codigo))

df_neonatais_completo <- full_join(df_neonatais_2022, df_neonatais_preliminares) |>
  arrange(codigo, ano)

##Exportando os dados 
write.table(df_neonatais_completo, gzfile('dados_oobr_obitos_neonatais_2012_2023.csv.gz'), sep = ",", dec = ".", row.names = FALSE, fileEncoding = "utf-8")










