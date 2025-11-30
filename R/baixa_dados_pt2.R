library(tidyverse)
library(janitor)
library(readr)
library(data.table)
library(glue)

# Criando um objeto com a data de atualização por extenso
## Obtendo o dia, mês e ano por extenso
df_meses <- data.frame(
  num_mes = 1:12,
  nome_mes = c("janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho",
               "agosto", "setembro", "outubro", "novembro", "dezembro")
)

data <- Sys.Date()
data_por_extenso <- glue(
  "{substr(data, 9, 10)} de {df_meses$nome_mes[which(df_meses$num_mes == as.numeric(substr(data, start = 6, stop = 7)))]} de {substr(data, 1, 4)}"
)

saveRDS(data_por_extenso, "R/data_por_extenso.RDS")

# Lendo data.frames auxiliares (criados em cria_dfs_auxiliares.R) ---------
df_cid10 <- read.csv("R/databases/df_cid10.csv") |>
  clean_names()

df_aux_municipios <- read.csv("R/databases/df_aux_municipios.csv") |>
  clean_names() |>
  mutate_if(is.numeric, as.character)


# Baixando os dados preliminares do SIM -----------------------------------
download.file("https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIM/DO24OPEN.csv", "R/databases/DO24OPEN.csv", mode = "wb")
download.file("https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIM/csv/DO25OPEN_csv.zip", "R/databases/DO25OPEN_csv.zip", mode = "wb")

## Lendo os dados preliminares e excluindo os arquivos baixados
df_sim_preliminares_24 <- fread("R/databases/DO24OPEN.csv", sep = ";") |> 
  clean_names()
file.remove("R/databases/DO24OPEN.csv")

df_sim_preliminares_25 <- fread("R/databases/DO25OPEN_csv.zip", sep = ";") |> 
  clean_names()
file.remove("R/databases/DO25OPEN_csv.zip")

## Verificando se os nomes das colunas são todos os mesmos
all(names(df_sim_preliminares_24) == names(df_sim_preliminares_25))

## Juntando as bases preliminares
df_sim_preliminares <- full_join(df_sim_preliminares_24, df_sim_preliminares_25)

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
    codmunres = as.character(codmunres),
    racacor = as.character(racacor)
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
    racacor = case_when(
      racacor == "1" ~ "Branca",
      racacor == "2" ~ "Preta",
      racacor == "3" ~ "Amarela",
      racacor == "4" ~ "Parda",
      racacor == "5" ~ "Indígena",
      is.na(racacor) | racacor == "9" ~ "Ignorado"
    ),
    grupo_principais = case_when(
      causabas >= "P00" & causabas <= "P04" ~ "principais_p00_p04",
      causabas >= "P05" & causabas <= "P08" ~ "principais_p05_p08",
      causabas >= "P10" & causabas <= "P15" ~ "principais_p10_p15",
      causabas >= "P20" & causabas <= "P29" ~ "principais_p20_p29",
      causabas >= "P35" & causabas <= "P39" ~ "principais_p35_p39",
      causabas >= "P50" & causabas <= "P61" ~ "principais_p50_p61",
      causabas >= "P70" & causabas <= "P74" ~ "principais_p70_p74",
      causabas >= "P75" & causabas <= "P78" ~ "principais_p75_p78",
      causabas >= "P80" & causabas <= "P83" ~ "principais_p80_p83",
      causabas >= "P90" & causabas <= "P96" ~ "principais_p90_p96",
      causabas >= "Q00" & causabas <= "Q99" ~ "principais_q00_q99",
      causabas >= "J00" & causabas <= "J99" ~ "principais_j00_j99",
      causabas >= "A00" & causabas <= "A99" |
        causabas >= "B00" & causabas <= "B99" ~ "principais_a00_b99"
    ),
    grupo_principais = ifelse(is.na(grupo_principais), "principais_outros", grupo_principais),
    obitos = 1
  ) |>
  left_join(df_aux_municipios) |>
  left_join(df_cid10) |>
  select(
    codigo = codmunres, ano, municipio, uf, regiao, tipo_do_obito, causabas,
    grupo_principais, capitulo_cid10, grupo_cid10, causabas_categoria, faixa_de_peso,
    racacor, obitos
  ) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup()

sum(df_fetais_preliminares$obitos)

## Juntando os dados preliminares com os dados de 2012 a 2023
df_fetais_2012_2023 <- read.csv(gzfile("R/databases/dados_obitos_fetais_2012_2023.csv.gz")) |>
  mutate(codigo = as.character(codigo))

if (nrow(df_fetais_preliminares) == 0) {
  df_fetais_completo <- df_fetais_2012_2023
} else {
  df_fetais_completo <- full_join(df_fetais_2012_2023, df_fetais_preliminares) |>
    arrange(codigo, ano)
}

## Exportando os dados 
write.table(df_fetais_completo, gzfile('dados_oobr_obitos_fetais_2012_2025.csv.gz'), sep = ",", dec = ".", row.names = FALSE, fileEncoding = "utf-8")


# Para os óbitos neonatais ------------------------------------------------
## Criando a variável de ano, limitando a variável 'causabas' a três caracteres, filtrando pelos óbitos neonatais e pós-neonatais e criando algumas variáveis
df_neonatais_preliminares <- df_sim_preliminares |>
  mutate(
    ano = as.numeric(substr(dtobito, nchar(dtobito) - 3, nchar(dtobito))),
    codmunres = as.character(codmunres),
    causabas = substr(causabas, 1, 3),
    peso = as.numeric(peso),
    idade = as.numeric(idade),
    racacor = as.character(racacor)
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
    racacor = case_when(
      racacor == "1" ~ "Branca",
      racacor == "2" ~ "Preta",
      racacor == "3" ~ "Amarela",
      racacor == "4" ~ "Parda",
      racacor == "5" ~ "Indígena",
      is.na(racacor) | racacor == "9" ~ "Ignorado"
    ),
    grupo_principais = case_when(
      causabas >= "P00" & causabas <= "P04" ~ "principais_p00_p04",
      causabas >= "P05" & causabas <= "P08" ~ "principais_p05_p08",
      causabas >= "P10" & causabas <= "P15" ~ "principais_p10_p15",
      causabas >= "P20" & causabas <= "P29" ~ "principais_p20_p29",
      causabas >= "P35" & causabas <= "P39" ~ "principais_p35_p39",
      causabas >= "P50" & causabas <= "P61" ~ "principais_p50_p61",
      causabas >= "P70" & causabas <= "P74" ~ "principais_p70_p74",
      causabas >= "P75" & causabas <= "P78" ~ "principais_p75_p78",
      causabas >= "P80" & causabas <= "P83" ~ "principais_p80_p83",
      causabas >= "P90" & causabas <= "P96" ~ "principais_p90_p96",
      causabas >= "Q00" & causabas <= "Q99" ~ "principais_q00_q99",
      causabas >= "J00" & causabas <= "J99" ~ "principais_j00_j99",
      causabas >= "A00" & causabas <= "A99" |
        causabas >= "B00" & causabas <= "B99" ~ "principais_a00_b99"
    ),
    grupo_principais = ifelse(is.na(grupo_principais), "principais_outros", grupo_principais),
    obitos = 1
  ) |>
  left_join(df_aux_municipios) |>
  left_join(df_cid10) |>
  select(
    codigo = codmunres, ano, municipio, uf, regiao, tipo_do_obito, causabas,
    grupo_principais, capitulo_cid10, grupo_cid10, causabas_categoria, faixa_de_peso,
    racacor, obitos
  ) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup()

## Juntando os dados preliminares com os dados de 2012 a 2023
df_neonatais_2012_2023 <- read.csv(gzfile("R/databases/dados_obitos_neonatais_2012_2023.csv.gz")) |>
  mutate(codigo = as.character(codigo))

df_neonatais_completo <- full_join(df_neonatais_2012_2023, df_neonatais_preliminares) |>
  arrange(codigo, ano)

## Exportando os dados 
write.table(df_neonatais_completo, gzfile('dados_oobr_obitos_neonatais_2012_2025.csv.gz'), sep = ",", dec = ".", row.names = FALSE, fileEncoding = "utf-8")










