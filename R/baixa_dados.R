library(tidyverse)
library(janitor)
library(readr)
library(microdatasus)


# Lendo data.frames auxiliares (criados em cria_dfs_auxiliares.R) ---------
df_cid10 <- read.csv("R/databases/df_cid10.csv") |>
  clean_names()

df_aux_municipios <- read.csv("R/databases/df_aux_municipios.csv") |>
  clean_names() |>
  mutate_if(is.numeric, as.character)


# Para os óbitos fetais ---------------------------------------------------
## Baixando os dados do SIM-DOFET de 2012 a 2022
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
    capitulo_cid10, grupo_cid10, grupo_principais, causabas_categoria, faixa_de_peso,
    racacor, obitos
  ) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup()

## Exportando os dados 
write.table(df_fetais, gzfile('R/databases/dados_obitos_fetais_2012_2022.csv.gz'), sep = ",", dec = ".", row.names = FALSE, fileEncoding = "utf-8")


# Para os óbitos neonatais ------------------------------------------------
## Baixando os dados do SIM-DOINF de 2012 a 2022
df_sim_doinf_aux <- fetch_datasus(
  year_start = 2012,
  year_end = 2022,
  information_system = "SIM-DOINF"
) |>
  clean_names()

## Criando a variável de ano, limitando a variável 'causabas' a três caracteres, filtrando pelos óbitos neonatais e pós-neonatais e criando algumas variáveis
df_neonatais <- df_sim_doinf_aux |>
  mutate(
    ano = as.numeric(substr(dtobito, nchar(dtobito) - 3, nchar(dtobito))),
    causabas = substr(causabas, 1, 3),
    peso = as.numeric(peso),
    idade = as.numeric(idade)
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

## Exportando os dados 
write.table(df_neonatais, gzfile('R/databases/dados_obitos_neonatais_2012_2022.csv.gz'), sep = ",", dec = ".", row.names = FALSE, fileEncoding = "utf-8")
















