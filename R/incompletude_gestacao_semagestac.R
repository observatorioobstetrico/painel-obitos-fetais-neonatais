library(tidyverse)
library(httr)
library(janitor)
library(getPass)
library(repr)
library(data.table)
library(readr)
library(microdatasus)
library(xlsx)

token = getPass()  #Token de acesso à API da PCDaS (todos os arquivos gerados se encontram na pasta "Databases", no Google Drive)

url_base = "https://bigdata-api.fiocruz.br"
endpoint <- paste0(url_base,"/","sql_query")

convertRequestToDF <- function(request){
  variables = unlist(content(request)$columns)
  variables = variables[names(variables) == "name"]
  column_names <- unname(variables)
  values = content(request)$rows
  df <- as.data.frame(do.call(rbind,lapply(values,function(r) {
    row <- r
    row[sapply(row, is.null)] <- NA
    rbind(unlist(row))
  } )))
  names(df) <- column_names
  return(df)
}


# Para os óbitos fetais ---------------------------------------------------
##Baixando os dados do SIM-DOFET (óbitos fetais) para os anos de 2012 a 2021
df_fetais_aux <- fetch_datasus(
  year_start = 2012,
  year_end = 2021,
  information_system = "SIM-DOFET",
  vars = c("CODMUNRES", "DTOBITO", "GESTACAO", "SEMAGESTAC", "PESO")
)
rownames(df_fetais_aux) <- 1:nrow(df_fetais_aux)

##Checando o que seriam valores faltantes para cada variável
unique(df_fetais_aux$GESTACAO)  #Valores inválidos: NA e 9
unique(df_fetais_aux$SEMAGESTAC)  #Valores inválidos: NA e 99 

##Filtrando pelos óbitos fetais e criando as variáveis de incompletude (cada linha é uma observação)
df_fetais <- df_fetais_aux |> 
  clean_names() |>
  mutate_at(c("gestacao", "semagestac", "peso"), as.numeric) |>
  mutate(
    ano = as.numeric(substr(dtobito, nchar(dtobito) - 3, nchar(dtobito))),
    .keep = "unused",
    .after = "codmunres"
  ) |>
  filter(((gestacao > 1 & gestacao != 9) | (semagestac >= 22 & semagestac != 99)) | (peso >= 500)) |>
  mutate(
    gestacao_incompletos = ifelse(is.na(gestacao) | gestacao == 9, 1, 0),
    gestacao_validos = ifelse(!is.na(gestacao) & gestacao != 9, 1, 0),
    gestacao_total = 1,
    semagestac_incompletos = ifelse(is.na(semagestac) | semagestac == 99, 1, 0),
    semagestac_validos = ifelse(!is.na(semagestac) & semagestac != 99, 1, 0),
    semagestac_total = 1,
    ambas_incompletas = ifelse((is.na(semagestac) | semagestac == 99) & (is.na(gestacao) | gestacao == 9), 1, 0),
    ambas_total = 1,
    ocorrencias_discordantes = ifelse(
      is.na(gestacao) | gestacao == 9 | is.na(semagestac) | semagestac == 99,
      0,
      ifelse(
        (gestacao == 1 & semagestac >= 22) |
          (gestacao == 2 & (semagestac < 22 & semagestac > 27)) |
          (gestacao == 3 & (semagestac < 28 & semagestac > 31)) |
          (gestacao == 4 & (semagestac < 32 & semagestac > 36)) |
          (gestacao == 5 & (semagestac < 37 & semagestac > 41)) |
          (gestacao == 6 & semagestac < 42),
        1,
        0
      )
    ),
    ocorrencias_total = 1
  ) |>
  group_by(ano) |>
  summarise(across(gestacao_incompletos:ocorrencias_total, ~ sum(.x))) |>
  mutate(
    porc_gestacao_incompletos = round(gestacao_incompletos / gestacao_total * 100, 2),
    .after = gestacao_total
  ) |>
  mutate(
    porc_semagestac_incompletos = round(semagestac_incompletos / semagestac_total * 100, 2),
    .after = semagestac_total
  ) |>
  mutate(
    porc_discordantes = round(ocorrencias_discordantes / ocorrencias_total * 100, 2),
    .after = ocorrencias_total
  ) |>
  mutate(
    porc_ambas_incompletas = round(ambas_incompletas / ambas_total * 100, 2),
    .after = ambas_total
  ) |>
  ungroup()

sum(df_fetais$gestacao_incompletos) + sum(df_fetais$gestacao_validos) == sum(df_fetais$gestacao_total)
sum(df_fetais$semagestac_incompletos) + sum(df_fetais$semagestac_validos) == sum(df_fetais$semagestac_total)

##Criando um data.frame com as observações discordantes para os óbitos fetais
df_discordantes_fetais <- df_fetais_aux |> 
  clean_names() |>
  mutate_at(c("gestacao", "semagestac", "peso"), as.numeric) |>
  mutate(
    ano = as.numeric(substr(dtobito, nchar(dtobito) - 3, nchar(dtobito))),
    .keep = "unused",
    .after = "codmunres"
  ) |>
  filter(((gestacao < 5) | (semagestac >= 22 & semagestac != 99)) | (peso >= 500)) |>
  mutate(
    ocorrencias_discordantes = ifelse(
      is.na(gestacao) | gestacao == 9 | is.na(semagestac) | semagestac == 99,
      0,
      ifelse(
        (gestacao == 1 & semagestac >= 22) |
          (gestacao == 2 & (semagestac < 22 & semagestac > 27)) |
          (gestacao == 3 & (semagestac < 28 & semagestac > 31)) |
          (gestacao == 4 & (semagestac < 32 & semagestac > 36)) |
          (gestacao == 5 & (semagestac < 37 & semagestac > 41)) |
          (gestacao == 6 & semagestac < 42),
        1,
        0
      )
    ),
    ocorrencias_total = 1
  ) |>
  filter(ocorrencias_discordantes == 1)

##Salvando o arquivo 
write.xlsx(
  as.data.frame(df_fetais |> select(!matches("validos"))),
  "R/databases/incompletude_gestacao_semagestac.xlsx",
  sheetName = "Óbitos fetais",
  row.names = FALSE
)


# Para os óbitos neonatais ------------------------------------------------
##Baixando os óbitos neonatais dos anos de 2012 a 2021
df_neonatais_aux <- data.frame()

params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT ano_obito, IDADE, GESTACAO, SEMAGESTAC, COUNT(1)',
                ' FROM \\"datasus-sim\\"',
                ' WHERE (ano_obito >= 2012 AND (idade_obito_mins <= 59 OR idade_obito_horas <= 23 OR idade_obito_dias <= 29 OR idade_obito_meses <= 11 OR idade_obito_anos <= 0))',
                ' GROUP BY ano_obito, IDADE, GESTACAO, SEMAGESTAC",
                        "fetch_size": 65000}
      }
    }')

request <- POST(url = endpoint, body = params, encode = "form")
df_neonatais_aux <- convertRequestToDF(request)
names(df_neonatais_aux) <- c("ano", "idade", "gestacao", "semagestac", "obitos")

head(df_neonatais_aux)

##Checando o que seriam valores faltantes para cada variável
unique(df_neonatais_aux$gestacao)  #Valor inválido: 9
unique(df_neonatais_aux$semagestac)  #Valores inválidos: NA e 99

##Filtrando pelos óbitos neonatais e criando as variáveis de incompletude (cada linha pode representar mais de um óbito)
df_neonatais <- df_neonatais_aux |>
  mutate_at(c("ano", "gestacao", "semagestac", "idade", "obitos"), as.numeric) |>
  filter(idade < 228) |>
  mutate(
    gestacao_incompletos = ifelse(gestacao == 9, obitos, 0),
    gestacao_validos = ifelse(gestacao != 9, obitos, 0),
    gestacao_total = obitos,
    semagestac_incompletos = ifelse(is.na(semagestac) | semagestac == 99, obitos, 0),
    semagestac_validos = ifelse(!is.na(semagestac) & semagestac != 99, obitos, 0),
    semagestac_total = obitos,
    ambas_incompletas = ifelse((gestacao == 9) & (is.na(semagestac) | semagestac == 99), obitos, 0), 
    ambas_total = obitos,
    ocorrencias_discordantes = ifelse(
      gestacao == 9 | is.na(semagestac) | semagestac == 99,
      0,
      ifelse(
        (gestacao == 1 & semagestac >= 22) |
          (gestacao == 2 & (semagestac < 22 & semagestac > 27)) |
          (gestacao == 3 & (semagestac < 28 & semagestac > 31)) |
          (gestacao == 4 & (semagestac < 32 & semagestac > 36)) |
          (gestacao == 5 & (semagestac < 37 & semagestac > 41)) |
          (gestacao == 6 & semagestac < 42),
        obitos,
        0
      )
    ),
    ocorrencias_total = obitos
  ) |>
  select(!obitos) |>
  group_by(ano) |>
  summarise(across(gestacao_incompletos:ocorrencias_total, ~ sum(.x))) |>
  mutate(
    porc_gestacao_incompletos = round(gestacao_incompletos / gestacao_total * 100, 2),
    .after = gestacao_total
  ) |>
  mutate(
    porc_semagestac_incompletos = round(semagestac_incompletos / semagestac_total * 100, 2),
    .after = semagestac_total
  ) |>
  mutate(
    porc_discordantes = round(ocorrencias_discordantes / ocorrencias_total * 100, 2),
    .after = ocorrencias_total
  ) |>
  mutate(
    porc_ambas_incompletas = round(ambas_incompletas / ambas_total * 100, 2),
    .after = ambas_total
  ) |>
  ungroup()

sum(df_neonatais$gestacao_incompletos) + sum(df_neonatais$gestacao_validos) == sum(df_neonatais$gestacao_total)
sum(df_neonatais$semagestac_incompletos) + sum(df_neonatais$semagestac_validos) == sum(df_neonatais$semagestac_total)

##Criando um data.frame com as observações discordantes para os óbitos neonatais
df_discordantes_neonatais <- df_neonatais_aux |>
  mutate_at(c("ano", "gestacao", "semagestac", "idade", "obitos"), as.numeric) |>
  filter(idade < 228) |>
  mutate(
    ocorrencias_discordantes = ifelse(
      gestacao == 9 | is.na(semagestac) | semagestac == 99,
      0,
      ifelse(
        (gestacao == 1 & semagestac >= 22) |
          (gestacao == 2 & (semagestac < 22 & semagestac > 27)) |
          (gestacao == 3 & (semagestac < 28 & semagestac > 31)) |
          (gestacao == 4 & (semagestac < 32 & semagestac > 36)) |
          (gestacao == 5 & (semagestac < 37 & semagestac > 41)) |
          (gestacao == 6 & semagestac < 42),
        obitos,
        0
      )
    ),
    ocorrencias_total = obitos
  ) |>
  select(!obitos) |>
  filter(ocorrencias_discordantes > 0)

##Salvando o arquivo
write.xlsx(
  as.data.frame(df_neonatais |> select(!matches("validos"))),
  "R/databases/incompletude_gestacao_semagestac.xlsx",
  sheetName = "Óbitos neonatais",
  row.names = FALSE,
  append = TRUE
)





