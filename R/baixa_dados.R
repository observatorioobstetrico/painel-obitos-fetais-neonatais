library(tidyverse)
library(httr)
library(janitor)
library(getPass)
library(repr)
library(data.table)
library(readr)
library(microdatasus)

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

#Baixando os dados do SIM-DOFET (óbitos fetais) para os anos de 2012 a 2020
df_fetais_aux1 <- fetch_datasus(
  year_start = 2012,
  year_end = 2021,
  information_system = "SIM-DOFET"
) |>
  process_sim(municipality_data = TRUE) 

df_fetais_aux1$SEMAGESTAC <- as.numeric(df_fetais_aux1$SEMAGESTAC)
df_fetais_aux1$PESO <- as.numeric(df_fetais_aux1$PESO)

#Selecionando as colunas necessárias e criando algumas variáveis
df_fetais_aux2 <- df_fetais_aux1 |> 
  select(
    municipio = munResNome, 
    uf = munResUf,
    codigo = CODMUNRES, 
    DTOBITO,
    CAUSABAS, 
    PESO,
    GESTACAO,
    SEMAGESTAC
  ) |>
  filter(
    (SEMAGESTAC >= 22 | (GESTACAO != "Menos de 22 semanas") & !is.na(GESTACAO)) | (is.na(SEMAGESTAC) & is.na(GESTACAO) & PESO > 500)
  ) |>
  mutate(
    ano = substr(DTOBITO, 1, 4),
    .after = DTOBITO
  ) |>  
  mutate(
    tipo_do_obito = "Fetal",
    faixa_de_peso = case_when(
      is.na(PESO) ~ "Sem informação",
      PESO < 1500 ~ "< 1500 g",
      PESO >= 1500 & PESO < 2000 ~ "1500 a 1999 g",
      PESO >= 2000 & PESO < 2500 ~ "2000 a 2499 g",
      PESO >= 2500 ~ "\U2265 2500 g"
    ),
    obitos = 1,
    .after = PESO
  ) |>
  select(!c(DTOBITO, PESO)) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup()

# sum(df_fetais_aux2$obitos[which(df_fetais_aux2$uf == "São Paulo" & df_fetais_aux2$ano == 2020)])  #Tem que dar 4974
# sum(df_fetais_aux2$obitos[which(df_fetais_aux2$uf == "Espírito Santo" & df_fetais_aux2$ano == 2019)])  #Tem que dar 489
# sum(df_fetais_aux2$obitos[which(df_fetais_aux2$uf == "Espírito Santo" & df_fetais_aux2$municipio == "Alegre" & df_fetais_aux2$ano == 2020)])  #Tem que dar 4
# sum(df_fetais_aux2$obitos[which(df_fetais_aux2$uf == "São Paulo" & df_fetais_aux2$municipio == "Bauru" & df_fetais_aux2$ano == 2020)])  #Tem que dar 44


#Obtendo um dataframe contendo o nome dos grupos, capítulos e categorias da CID10
df1_nomes_cid10 <- data.frame()
params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT CAUSABAS, causabas_grupo, causabas_capitulo, causabas_categoria, COUNT(1)',
                ' FROM \\"datasus-sim\\"',
                ' GROUP BY CAUSABAS, causabas_grupo, causabas_capitulo, causabas_categoria",
                        "fetch_size": 65000}
      }
    }')

request <- POST(url = endpoint, body = params, encode = "form")
df1_nomes_cid10 <- convertRequestToDF(request)
names(df1_nomes_cid10) <- c("CAUSABAS", "causabas_grupo", "capitulo_cid10", "causabas_categoria", "obitos")

df1_nomes_cid10 <- df1_nomes_cid10 |>
  select(!obitos)

df2_nomes_cid10 <- data.frame()
params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT CAUSABAS, causabas_grupo, causabas_capitulo, causabas_categoria, COUNT(1)',
                ' FROM \\"datasus-sim-dofet\\"',
                ' GROUP BY CAUSABAS, causabas_grupo, causabas_capitulo, causabas_categoria",
                        "fetch_size": 65000}
      }
    }')

request <- POST(url = endpoint, body = params, encode = "form")
df2_nomes_cid10 <- convertRequestToDF(request)
names(df2_nomes_cid10) <- c("CAUSABAS", "causabas_grupo", "capitulo_cid10", "causabas_categoria", "obitos")

df2_nomes_cid10 <- df2_nomes_cid10 |>
  select(!obitos)

df_nomes_cid10 <- full_join(df1_nomes_cid10, df2_nomes_cid10)

df_fetais_aux3 <- left_join(df_fetais_aux2, df_nomes_cid10)

#Obtendo um dataframe com as regiões às quais pertencem os estados
df_regioes <- data.frame()
params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT res_REGIAO, CODMUNRES, COUNT(1)',
                ' FROM \\"datasus-sim-dofet\\"',
                ' GROUP BY res_REGIAO, CODMUNRES",
                        "fetch_size": 65000}
      }
    }')

request <- POST(url = endpoint, body = params, encode = "form")
df_regioes <- convertRequestToDF(request)
names(df_regioes) <- c("regiao", "codigo", "obitos")

df_regioes <- df_regioes |>
  select(!obitos)

#Criando a base final de óbitos fetais
df_obitos_fetais <- left_join(df_fetais_aux3, df_regioes) |>
  select(regiao, uf, municipio, codigo, ano, causabas_grupo, capitulo_cid10, causabas_categoria, tipo_do_obito, faixa_de_peso, obitos) |>
  arrange(codigo)

##Exportando os dados 
write.table(df_obitos_fetais, 'R/databases/dados_obitos_fetais.csv', sep = ",", dec = ".", row.names = FALSE, fileEncoding = "utf-8")


#Óbitos neonatais dos anos de 2012 a 2021
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')

df_neonatais_aux <- dataframe <- data.frame()

for (estado in estados){
  
  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, causabas_grupo, causabas_capitulo, causabas_categoria, IDADE, PESO, COUNT(1)',
                  ' FROM \\"datasus-sim\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_obito >= 2012 AND (idade_obito_mins <= 59 OR idade_obito_horas <= 23 OR idade_obito_dias <= 29 OR idade_obito_meses <= 11 OR idade_obito_anos <= 0))',
                  ' GROUP BY res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, causabas_grupo, causabas_capitulo, causabas_categoria, IDADE, PESO",
                        "fetch_size": 65000}
      }
    }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c("regiao", "uf", "municipio", "codigo", "ano", "causabas_grupo", "capitulo_cid10", "causabas_categoria", "idade", "peso", "obitos")
  df_neonatais_aux <- rbind(df_neonatais_aux, dataframe)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":" SELECT res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, causabas_grupo, causabas_capitulo, causabas_categoria, IDADE, PESO, COUNT(1)',
                    ' FROM \\"datasus-sim\\"',
                    ' WHERE WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_obito >= 2012 AND (idade_obito_mins <= 59 OR idade_obito_horas <= 23 OR idade_obito_dias <= 29 OR idade_obito_meses <= 11 OR idade_obito_anos <= 1))',
                    ' GROUP BY res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, causabas_grupo, causabas_capitulo, causabas_categoria, IDADE, PESO",
                            "fetch_size": 65000, "cursor": "',cursor,'"}
                            }
                    }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    request <- POST(url = endpoint, body = params, encode = "form")
    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c("regiao", "uf", "municipio", "codigo", "ano", "causabas_grupo", "capitulo_cid10", "causabas_categoria", "idade", "peso", "obitos")
    df_neonatais_aux <- rbind(df_neonatais_aux, dataframe)
  }
}

head(df_neonatais_aux)

df_neonatais_aux$ano <- as.numeric(df_neonatais_aux$ano)
df_neonatais_aux$peso <- as.numeric(df_neonatais_aux$peso)
df_neonatais_aux$idade <- as.numeric(df_neonatais_aux$idade)
df_neonatais_aux$obitos <- as.numeric(df_neonatais_aux$obitos)

df_obitos_neonatais <- df_neonatais_aux |>
  mutate(
    tipo_do_obito = case_when(
      idade < 207 ~ "Neonatal precoce",
      idade >= 207 & idade < 228 ~ "Neonatal tardio",
      idade >= 228 ~ "Pós-neonatal"
    ),
    faixa_de_peso = case_when(
      is.na(peso) ~ "Sem informação",
      peso < 1500 ~ "< 1500 g",
      peso >= 1500 & peso < 2000 ~ "1500 a 1999 g",
      peso >= 2000 & peso < 2500 ~ "2000 a 2499 g",
      peso >= 2500 ~ "\U2265 2500 g"
    ),
    .after = peso
  ) |>
  select(!c(peso, idade)) 

##Exportando os dados 
write.table(df_obitos_neonatais, 'R/databases/dados_obitos_neonatais.csv', sep = ",", dec = ".", row.names = FALSE, fileEncoding = "utf-8")

# #Juntando as bases de óbitos fetais e neonatais
# df_obitos_fetais_neonatais <- rbind(df_obitos_fetais, df_obitos_neonatais)
# 
# ##Exportando os dados 
# write.table(df_obitos_fetais_neonatais, 'R/databases/dados_obitos_fetais_neonatais.csv', sep = ",", dec = ".", row.names = FALSE, fileEncoding = "utf-8")














