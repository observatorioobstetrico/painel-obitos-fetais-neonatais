library(dplyr)
library(tidyverse)
library(getPass)
library(httr)

token = getPass()  #Token de acesso ? API da PCDaS

url_base = "https://bigdata-api.fiocruz.br"

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

endpoint <- paste0(url_base,"/","sql_query")

# Obtendo um dataframe com o nome dos cap?tulos e categorias da CID10 -----
df_capitulos <- read.csv2("R/databases/CID-10-CAPITULOS.CSV") |>
  mutate(
    DESCRICAO = str_sub(DESCRICAO, nchar("Cap?tulo "), nchar(DESCRICAO)),
    across(
      everything(),
      ~ str_replace_all(., "\\[", "(") |>
        str_replace_all("\\]", ")") |>
        str_replace_all("ü", "u"))
  ) 


df_categorias <- read.csv2("R/databases/CID-10-CATEGORIAS.CSV") |>
  mutate(
    across(
      everything(),
      ~ str_replace_all(., "\\[", "(") |>
        str_replace_all("\\]", ")") |>
        str_replace_all("ü", "u"))
  ) 

df_subcategorias <- read.csv2("R/databases/CID-10-SUBCATEGORIAS.CSV") |>
  mutate(
    CAUSABAS = str_sub(SUBCAT, 1, 3),
    causabas_subcategoria = paste(SUBCAT, DESCRICAO),
    across(
      everything(),
      ~ str_replace_all(., "\\[", "(") |>
        str_replace_all("\\]", ")") |>
        str_replace_all("ü", "u"))
  )

df_grupos <- read.csv2("R/databases/CID-10-GRUPOS.CSV") |>
  mutate(
    across(
      everything(),
      ~ str_replace_all(., "\\[", "(") |>
        str_replace_all("\\]", ")") |>
        str_replace_all("ü", "u"))
  )

df_cid10 <- data.frame(
  CAUSABAS = character(nrow(df_categorias)),
  capitulo_cid10 = character(nrow(df_categorias)),
  grupo_cid10 = character(nrow(df_categorias)),
  causabas_categoria = character(nrow(df_categorias))
)

for (i in 1:nrow(df_categorias)) {
  j <- 1
  k <- 1
  parar <- FALSE
  while (!parar) {
    if (df_categorias$CAT[i] >= df_capitulos$CATINIC[j] & df_categorias$CAT[i] <= df_capitulos$CATFIM[j]) {
      if (df_categorias$CAT[i] >= df_grupos$CATINIC[k] & df_categorias$CAT[i] <= df_grupos$CATFIM[k]) {
        df_cid10$CAUSABAS[i] <- df_categorias$CAT[i]
        df_cid10$capitulo_cid10[i] <- df_capitulos$DESCRICAO[j]
        df_cid10$grupo_cid10[i] <- glue::glue("({df_grupos$CATINIC[k]}-{df_grupos$CATFIM[k]}) {df_grupos$DESCRICAO[k]}")
        df_cid10$causabas_categoria[i] <- paste(df_categorias$CAT[i], df_categorias$DESCRICAO[i])
        parar <- TRUE
      } else {
        k <- k + 1
      }
    } else {
      j <- j + 1
    }
  }
}

df_cid10_completo <- right_join(df_cid10, df_subcategorias |> select(!c(DESCRICAO))) |>
  janitor::clean_names() |>
  select(!causabas) |>
  select(causabas = subcat, capitulo_cid10, grupo_cid10, causabas_categoria, causabas_subcategoria)

## Exportando o arquivo
write.csv(df_cid10_completo, "R/databases/df_cid10_completo.csv", row.names = FALSE)

## Exportando o arquivo
write.csv(df_cid10, "R/databases/df_cid10.csv", row.names = FALSE)

# Obtendo um dataframe com as regi?es, UFs e nomes de cada munic?pio -----
df_aux_municipios <- data.frame()
params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT res_codigo_adotado, res_MUNNOME, res_SIGLA_UF, res_REGIAO, COUNT(1)',
                ' FROM \\"datasus-sinasc\\"',
                ' GROUP BY res_codigo_adotado, res_MUNNOME, res_SIGLA_UF, res_REGIAO",
                        "fetch_size": 65000}
      }
    }')

request <- POST(url = endpoint, body = params, encode = "form")
df_aux_municipios <- convertRequestToDF(request)
names(df_aux_municipios) <- c("CODMUNRES", "municipio", "uf", "regiao", "nascidos")

df_aux_municipios <- df_aux_municipios |>
  select(!nascidos) |>
  arrange(CODMUNRES)

## Exportando o arquivo
write.csv(df_aux_municipios, "R/databases/df_aux_municipios.csv", row.names = FALSE)
