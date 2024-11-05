library(openxlsx)
library(dplyr)

anio <- 2024
mes <- 10

cba_ant <- read.csv(
  paste0(
    "CBA_",
    anio - (mes == 1),
    "_",
    sprintf("%02d", (mes - 2) %% 12 + 1),
    ".csv"
  )
)

catalogo <- read.csv("productos_y_variedades.csv")

config <- ini::read.ini("config.ini")$prod

con <- odbc::dbConnect(
  odbc::odbc(),
  .connection_string = paste0(
    "Driver={", config$driver, "};",
    "Server=", config$server, ";",
    "Database=", config$database, ";",
    "Uid=", config$user, ";",
    "Pwd=", config$password
  )
)

base <- odbc::dbGetQuery(
  con,
  paste(
    "EXEC [dbo].[sp_get_precios_recolectados_mes]", anio, ",", mes
  )
)

indices <- odbc::dbGetQuery(
  con,
  paste(
    "EXEC [dbo].[sp_get_indice_grupo]", anio, ",", mes
  )
)

# Cerramos la conexión
odbc::dbDisconnect(con)

#safe_base <- base
#saved_ind <- indices
  
indices <- indices[
  (indices$region == 0) &
  (indices$grupo_codigo %in% (catalogo$codigo_articulo %/% 10)),
  c("grupo_codigo", "indice_grupo", "indice_anterior")
] |> within(
  var <- indice_grupo / indice_anterior
) |> within(
  grupo_codigo <- as.integer(grupo_codigo)
)
indices <- indices[ , c("grupo_codigo", "var")]

base <- safe_base

# Quitamos periodos de espera
#base <- base[ base$nt_tipo != "Periodo de espera" , ]
base_filtrado <- base %>% filter(nt_tipo_nombre != "Periodo de espera")

# Quitamos fuentes inactivas
base <- base[ base$estado_fuente == "Activo" , ]

base <- base[
  base$estado_registro == "Validada",
  c(
    "pk",
    "codigo_articulo",
    "cantidad_base",
    "cantidad_actual",
    "precio_actual"
  )
] |> merge(
  catalogo[ , c("codigo_articulo", "codigo_enigh")],
  by = "codigo_articulo"
) |> within(
  precio_base <- precio_actual * cantidad_base / cantidad_actual
) |> within(
  grupo_codigo <- as.integer(
    substr(codigo_articulo, 1, nchar(codigo_articulo) - 1)
  )
)

base <- base[
  !is.na(base$precio_base),
  c("codigo_enigh", "grupo_codigo", "precio_base", "pk")
]

cba_ant <- merge(
  cba_ant,
  catalogo[ , c("codigo_enigh", "codigo_articulo")],
  by = "codigo_enigh"
)
cba_ant$codigo_articulo <- cba_ant$codigo_articulo %/% 10
cba_ant <- unique(cba_ant)
colnames(cba_ant)[colnames(cba_ant) == 'codigo_articulo'] <- 'grupo_codigo'

cba_ant <- merge(
  cba_ant,
  indices,
  by = "grupo_codigo"
) |> within(
  pb_ideal <- precio_med_m * var
)

base$grupo_codigo <- NULL
cba_ant$grupo_codigo <- NULL

medians <- merge(
  base[ , c("codigo_enigh", "precio_base")],
  cba_ant,
  by = "codigo_enigh"
) |> within(
  p_var <- precio_base / precio_med_m
) |> within(
  desviacion <- abs(pb_ideal - precio_base)
)

targets <- medians[
  (medians$var > 1 & medians$p_var >= 1) |
  (medians$var == 1) |
  (medians$var < 1 & medians$p_var <= 1) ,
] |> group_by(codigo_enigh) |> 
  summarize(
    target = min(desviacion),
)

medians <- merge(
  targets,
  medians[ , c("codigo_enigh", "precio_base", "desviacion")],
  by = "codigo_enigh"
)

medians <- medians[
  medians$desviacion == medians$target,
  c("codigo_enigh", "precio_base")
] |> unique()

# Cambio el nombre para no confundirme
colnames(medians) <- c("codigo_enigh", "precio_obj")

medians <- medians[!is.na(medians$codigo_enigh), ]

# Crear una función para balancear los precios
balance_precios <- function(df_base) {
  precio_obj <- df_base$precio_obj[1]  # Obtener el valor único de precio_obj para este grupo
  
  mayores <- df_base[df_base$precio_base > df_base$precio_obj, ]
  mayores <- mayores[order(mayores$precio_base), ]
  
  iguales <- df_base[df_base$precio_base == df_base$precio_obj, ]
  
  menores <- df_base[df_base$precio_base < df_base$precio_obj, ]
  menores <- menores[order(menores$precio_base), ]
  
  # Calcular la cantidad mínima entre los dos grupos
  min_count <- min(nrow(mayores), nrow(menores))
  
  # Quedarse solo con la cantidad mínima en ambos grupos
  mayores <- mayores %>% slice_tail(n = min_count)
  menores <- menores %>% slice_head(n = min_count)
  
  # Combinar los dos grupos
  bind_rows(mayores, iguales, menores)
}

# Aplicar la función a cada grupo de codigo_enigh
base_balanceado <- base %>%
  inner_join(medians, by = "codigo_enigh") %>% # Unir con medians para obtener precio_obj
  group_by(codigo_enigh) %>%
  group_modify(~ balance_precios(.x)) %>% # Aplicar la función por grupo
  ungroup() # Desagrupar al final

# Guardamos las boletas
write.csv(
  base_balanceado[, "pk"],
  paste0(
    "pk_",
    anio,
    "_",
    sprintf("%02d", mes),
    ".csv"
  ),
  row.names = FALSE
)

#Guardamos los valores
#colnames(medians) <- c("codigo_enigh", "precio_med_m")
write.csv(
  medians,
  paste0(
    "CBA_",
    anio,
    "_",
    sprintf("%02d", mes),
    ".csv"
  ),
  row.names = FALSE
)

cba_u <- read.csv("Cantidades_urbana.csv")
cba_r <- read.csv("Cantidades_rural.csv")

colnames(medians) <- c("codigo_enigh", "precio_med_m")

cba_u <- left_join(cba_u, medians, by = "codigo_enigh")
cba_r <- left_join(cba_r, medians, by = "codigo_enigh")

cba_ant <-  cba_ant[ , c("codigo_enigh", "precio_med_m")]
colnames(cba_ant) <- c("codigo_enigh", "precio_anterior")

cba_u <- left_join(cba_u, cba_ant, by = "codigo_enigh")
cba_r <- left_join(cba_r, cba_ant, by = "codigo_enigh")

cba_u$var <- cba_u$precio_med_m / cba_u$precio_anterior * 100 - 100
cba_r$var <- cba_r$precio_med_m / cba_r$precio_anterior * 100 - 100

# Se guardan para enviar y modificar si hace falta

wb <- createWorkbook()

addWorksheet(wb, sheetName = "CBA_u")
addWorksheet(wb, sheetName = "CBA_r")

writeData(wb, sheet = "CBA_u", x = cba_u)
writeData(wb, sheet = "CBA_r", x = cba_r)

CBAfile <- paste0("CBA_", anio, "_", sprintf("%02d", mes), ".xlsx")
  
if (file.exists(CBAfile)) { file.remove(CBAfile) }
saveWorkbook(wb, CBAfile)

# Se lee el archivo modificado si existe

adjusted <- paste0("CBA_", anio, "_", sprintf("%02d", mes), "adj", ".xlsx")

if (file.exists(adjusted)) {
   cba_u <- read.xlsx(adjusted, "CBA_u")
   cba_r <- read.xlsx(adjusted, "CBA_r")
}

cba_u$Costo_diarioxpersona <- cba_u$precio_med_m * cba_u$Cantgbxdia / cba_u$Cant_base
cba_r$Costo_diarioxpersona <- cba_r$precio_med_m * cba_r$Cantgbxdia / cba_r$Cant_base

gr_u <- group_by(cba_u, Codigo_Cepal) |> 
  reframe(
    Grupo_alimenticio = first(Grupo_alimenticio),
    Costo = sum(Costo_diarioxpersona),
    Kilocalorias = sum(Kilocalorias_xdia)
  )

gr_r <- group_by(cba_r, Codigo_Cepal) |> 
  reframe(
    Grupo_alimenticio = first(Grupo_alimenticio),
    Costo = sum(Costo_diarioxpersona),
    Kilocalorias = sum(Kilocalorias_xdia)
  )