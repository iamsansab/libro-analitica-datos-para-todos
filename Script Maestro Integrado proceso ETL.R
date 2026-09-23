#============================================================
# SCRIPT MAESTRO COMPLETO DE ETL INTEGRADO: DESDE LA API HASTA INTELIGENCIA DE NEGOCIOS
# Autores: Mag. Luz Amparo Mejía Castellanos; Mag. Olga Ines Ceballos Rincón; Ing. Santiago Sabogal Correa
# Dataset: SECOP II - Contratos Electrónicos (Colombia)
# Dataset ID: jbjy-vk9h
# Fuente: Plataforma Nacional de Datos Abiertos de Colombia
#============================================================

# PASO 0: CARGA DE LIBRERÍAS DE CONTROL
if(!require("tidyverse")) install.packages("tidyverse")
if(!require("lubridate")) install.packages("lubridate")
if(!require("arrow")) install.packages("arrow") # Para exportación Parquet

#Activacion de las librerias
library(tidyverse)
library(lubridate)
library(arrow)

print(">>> INICIANDO MOTOR ETL EN R <<<")

# ------------------------------------------------------------------------------
# FASE 1: EXTRACCIÓN EFICIENTE
# ------------------------------------------------------------------------------
dataset_id <- "jbjy-vk9h"
url_base   <- paste0("https://datos.gov.co/resource/", dataset_id, ".csv")

# Búsqueda flexible usando lower() y %quind% para ignorar mayúsculas/tildes
query_url <- paste0(
  url_base,
  "?$limit=50000",
  "&$where=lower(departamento) like '%quind%'",
  " AND estado_contrato in('En ejecución', 'terminado', 'Cerrado', 'Modificado', 'Aprobado')"
)


query_url_limpia <- URLencode(query_url)

print("1. Conectando con la API de datos.gov.co y extrayendo datos crudos...")
datos_crudos <- read_csv(query_url_limpia, show_col_types = FALSE)
print(paste("   -> Extraídos exitosamente", nrow(datos_crudos), "registros crudos."))


#-------------------------------------------------------------
# FASE 2: TRANSFORMACIÓN Y DATA WRANGLING (T)
#-------------------------------------------------------------
print("2. Ejecutando pipeline de limpieza, tipificación y enriquecimiento...")

secop_limpio <- datos_crudos %>% 
  # A. Selección de campos relevantes
  select(
    nombre_entidad,
    documento_proveedor,
    proveedor_adjudicado,
    departamento,
    ciudad,
    tipo_de_contrato,
    modalidad_de_contratacion,
    estado_contrato,
    valor_del_contrato,
    valor_facturado,
    valor_pagado,
    valor_pendiente_de_pago,
    fecha_de_firma
  ) %>% 
  # B. Depuración de registros inconsistentes
  filter(valor_del_contrato > 0) %>% 
  # C. Estandarización de cadenas de texto
  mutate(
    proveedor_adjudicado = str_to_upper(str_squish(proveedor_adjudicado)),
    nombre_entidad = str_to_upper(str_squish(nombre_entidad)),
    ciudad = str_to_upper(str_squish(ciudad))
  ) %>% 
  # D. Modelado de variables financieras e indicadores de riesgo
  mutate(
    # 1. Tratar valores nulos en columnas monetarias
    valor_facturado         = ifelse(is.na(valor_facturado), 0, valor_facturado),
    valor_pagado            = ifelse(is.na(valor_pagado), 0, valor_pagado),
    valor_pendiente_de_pago = ifelse(is.na(valor_pendiente_de_pago), 0, valor_pendiente_de_pago),
    
    # 2. Calcular Sobrecosto / Desviación Monetaria (Facturado vs Contratado)
    desviacion_monetaria    = valor_facturado - valor_del_contrato,
    porcentaje_desviacion   = round((desviacion_monetaria / valor_del_contrato) * 100, 2),
    
    # 3. Índice de Ejecución Real
    porcentaje_ejecucion    = round((valor_pagado / valor_del_contrato) * 100, 2),
    
    # 4. Flag de Alerta Gerencial
    alerta_sobreejecucion   = ifelse(desviacion_monetaria > 0, "SOBRECOSTO / ADICIÓN", "NORMAL")
  ) %>% 
  # E. Inteligencia temporal
  mutate(
    # 1. Convertir a fecha nativa de R de forma segura
    fecha_firma_limpia = as_datetime(fecha_de_firma),
    
    # 2. Extracción de componentes temporales
    anio_firma         = year(fecha_firma_limpia),
    mes_firma          = month(fecha_firma_limpia, label = TRUE, abbr = TRUE)
  )

print(paste("   -> Registros transformados válidos:", nrow(secop_limpio)))

#-------------------------------------------------------------
# FASE 3: CARGA Y OPTIMIZACIÓN DE MODELO DIMENSIONAL (L)
#-------------------------------------------------------------
print("3. Generando tablas del Modelo Dimensional y exportando archivos...")

# Creación de directorio de salida local
if(!dir.exists("data_procesada")) dir.create("data_procesada")

# A. Generación de Tablas de Dimensiones
dim_proveedor <- secop_limpio %>% 
  select(documento_proveedor, proveedor_adjudicado) %>% 
  distinct(documento_proveedor, .keep_all = TRUE)

dim_entidad <- secop_limpio %>% 
  select(nombre_entidad, ciudad, departamento) %>% 
  distinct(nombre_entidad, .keep_all = TRUE)

# B. Generación de Tabla de Hechos Transaccional
fact_contratacion <- secop_limpio %>% 
  select(
    nombre_entidad,
    documento_proveedor,
    ciudad,
    tipo_de_contrato,
    modalidad_de_contratacion,
    estado_contrato,
    valor_del_contrato,
    valor_facturado,
    valor_pagado,
    valor_pendiente_de_pago,
    desviacion_monetaria,
    porcentaje_desviacion,
    porcentaje_ejecucion,
    alerta_sobreejecucion,
    fecha_firma_limpia,
    anio_firma,
    mes_firma
  )

# C. Exportación en Múltiples Formatos Optimizado
# Exportación CSV Universal
write_csv(fact_contratacion, "data_procesada/fact_contratacion.csv")
write_csv(dim_proveedor, "data_procesada/dim_proveedor.csv")
write_csv(dim_entidad, "data_procesada/dim_entidad.csv")

# Exportación Parquet (Formato Columnar de Alto Rendimiento)
write_parquet(fact_contratacion, "data_procesada/fact_contratacion.parquet")

# Exportación RDS (Formato Nativo de R)
saveRDS(secop_limpio, "data_procesada/secop_quindio_completo.rds")

print(">>> PROCESO ETL FINALIZADO CON ÉXITO <<<")
print("Archivos disponibles en la carpeta 'data_procesada/' listos para importar en Power BI.")
