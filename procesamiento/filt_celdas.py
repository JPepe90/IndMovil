# -*- coding: utf-8 -*-

import os
import numpy as np
import pandas as pd
#import dask.dataframe as dd


direc_celdas = os.path.dirname(os.getcwd()) + '\\ind_celdas_mensual\\indisponibilidad_global_celda_2109.csv'
direc_celdas2 = os.path.dirname(os.getcwd()) + '\\ind_celdas_mensual\\indisponibilidad_global_celda_2110.csv'
direc_fprof = os.path.dirname(os.getcwd()) + '\\procesamiento\\json_filtrados\\filtrado.json'

celdasdtypes = {
    'fecha': 'object', 
    'celda': 'object', 
    'sitio': 'object', 
    'nombre': 'object', 
    'region': 'object', 
    'op': 'object',
    'incidentes': 'object', 
    'tecnologia': 'object', 
    'estado': 'object', 
    'disponibilidad': 'float64', 
    'down_time': 'float64', 
    'medicion': 'float64', 
    'ticket': 'object', 
    'ot': 'object', 
    'dias_indispo': 'float64'}


chunk = pd.read_csv(direc_celdas, encoding="ISO-8859-1", chunksize=1000000, usecols=['fecha', 'celda', 'sitio', 'nombre', 'region', 'op','incidentes', 'tecnologia', 'estado', 'disponibilidad', 'down_time', 'medicion', 'ticket', 'ot', 'dias_indispo'], dtype=celdasdtypes, low_memory=False)
df_totceldas = pd.concat(chunk)
# df_totceldas["col_cruce"] = df_totceldas["sitio"] + df_totceldas["fecha"]

df_totceldas["col_cruce"] = df_totceldas.apply(lambda row: df_totceldas.sitio + df_totceldas.fecha, axis=1)

# df_totceldas = df_totceldas.set_index("col_cruce")

# print(df_totceldas.to_string())

df_proyfilt = pd.read_json(direc_fprof, orient='records', encoding="ISO-8859-1")
df_proyfilt["col_cruce"] = df_proyfilt["codigoSitio"] + df_proyfilt["fecha"]
lista_aux = df_proyfilt["col_cruce"].tolist()
set_aux = set(lista_aux)


df_finalceldas = pd.DataFrame(columns=['col_cruce', 'fecha', 'celda', 'sitio', 'nombre', 'region', 'op','incidentes', 'tecnologia', 'estado', 'disponibilidad', 'down_time', 'medicion', 'ticket', 'ot', 'dias_indispo'])

for i, x in enumerate(set_aux):
    df_celaux = df_totceldas[(df_totceldas["col_cruce"] == x)]
    df_finalceldas = pd.concat([df_finalceldas, df_celaux])
    


# print(df_finalceldas.to_string())


