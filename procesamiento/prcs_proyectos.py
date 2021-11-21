# -*- coding: utf-8 -*-
"""
Created on Mon Nov  1 16:50:49 2021

@author: u565589
"""

from datetime import datetime
import os
import numpy as np
import pandas as pd
#import dask.dataframe as dd
import json
import pprint


direc_celdas = os.path.dirname(os.getcwd()) + '\\ind_celdas_mensual\\indisponibilidad_global_celda_2109.csv'

data_jnb = pd.read_json(os.path.dirname(os.getcwd()) + '\\sim_bajadas\\NB.json', orient='records', encoding="ISO-8859-1", dtype={'fechaRFASIM': 'string'})

data_jnb['fechaRFASIM'] = pd.to_datetime(data_jnb.fechaRFASIM, format="%Y-%m-%d %H:%M:%S")
data_jnb['fecha'] = data_jnb.fechaRFASIM.dt.strftime("%d/%m/%Y")

df_filtrado = data_jnb[(data_jnb['fechaRFASIM'] >= "2021-09-23")]

print(df_filtrado.to_string())


data_j850 = pd.read_json(os.path.dirname(os.getcwd()) + '\\sim_bajadas\\LTE850.json', orient='records', encoding="ISO-8859-1", dtype={'fechaRFASIM': 'string'})

data_j850['fechaRFASIM'] = pd.to_datetime(data_j850.fechaRFASIM, format="%Y-%m-%d %H:%M:%S")
data_j850['fecha'] = data_j850.fechaRFASIM.dt.strftime("%d/%m/%Y")

df_filt2 = data_j850[(data_j850['fechaRFASIM'] >= "2021-09-23")]

print(df_filt2.to_string())


data_j1900 = pd.read_json(os.path.dirname(os.getcwd()) + '\\sim_bajadas\\LTE1900.json', orient='records', encoding="ISO-8859-1", dtype={'fechaRFASIM': 'string'})

data_j1900['fechaRFASIM'] = pd.to_datetime(data_j1900.fechaRFASIM, format="%Y-%m-%d %H:%M:%S")
data_j1900['fecha'] = data_j1900.fechaRFASIM.dt.strftime("%d/%m/%Y")

df_filt3 = data_j1900[(data_j1900['fechaRFASIM'] >= "2021-09-23")]

print(df_filt3.to_string())

# concat and generate csv and json complete files
framestot = [df_filtrado, df_filt2 , df_filt3]
df_tot = pd.concat(framestot)
print(df_tot.to_string())

jft = open(os.path.dirname(os.getcwd()) + '\\procesamiento\\json_filtrados\\total.json', 'w')
jft.write(df_tot.to_json(orient='records'))
jft.close()

csvft = open(os.path.dirname(os.getcwd()) + '\\procesamiento\\json_filtrados\\total.csv', 'w', newline='')
csvft.write(df_tot.to_csv())
csvft.close()


df_filtrado.pop('NombreSitio')
df_filtrado.pop('Region')
df_filtrado.pop('Vendor')
df_filtrado.pop('EstadoIM')
df_filtrado.pop('FechaEstadoIM')
df_filtrado.pop('fechaRFPSIM')
df_filtrado.pop('fechaRFISIM')
df_filtrado.pop('fechaRFTSIM')
df_filtrado.pop('fechaOASIM')

df_filt2.pop('NombreSitio')
df_filt2.pop('Region')
df_filt2.pop('Vendor')
df_filt2.pop('EstadoIM')
df_filt2.pop('FechaEstadoIM')
df_filt2.pop('fechaRFPSIM')
df_filt2.pop('fechaRFISIM')
df_filt2.pop('fechaRFTSIM')
df_filt2.pop('fechaOASIM')

df_filt3.pop('NombreSitio')
df_filt3.pop('Region')
df_filt3.pop('Vendor')
df_filt3.pop('EstadoIM')
df_filt3.pop('FechaEstadoIM')
df_filt3.pop('fechaRFPSIM')
df_filt3.pop('fechaRFISIM')
df_filt3.pop('fechaRFTSIM')
df_filt3.pop('fechaOASIM')

frames = [df_filtrado, df_filt2 , df_filt3]
df_total = pd.concat(frames)
print(df_total.to_string())

jf = open(os.path.dirname(os.getcwd()) + '\\procesamiento\\json_filtrados\\filtrado.json', 'w')
jf.write(df_total.to_json(orient='records'))
jf.close()




