# -*- coding: utf-8 -*-
"""
Created on Mon Nov  1 16:35:57 2021

@author: u565589
"""

from datetime import datetime
import requests
import json
import pprint
import csv


url = "http://api.sim.telecom.com.ar/api/v3/proyecto/ActivacionLTE1900"

payload={}
headers = {
  'Content-Type': 'application/json'
}

response = requests.request("GET", url, headers=headers, data=payload)
listado = json.loads(response.text)

actualyear = datetime.today().year
sitios21 = []

for elemento in listado:
    print("el sitio es {0} la fecha rfa sim es: {1}".format(elemento["codigoSitio"] ,elemento["fechaRFASIM"]))
    if elemento["fechaRFASIM"] is not None and datetime.strptime(elemento["fechaRFASIM"], '%Y-%m-%d %H:%M:%S').date().year == actualyear:
        print("si")
        elemento.pop('ALM')
        elemento.pop('Apellido')
        elemento.pop('CasoSigo')
        elemento.pop('EMG')
        elemento.pop('FechaIntegracionProgramada')
        elemento.pop('ICD')
        elemento.pop('Latitud')
        elemento.pop('Longitud')
        elemento.pop('ManoObra')
        elemento.pop('Nombre')
        elemento.pop('contratista')
        elemento.pop('estadoSigo')
        elemento.pop('fechaOASIGO')
        elemento.pop('fechaRFASIGO')
        elemento.pop('fechaRFISIGO')
        elemento.pop('fechaRFPSIGO')
        elemento.pop('fechaRFTSIGO')
        elemento.pop('intervencionId')
        elemento.pop('nombreCorto')
        elemento.pop('subEstado')
        sitios21.append(elemento)

pprint.pprint(sitios21, indent=2)

jsonString = json.dumps(sitios21)
jfile = open('C:\\Users\\u565589\\Documents\\indmovil\\SpyderProy\\sim_bajadas\\LTE1900.json', 'w')
jfile.write(jsonString)
jfile.close()

# fields = ['ALM','Apellido','CasoSigo','EMG','EstadoIM','FechaEstadoIM','FechaIntegracionProgramada','ICD','Latitud','Longitud','ManoObra','Nombre','NombreProyecto','NombreSitio','Region','Vendor','codigoSitio','contratista','estadoSigo','fechaOASIGO','fechaOASIM','fechaRFASIGO','fechaRFASIM','fechaRFISIGO','fechaRFISIM','fechaRFPSIGO','fechaRFPSIM','fechaRFTSIGO','fechaRFTSIM','intervencionId','nombreCorto','subEstado']

fields = ['codigoSitio', 'NombreProyecto', 'estadoSigo', 'Vendor', 'EstadoIM', 'subEstado', 'FechaEstadoIM', 'fechaRFPSIM', 'fechaRFISIM', 'fechaRFASIM', 'fechaRFTSIM', 'fechaOASIM']

f = open('C:\\Users\\u565589\\Documents\\indmovil\\SpyderProy\\sim_bajadas\\LTE1900.csv', 'w', newline="")
write = csv.writer(f)
write.writerow(fields)

for e in sitios21:
    write.writerow([e['EstadoIM'],e['FechaEstadoIM'],e['NombreProyecto'],e['NombreSitio'],e['Region'],e['Vendor'],e['codigoSitio'],e['fechaOASIM'],e['fechaRFASIM'],e['fechaRFISIM'],e['fechaRFPSIM'],e['fechaRFTSIM']])
f.close()