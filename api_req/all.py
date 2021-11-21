# -*- coding: utf-8 -*-
"""
Created on Mon Nov  1 16:35:57 2021

@author: u565589
"""

from datetime import datetime
import os
import requests
import json
import pprint
import csv

proyectos = ['4T4R','4T4R Reuso','LTE2600+4T4R','5G BAFI','5G DSS','ActivacionLTE1900','ActivacionLTE850','Bateria de Litio','Bateria de Litio Ampliacion','HardwareReady','HardwareReady850','NB_IOT','Overlay900','OverlayLTE2600','RanSharing','Swap-Modernizacion','AmpliacionBW20Mhz_LTE1900','AmpliacionBWLTE1900','SatelitalModernizacion']

actualyear = datetime.today().year

url = "http://api.sim.telecom.com.ar/api/v3/proyecto/"

payload={}
headers = {
  'Content-Type': 'application/json'
}

for pjct in proyectos:
    endpnt = url + pjct
    print(os.path.dirname(os.getcwd()) + '\\sim_bajadas\\' + pjct)
    r = requests.request("GET", endpnt, headers=headers, data=payload)
    
    lista = json.loads(r.text)
    
    filtrado = [ele for ele in lista if ele["fechaRFASIM"] is not None if datetime.strptime(ele["fechaRFASIM"], '%Y-%m-%d %H:%M:%S').date().year == actualyear]
    
    if len(filtrado) > 0:
        enca = filtrado[0].keys()
        with open(os.path.dirname(os.getcwd()) + '\\sim_bajadas\\' + pjct + '\\filtrado.csv', 'w', newline='') as csv_f:
            dict_writer = csv.DictWriter(csv_f, enca)
            dict_writer.writeheader()
            dict_writer.writerows(filtrado)
        
        if not csv_f.closed:
            csv_f.close()
        else:
            print("archivo cerrado en with")
            
        
        with open(os.path.dirname(os.getcwd()) + '\\sim_bajadas\\' + pjct + '\\filtrado.json', 'w') as jout:
            # jout.write(json.dumps(filtrado, indent=2))
            json.dump(filtrado, jout, indent=2)
        
        if not jout.closed:
            jout.close()
        else:
            print("archivo cerrado en with")
        

# csv_f = open(os.path.dirname(os.getcwd()) + '\\sim_bajadas\\total\\all.csv', 'w', newline='')
# dict_writer = csv.DictWriter(csv_f, enca)
# dict_writer.writeheader()
# dict_writer.writerows(filtrado)
# csv_f.close()
