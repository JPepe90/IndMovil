# -*- coding: utf-8 -*-

from datetime import datetime
import os
import os.path
import requests
import json
import pandas as pd
import csv

total="INICIO;FIN;SITIO;NOMBRE_LUGAR;EMG;TECNOLOGIA;TT;DESCRIPCION;LUGAR\n"
for i in range(1, 13, 1):
    for j in range(1, 32, 1):
        
        if i < 10:
                mes = '0' + str(i)
        else: 
            mes = str(i)
        
        if j < 10:
            dia = '0' + str(j)
        else:
            dia = str(j)
        
        direc = '\\\pwin0616\\IntervencionesMoviles\\TodoChangeMoviles21'+mes+dia+'.csv'
        file_exists = os.path.exists(direc)
        
        if file_exists:
            
            with open(direc, 'r') as f:
                next(f)

                arc = f.read()
                total += arc

with open(os.path.dirname(os.getcwd()) + '\\noc_bajadas\\changeNOC.txt', 'w') as g:
    g.write(total)
    
    

    