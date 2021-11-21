# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import os
import os.path
import json
import pandas as pd
import csv

# Defino los proyectos a trabajar
proyectos = ["NB_IOT","ActivacionLTE850","ActivacionLTE1900","Overlay900",'OverlayLTE2600','RanSharing','Swap-Modernizacion','AmpliacionBW20Mhz_LTE1900','AmpliacionBWLTE1900','SatelitalModernizacion','Bateria de Litio','Bateria de Litio Ampliacion','HardwareReady','4T4R','4T4R Reuso','LTE2600+4T4R','5G BAFI','5G DSS']

proyectos=["4T4R"]


# Cargo la tabla de changes del NOC de la cual extraigo los ICD. Luego saco todos los registros que sean de AEAM ya que no corresponden a proyectos de IM
df_chgtot = pd.read_csv(os.path.dirname(os.getcwd()) + '\\noc_bajadas\\changeNOC.txt', sep=";", encoding="ISO-8859-1", error_bad_lines=False)

df_chg = df_chgtot[~df_chgtot["DESCRIPCION"].str.contains('AEAM')]

# Cargo la tabla de cambios de excelencia operacional de la cual voy a extraer los rangos de fecha de trabajo reales y filtro los que tengan status = CAN (cancelado)
df_eochgtot = pd.read_excel(os.path.dirname(os.getcwd()) + '\\eo_bajada\\cambios_eo.xlsx', sheet_name="Sheet1")
df_eochg = df_eochgtot[~df_eochgtot["status"].str.contains('CAN')]
df_eochg["inicioreal"] = pd.to_datetime(df_eochg['actstart'], dayfirst=True)



for prjt in proyectos:
    
    direc = os.path.dirname(os.getcwd()) + '\\sim_bajadas\\' + prjt + '\\filtrado.csv'
    file_exists = os.path.exists(direc)

    if file_exists:
        df_proy = pd.read_csv(direc, encoding="ISO-8859-1")
        df_proy_min = df_proy[['intervencionId', 'codigoSitio', 'NombreProyecto','ICD', 'estadoSigo', 'Vendor', 'EstadoIM', 'subEstado', 'FechaEstadoIM','fechaRFISIM', 'fechaRFASIM', 'fechaRFTSIM','fechaOASIM']]
        df_proy_min["fechamin"] = df_proy_min["fechaRFISIM"].astype('datetime64[ns]') - timedelta(days=15)
        df_proy_min["fechamax"] = df_proy_min["fechaRFISIM"].astype('datetime64[ns]') + timedelta(days=15)
        dfpaux = df_proy_min.copy()
        chg_dflst = df_chg[df_chg["SITIO"].isin(df_proy_min['codigoSitio'].tolist())]
        chg_dflst["TT"] = chg_dflst["TT"] - 1050000000
        chg_dflst.rename(columns = {'SITIO': 'codigoSitio'}, inplace=True)
        new_df = dfpaux.merge(chg_dflst, on='codigoSitio', how='inner', indicator=True)
        
        
        eofilt_df = df_eochg[(df_eochg["wonum"].isin(chg_dflst["TT"].tolist())) & (df_eochg["EO_Estado"].str.contains("Cerrado"))]
        eofilt_df.rename(columns={'wonum': 'TT'}, inplace=True)
        
        new_eo = new_df.merge(eofilt_df, on='TT', how='inner')
        
        
        ######################################################################
        # Filtrado por extremos de fecha: fechaRFI - 15 a fechaRFI +15
        new_eo_filt = new_eo[new_eo["inicioreal"].between(new_eo["fechamin"], new_eo["fechamax"])]
        
        new_eo_filt.reset_index(inplace=True)
        new_eo_filt["check"] = ''
        
        # for index in new_eo_filt.index:
        #     print(new_eo_filt.loc[index, 'codigoSitio'])
        #     print(new_eo_filt.loc[index, 'DESCRIPCION'])
            
        #     if new_eo_filt.loc[index, 'codigoSitio'] in new_eo_filt.loc[index, 'DESCRIPCION']:
        #         new_eo_filt.loc[index, 'check'] = 'SI'
        #     else:
        #         new_eo_filt.loc[index, 'check'] = 'NO'
        
        # dfas = new_eo_filt[new_eo_filt["check"].str.contains('SI')]
        
        dfas = new_eo_filt.copy()
        finaldf = dfas[['intervencionId', 'codigoSitio', 'NombreProyecto','ICD', 'DESCRIPCION', 'estadoSigo', 'Vendor', 'EstadoIM', 'subEstado', 'FechaEstadoIM','fechaRFISIM', 'fechaRFASIM', 'fechaRFTSIM','fechaOASIM', 'TT', 'status', 'schedstart', 'schedfinish', 'actstart','actfinish']]
        finaldf.drop_duplicates(inplace=True)
        finaldf['iql'] = False
        
        for index in finaldf.index:
            if str(finaldf.loc[index, 'ICD']) == str(finaldf.loc[index, 'TT']):
                finaldf.loc[index, 'iql'] = True
            else:
                finaldf.loc[index, 'iql'] = False
        
        finaldf["schedstart"] = pd.to_datetime(finaldf["schedstart"], dayfirst=True)
        finaldf["schedfinish"] = pd.to_datetime(finaldf["schedfinish"], dayfirst=True)
        finaldf["actstart"] = pd.to_datetime(finaldf["actstart"], dayfirst=True)
        finaldf["actfinish"] = pd.to_datetime(finaldf["actfinish"], dayfirst=True)
        
        finaldf.to_csv('sim_noc_eo\\procesado_'+prjt+'.txt', header=['intervencionId', 'codigoSitio', 'NombreProyecto','ICD', 'DESCRIPCION', 'estadoSigo', 'Vendor', 'EstadoIM', 'subEstado', 'FechaEstadoIM','fechaRFISIM', 'fechaRFASIM', 'fechaRFTSIM','fechaOASIM', 'TT', 'status', 'schedstart', 'schedfinish', 'actstart', 'actfinish', 'check_icd'], index=None, sep='|', mode='w')
        
        ######################################################################
            
        ######################################################################
        #Filtrado por extremos de fecha: fechaRFI - 15 a fechaRFT (si tiene fecha RFT, sino RFI+15)
        # new_eo_rft_aux = new_eo[~new_eo['fechaRFTSIM'].isnull()]
        # new_eo_rft_aux.reset_index(inplace=True)      
        
        # new_eo_filt_rft = new_eo_rft_aux[new_eo_rft_aux["inicioreal"].between(new_eo_rft_aux["fechamin"], new_eo_rft_aux["fechaRFTSIM"])]
        
        # new_eo_filt_rft.reset_index(inplace=True)
        # new_eo_filt_rft["check"] = ''
        
        # for index in new_eo_filt_rft.index:
        #     print(new_eo_filt_rft.loc[index, 'codigoSitio'])
        #     print(new_eo_filt_rft.loc[index, 'DESCRIPCION'])
                
        #     if new_eo_filt_rft.loc[index, 'codigoSitio'] in new_eo_filt_rft.loc[index, 'DESCRIPCION']:
        #         new_eo_filt_rft.loc[index, 'check'] = 'SI'
        #     else:
        #         new_eo_filt_rft.loc[index, 'check'] = 'NO'
        
        # dfas_rft = new_eo_filt_rft[new_eo_filt["check"].str.contains('SI')]
        # finaldf_rft = dfas_rft[['intervencionId', 'codigoSitio', 'NombreProyecto','ICD', 'DESCRIPCION', 'estadoSigo', 'Vendor', 'EstadoIM', 'subEstado', 'FechaEstadoIM','fechaRFISIM', 'fechaRFASIM', 'fechaRFTSIM','fechaOASIM', 'TT', 'status', 'schedstart', 'schedfinish', 'actstart','actfinish']]
        # finaldf_rft.drop_duplicates(inplace=True)
        # finaldf_rft['iql'] = False
        
        # for index in finaldf_rft.index:
        #     if str(finaldf_rft.loc[index, 'ICD']) == str(finaldf_rft.loc[index, 'TT']):
        #         finaldf_rft.loc[index, 'iql'] = True
        #     else:
        #         finaldf_rft.loc[index, 'iql'] = False
        
        # finaldf_rft["schedstart"] = pd.to_datetime(finaldf_rft["schedstart"], dayfirst=True)
        # finaldf_rft["schedfinish"] = pd.to_datetime(finaldf_rft["schedfinish"], dayfirst=True)
        # finaldf_rft["actstart"] = pd.to_datetime(finaldf_rft["actstart"], dayfirst=True)
        # finaldf_rft["actfinish"] = pd.to_datetime(finaldf_rft["actfinish"], dayfirst=True)
        
        # finaldf_rft.to_csv('sim_noc_eo\\procesado_'+prjt+'_rft.txt', header=['intervencionId', 'codigoSitio', 'NombreProyecto','ICD', 'DESCRIPCION', 'estadoSigo', 'Vendor', 'EstadoIM', 'subEstado', 'FechaEstadoIM','fechaRFISIM', 'fechaRFASIM', 'fechaRFTSIM','fechaOASIM', 'TT', 'status', 'schedstart', 'schedfinish', 'actstart', 'actfinish', 'check_icd'], index=None, sep='|', mode='w')
        
        ######################################################################
            
            
        
        
        
        
        
        
        
        
