# -*- coding: utf-8 -*-

from datetime import datetime
import os
import os.path
import requests
import json
import pandas as pd
import csv

eo_csv_url = "http://10.75.132.243/Reportes/Cambios.xlsx"

xl_df = pd.read_excel(eo_csv_url, sheet_name="SalidaChange")

xl_df.to_excel(os.path.dirname(os.getcwd()) + '\\eo_bajada\\cambios_eo.xlsx')