# Importa la classe
import logging

# Configurazione base del logging per tutta l'applicazione
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

import os
import sys
import pandas as pd
import re
import DF_Tools
import File_tools
import RE_tools
from PyQt5.QtWidgets import (QApplication, QMainWindow, QPushButton, QVBoxLayout, 
                           QHBoxLayout, QWidget, QTextEdit, QListWidget, QLabel, QMessageBox)
import SAP_Connection
import SAP_Transactions
import DF_Tools
import Config.constants as constants
from RE_tools import RegularExpressionsTools



# Logger specifico per questo modulo
logger = logging.getLogger("test_RE").setLevel(logging.DEBUG)

df_utils = DF_Tools.DataFrameTools()
tech_code = "S"
country_code = "IT"

# Genera il DataFrame con le espressioni regolari
risultato_ZPMR_CONTROL_FL1_lev_1 = ["A1","A2","A3", "A4"]
risultato_ZPMR_CONTROL_FL1_lev_2 = ["AA1","AA2","AA3", "AA4"]

liste_ZPMR_CONTROL_FL1 = [
    risultato_ZPMR_CONTROL_FL1_lev_1,
    risultato_ZPMR_CONTROL_FL1_lev_2
]

df, error = df_utils.create_df_from_lists_ZPMR_CONTROL_FL2(constants.intestazione_ZPMR_FL_2,
                                                        liste_ZPMR_CONTROL_FL1,
                                                        tech_code,
                                                        country_code)
# Verifica del risultato
if error is None:
    print(f"Operazione completata con successo!")
else:
    print(f"Si è verificato un errore: {error}")
print(df)


result, error = df_utils.save_dataframe_to_csv(df, 
                                constants.file_ZPMR_FL_2_UpLoad)

# Verifica del risultato
if error is None:
    print(f"File {constants.file_ZPMR_FL_2_UpLoad} salvato correttamente")
else:
    print(f"Si è verificato un errore: {error}")

# Genera il DataFrame con le espressioni regolari
risultato_ZPMR_CONTROL_FL1_lev_3 = ["A1","A2","A3", "A4"]
risultato_ZPMR_CONTROL_FL1_lev_4 = ["AA1","AA2","AA3", "AA4"]
risultato_ZPMR_CONTROL_FL1_lev_5 = ["AAA1","AAA2","AAA3", "AAA4"]
risultato_ZPMR_CONTROL_FL1_lev_6 = ["AAAA1","AAAA2","AAAA3", "AAAA4"]


liste_ZPMR_CONTROL_FL1 = [
    risultato_ZPMR_CONTROL_FL1_lev_3,
    risultato_ZPMR_CONTROL_FL1_lev_4,
    risultato_ZPMR_CONTROL_FL1_lev_5,
    risultato_ZPMR_CONTROL_FL1_lev_6    
]

df, error = df_utils.create_df_from_lists_ZPMR_CONTROL_FLn(constants.intestazione_ZPMR_FL_n,
                                                        liste_ZPMR_CONTROL_FL1,
                                                        tech_code)
# Verifica del risultato
if error is None:
    print(f"Operazione completata con successo!")
else:
    print(f"Si è verificato un errore: {error}")
print(df)

result, error = df_utils.save_dataframe_to_csv(df, 
                                constants.file_ZPMR_FL_n_UpLoad)

# Verifica del risultato
if error is None:
    print(f"File {constants.file_ZPMR_FL_n_UpLoad} salvato correttamente")
else:
    print(f"Si è verificato un errore: {error}")    