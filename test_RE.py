# Importa la classe
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

""" 
# Genera il DataFrame con le espressioni regolari
df_regex = RegularExpressionsTools.Make_DF_RE(constants.file_Rules, constants.file_FL_Bess)

 """
# Genera il DataFrame con le espressioni regolari
File_guideLine_list = [constants.file_FL_B_SubStation, constants.file_FL_Bess]
try:
    df_regex = RegularExpressionsTools.Make_DF_RE_list(constants.file_Rules, File_guideLine_list)
except Exception as e:
    print(f"Errore durante il processing dei file: {str(e)}")

print(df_regex)
# salvo il df in un file csv
df_regex.to_csv('df_re_completo.csv', index=False)

# Usa il DataFrame con le espressioni regolari per verificare un altro DataFrame
df_fl = pd.read_csv('FL_Bess_Ables.csv', sep=';')
# Disabilita temporaneamente i limiti di riga e colonna
print(df_fl)

# Verifica le FL
try:
    result_df = RegularExpressionsTools.verifica_fl_con_regex(df_fl, df_regex)
except Exception as e:
    print(f"Errore durante il processing dei file: {str(e)}")

# salvo il df in un file csv
result_df.to_csv('df_result_completo.csv', index=False)
print(result_df)

# Trova le FL non valide
fl_non_valide = result_df[result_df['Check_Result'] != True]['FL'].tolist()
print(fl_non_valide)