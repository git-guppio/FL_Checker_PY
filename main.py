from ast import pattern
from opcode import hasconst
import os
import sys
import pandas as pd
import re
import DF_Tools
import File_tools
import RE_tools
from PyQt5.QtWidgets import (QApplication, QMainWindow, QPushButton, QVBoxLayout, 
                           QHBoxLayout, QWidget, QTextEdit, QListWidget, QLabel, QMessageBox,
                           QDialog, QRadioButton, QButtonGroup, QDialogButtonBox)
import SAP_Connection
import SAP_Transactions
import DF_Tools
import Config.constants as constants
from RE_tools import RegularExpressionsTools

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

# Logger specifico per questo modulo
logger = logging.getLogger("main").setLevel(logging.DEBUG)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        # Inizializza l'istanza di DataFrameTools come attributo della classe
        self.df_utils = DF_Tools.DataFrameTools()
        self.file_utils = File_tools.FileTools()
        self.re_utils = RE_tools.RegularExpressionsTools()
        # Inizializza l'interfaccia utente
        self.setWindowTitle("FL Data Manager")
        self.setGeometry(100, 100, 1000, 600)
        self.init_ui()
        # Ottiene il percorso della directory del file Python corrente
        self.current_dir = os.path.dirname(os.path.abspath(__file__))

    def init_ui(self):
        # Widget centrale
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Layout principale
        main_layout = QVBoxLayout(central_widget)
        
        # Layout orizzontale per i due pannelli
        content_layout = QHBoxLayout()
        
        # Pannello sinistro (TextEdit per clipboard)
        left_panel = QVBoxLayout()
        left_label = QLabel("Dati da Clipboard:")
        left_panel.addWidget(left_label)
        
        self.clipboard_area = QTextEdit()
        self.clipboard_area.setPlaceholderText("Incolla qui i dati (Ctrl+V)")
        left_panel.addWidget(self.clipboard_area)
        
        # Aggiungi il layout sinistro al layout orizzontale
        content_layout.addLayout(left_panel)
        
        # Pannello destro (ListView per log)
        right_panel = QVBoxLayout()
        right_label = QLabel("Log operazioni:")
        right_panel.addWidget(right_label)
        
        self.log_list = QListWidget()
        right_panel.addWidget(self.log_list)
        
        # Aggiungi il layout destro al layout orizzontale
        content_layout.addLayout(right_panel)
        
        # Aggiungi il layout dei contenuti al layout principale
        main_layout.addLayout(content_layout)
        
        # Layout per i bottoni
        button_layout = QHBoxLayout()
        
        # Bottone Pulisci
        self.clear_button = QPushButton('Pulisci Finestre')
        self.clear_button.clicked.connect(self.clear_windows)
        button_layout.addWidget(self.clear_button)
        
        # Bottone Estrai
        self.extract_button = QPushButton('Estrai Dati')
        self.extract_button.clicked.connect(self.extract_data)
        button_layout.addWidget(self.extract_button)
        
        # Bottone Upload
        self.upload_button = QPushButton('Upload Dati')
        self.upload_button.clicked.connect(self.upload_data)
        self.upload_button.setEnabled(False)  # Disabilitato finché non implementato
        button_layout.addWidget(self.upload_button)
        
        # Aggiungi il layout dei bottoni al layout principale
        main_layout.addLayout(button_layout)

    def log_message(self, message, icon_type='info'):
        """
        Aggiunge un messaggio al log con un'emoji come icona
        """
        icons = {
            'info': '\U0001f604',
            'error': '❌',
            'success': '✅',
            'warning': '⚠️',
            'loading': '⏳'
        }
        
        icon = icons.get(icon_type, '')
        self.log_list.addItem(f"{icon} {message}")
        self.log_list.scrollToBottom()

    def log_risultato_differenze(self, nomeTabella, risultato):
        try:
            if risultato is not None:
                # Il risultato contiene elementi
                num_elementi = len(risultato)
                self.log_message(nomeTabella + " - Elementi non presenti: " + str(num_elementi), 'error')
                self.log_message(risultato, 'warning')
            else:
                # Il risultato è None
                self.log_message(nomeTabella + ": Tutti gli elementi presenti", 'success')
        
        except Exception as e:
            raise Exception(f"Errore durante la stampa dei risultati: {str(e)}")

    def clear_windows(self):
        self.clipboard_area.clear()
        self.log_list.clear()
        self.log_message("Finestre pulite")

    def validate_clipboard_data(self):
        """Valida i dati nella clipboard"""
        data = self.clipboard_area.toPlainText().strip().split('\n')
        data = [line.strip() for line in data if line.strip()]  # Rimuove linee vuote
        
        # Verifica se ci sono dati
        if not data:
            QMessageBox.warning(self, "Attenzione", "Inserire i dati nella finestra di sinistra prima di procedere.")
            return False
            
        # Esempio di pattern regex per la validazione
        # utilizzo una maschera generica, dato che ancora non ho rilevato la tecnologia
        patterns = {
            'MaskGenerica': r'^(?:([A-Z0-9]{3})(?:-([A-Z0-9]{4})(?:-([A-Z0-9]{2})(?:-([A-Z0-9]{2,3})(?:-([A-Z0-9]{2,3})(?:-([A-Z0-9]{2}))?)?)?)?)?)?$',
            # aggiungere altre maschere se necessario
        }
        
        #lines = data.split('\n')
        for i, line in enumerate(data, 1):
            if not line.strip():
                continue
                
            try:
                if not re.match(patterns['MaskGenerica'], line):
                    error_msg = ("Errore riga {}: il testo non rispetta le maschere FL '-'").format(i)
                    self.log_message(error_msg, 'error')
                    QMessageBox.warning(self, "Errore di Validazione", error_msg)
                    return False                 

            except Exception as e:
                self.log_message(f"Errore nel processare la riga {i}: {str(e)}", 'error')
                return False
                
        self.log_message("Validazione dati completata con successo", 'success')
        return True  

    def create_dataframe(self):
        """Crea un DataFrame dai dati della clipboard e aggiunge le colonne richieste"""
        try:
            # Ottiene i dati validati dalla clipboard
            data = self.clipboard_area.toPlainText().strip().split('\n')
            data = [line.strip() for line in data if line.strip()]  # Rimuove linee vuote
            
            # Crea DataFrame base con colonna FL
            df = pd.DataFrame(data, columns=['FL'])
            
            # Aggiunge le colonne per i livelli e la lunghezza 
            df, error = self.df_utils.add_level_lunghezza(df, 'FL')
            if error is None:
                print("Aggiunta colonna lunghezza al DF delle FL")
                print(df)  # Funziona correttamente
            else:
                print(f"Si è verificato un errore: {error}")
                return

            # Aggiunge la colonna <Check> per la verifica della presenza delle singole FL nelle tabelle globali
            df, error = self.df_utils.add_concatenated_column_FL(df, "Livello_6", "Livello_5", "Livello_4", "Livello_3", "FL_Lunghezza")
            if error is None:
                print("Aggiunta colonna check al DF delle FL")
                print(df)  # Funziona correttamente
            else:
                print(f"Si è verificato un errore: {error}")
                return            
            # Memorizza il DataFrame
            self.df_FL = df
            
            # Log dei risultati
            self.log_message(f"DataFrame creato con successo: {len(df)} righe", 'success')
            """         
            self.log_message("\nStruttura del DataFrame:")
            self.log_message(f"Colonne: {list(df.columns)}")
            self.log_message("\nPrime righe del DataFrame:")
            for idx, row in df.head().iterrows():
                self.log_message(f"Riga {idx + 1}:")
                self.log_message(f"  FL: {row['FL']}")
                self.log_message(f"  Elementi: {row['1']} | {row['2']} | {row['3']} | {row['4']} | {row['5']} | {row['6']}")
                self.log_message(f"  Lunghezza: {row['FL_Lunghezza']}")
                self.log_message("---")
            """
                # Stampa anteprima del dataframe
            self.df_utils.analyze_data(self.df_FL)
            
            return True
            
        except Exception as e:
            self.log_message(f"Errore nella creazione del DataFrame: {str(e)}", 'error')
            return False       

    def extract_data(self):

        # Prima verifica i dati nella clipboard
        if not self.validate_clipboard_data():
            return
        # Creo un DF con i dati contenuti nella finestra
        if not self.create_dataframe():
            return
        # ----------------------------------------------------
        # Verifico che i dati della prima e seconda colonna siano univoci
        # ----------------------------------------------------
        if not (self.df_FL['Livello_1'].nunique()):
            self.log_message("Errore: Valori nella prima colonna non univoci", 'error')
            return
        else:
            self.log_message("Check: Valori nella prima colonna univoci", 'success')

        if not (self.df_FL['Livello_2'].nunique()):
            self.log_message("Errore: Valori nella seconda colonna non univoci", 'error')
            return
        else:
            self.log_message("Check: Valori nella seconda colonna univoci", 'success')

        # ----------------------------------------------------
        # ricavo codice Country 
        # ----------------------------------------------------
        file_country = constants.file_Country
        country_code = self.df_utils.get_first_two_chars(self.df_FL, "Livello_1")
        if (country_code == None):
            self.log_message("Errore: Valore country code non trovato", 'error')
            return
        description_country = self.file_utils.trova_valore(file_country, 
                                    valore_da_cercare=country_code, 
                                    colonna_da_cercare="Country", 
                                    colonna_da_restituire="Description")
        if (description_country == None):
            self.log_message("Errore: Valore country non trovato", 'error')
            return
        else:
            self.log_message(f"Check: Country = {description_country}", 'success')
        # ----------------------------------------------------    
        # ricavo codice Tecnologia
        # ----------------------------------------------------
        file_tech = constants.file_Tech
        tech_code = self.df_utils.get_last_char(self.df_FL, "Livello_1")
        if (tech_code == None):
            self.log_message("Errore: Valore tecnologia code non trovato", 'error')
            return
        description_techno = self.file_utils.trova_valore(file_tech, 
                                    valore_da_cercare=tech_code, 
                                    colonna_da_cercare="Code", 
                                    colonna_da_restituire="Description")
        if (description_techno == None):
            self.log_message("Errore: Valore tecnologia non trovato", 'error')
            return
        else:
            self.log_message(f"Check: Country = {description_techno}", 'success')

        self.extract_button.setEnabled(False)
        # ----------------------------------------------------    
        # verifico coerenza con la maschera della tecnolgia
        # ----------------------------------------------------

       
        # ----------------------------------------------------    
        # verifico coerenza con la guideline dele tecnologia
        # ----------------------------------------------------
        """         
        Il parametro regex_dict è un dizionario che definisce dei pattern per classificare le righe
        del DataFrame contenente le FL e il DataFrame contenenti le espressioni regolari in categorie specifiche.
        Questo permette di applicare i pattern corretti a seconda della categoria di FL, in modo da non avere duplicati nei controlli

        """
        if tech_code == 'E':
            # Creo una lista con i file delle guideLine da utilizzare per la tecnologia
            File_guideLine_list = [constants.file_FL_B_SubStation, constants.file_FL_Bess]
            # Definisco il dizionario di regex
            regex_dict = {
                'SubStation': [r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-0A'],
                'Common': [r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-00',r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-0E',r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-WE',r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-ZE']
            }            

        elif tech_code == 'W':
            # Creo una lista con i file delle guideLine da utilizzare per la tecnologia
            File_guideLine_list = [constants.file_FL_W_SubStation, constants.file_FL_Wind]
            # Definisco il dizionario di regex            
            regex_dict = {
                'SubStation': [r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-0A'],
                'Common': [r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-00']
            }             

        elif tech_code == 'S':
            # Apri la finestra di dialogo per selezionare il tipo di inverter
            self.log_message("Tecnologia Solare rilevata: selezione tipo inverter...", 'info')
            dialog = InverterSelectionDialog(self)
            if dialog.exec_() == QDialog.Accepted:
                inverter_type = dialog.get_selected_inverter_type()
                self.log_message(f"Tipo di inverter selezionato: {inverter_type}", 'success')
                
                # Creo una lista con i file delle guideLine da utilizzare per la tecnologia solare
                # con il tipo di inverter specifico
                File_guideLine_list = [constants.file_FL_S_SubStation, constants.file_FL_Solar_Common]
                
                # Aggiungi il file specifico per il tipo di inverter selezionato
                if inverter_type == "Central Inverter":
                    File_guideLine_list.append(constants.file_FL_Solar_CentralInv)
                elif inverter_type == "String Inverter":
                    File_guideLine_list.append(constants.file_FL_Solar_StringInv)
                elif inverter_type == "Inverter Module":
                    File_guideLine_list.append(constants.file_FL_Solar_InvModule)
                # Definisco il dizionario di regex                    
                regex_dict = {
                    'SubStation': [r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-0A'],
                    'Common': [r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-00',r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-ZZ',r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-9Z']
                } 

            else:
                # L'utente ha annullato la selezione dell'inverter
                self.log_message("Selezione tipo inverter annullata", 'warning')
                self.extract_button.setEnabled(True)
                return
            
        elif tech_code == 'H':
            # Creo una lista con i file delle guideLine da utilizzare per la tecnologia
            File_guideLine_list = [constants.file_FL_B_SubStation, constants.file_FL_Bess]
            # Definisco il dizionario di regex                    
            regex_dict = {
                'SubStation': [r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-0A'],
                'Common': [r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-00',r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-ZZ',r'^[a-zA-Z]{3}-[a-zA-Z0-9]{4}-9Z']
            }             
        else:
            self.log_message("Errore: Tecnologia non riconosciuta", 'error')
            return

        # Genera un unico DataFrame con le espressioni regolari a partire dai file di regole e la lista delle guideLine
        try:
            df_regex = RegularExpressionsTools.Make_DF_RE_list(constants.file_Rules, File_guideLine_list)
        except Exception as e:
            print(f"Errore durante il processing dei file: {str(e)}")
       
        print("#----------- df_regex ---------#")
        print(df_regex)
        # salvo il df in un file csv
        df_regex.to_csv('df_re_completo.csv', index=False)
        
        """ 
        Creo un Dizionario contenente i DataFrame filtrati con le chiavi contenute 
        in regex_dict originali più una chiave 'Others' per le righe che non corrispondono a nessun pattern
        """
        try:
            # Eseguiamo la verifica
            result_df = RegularExpressionsTools.verifica_fl_con_regex_per_categorie(self.df_FL, df_regex, regex_dict)
            
            # Stampiamo i risultati
            print(f"\nRisultati della verifica: result_df = {len(result_df)} righe | self.df_fl = {len(self.df_FL)} righe")
            print(result_df)
            
        except Exception as e:
            print(f"Errore nell'esecuzione: {str(e)}")   
       
        # salvo il df in un file csv
        result_df.to_csv('df_result_completo.csv', index=False)
        print(result_df)

        # Trova le FL non valide
        fl_non_valide = result_df[result_df['Check_Result'] != True]['FL'].tolist()

        # Mostra la lista delle FL non valide
        if fl_non_valide:
            # Mostra il numero di FL non valide
            # Mostra il numero di FL non valide con singolare o plurale
            self.log_message(f"Errore: {len(fl_non_valide)} FL non valid{'a' if len(fl_non_valide) == 1 else 'e'}:", 'warning')
            #self.log_message(f"{"FL non valida" if len(fl_non_valide) == 1 else "Lista delle FL non valide"}:", 'warning')
            for fl in fl_non_valide:
                # Ottieni il messaggio di errore specifico per questa FL
                error_msg = result_df[result_df['FL'] == fl]['Check_Result'].values[0]
                self.log_message(f"{fl}: {error_msg}", 'error')
            return
        else:
            self.log_message("Tutte le FL sono valide", 'success')

      
        # ----------------------------------------------------
        # estraggo i dati da SAP e creo i df
        # ----------------------------------------------------
        # Verifico se sono in modalità debug o no
        if constants.DEBUG_MODE:
            # In modalità debug, non estraggo i dati da SAP ma utilizzo quelli salvati in precedenza
            self.log_message("Modalità debug attiva: leggo i dati da file", 'warning')
            # Lista dei nomi dei DataFrame da caricare
            df_list = [
                "df_ZPMR_CTRL_ASS",
                "df_ZPM4R_GL_T_FL",
                "df_ZPMR_CONTROL_FL2_pivot",
                "df_ZPMR_CONTROL_FL1_pivot"
            ]
            dfs, error = self.df_utils.load_dataframes_from_csv(df_list)
            # Verifica del risultato
            if dfs:
                print(f"File SAP caricati correttamente")
                # Estrai i DataFrame in variabili separate
                df_ZPMR_CONTROL_FL1_pivot = dfs["df_ZPMR_CONTROL_FL1_pivot"]        
                df_ZPMR_CONTROL_FL2_pivot = dfs["df_ZPMR_CONTROL_FL2_pivot"]                        
                df_ZPMR_CTRL_ASS = dfs["df_ZPMR_CTRL_ASS"]
                df_ZPM4R_GL_T_FL = dfs["df_ZPM4R_GL_T_FL"]

                self.log_message("File SAP caricati correttamente!", 'success')
            elif error is not None:
                print(f"Si è verificato un errore nel caricamento dei file: {error}")
                self.log_message("Errore nel caricamento dei file SAP", 'error')

        else:
            # altrimenti estraggo i dati da SAP
            self.log_message("Avvio estrazione...")
            try:
                with SAP_Connection.SAPGuiConnection() as sap:
                    if sap.is_connected():
                        session = sap.get_session()
                        if session:
                            self.log_message("Connessione SAP attiva", 'success')
                            extractor = SAP_Transactions.SAPDataExtractor(session)
                            self.log_message("Estrazione dati tabella ZPMR_CONTROL_FL1", 'loading')
                            string_ZPMR_CONTROL_FL1 = extractor.extract_ZPMR_CONTROL_FL1(tech_code)
                            
                            self.log_message("Estrazione dati tabella ZPMR_CONTROL_FL2", 'loading')
                            string_ZPMR_CONTROL_FL2 = extractor.extract_ZPMR_CONTROL_FL2(tech_code)                        
                            
                            self.log_message("Estrazione dati tabella ZPM4R_GL_T_FL", 'loading')
                            string_ZPM4R_GL_T_FL = extractor.extract_ZPM4R_GL_T_FL(tech_code)
                            
                            self.log_message("Estrazione dati tabella ZPMR_CTRL_ASS", 'loading')
                            string_ZPMR_CTRL_ASS = extractor.extract_ZPMR_CTRL_ASS(tech_code)
                            
                            self.log_message("Estrazione completata con successo", 'success')
                    else:
                        self.log_message("Connessione SAP NON attiva", 'error')
                        return
            except Exception as e:
                self.log_message(f"Estrazione dati SAP: Errore: {str(e)}", 'error')
                return           
            # ------------estrazione SAP completata---------------
            try:
            # ----------------------------------------------------
            # creo DF per ZPMR_CONTROL_FL1
            # ----------------------------------------------------
                # Pulisce i nomi delle colonne
                df_ZPMR_CONTROL_FL1 = self.df_utils.clean_data(string_ZPMR_CONTROL_FL1)
                # Verifica che il DataFrame sia valido
                if not(self.df_utils.check_dataframe(df_ZPMR_CONTROL_FL1, name="ZPM4R_GL_T_FL1")):
                    print("Errore nella verifica del DataFrame")
                    sys.exit(1)
                else:
                    # Stampa anteprima del dataframe
                    self.df_utils.analyze_data(df_ZPMR_CONTROL_FL1)
                # creo un nuovo DF facendo pivot sulla colonna <Liv.Sede>
                try:
                    df_ZPMR_CONTROL_FL1_pivot = self.df_utils.pivot_hierarchy(df_ZPMR_CONTROL_FL1, "Valore Livello", "Liv.Sede")
                    print("#---- df_ZPMR_CONTROL_FL1_pivot ----#")
                    print(df_ZPMR_CONTROL_FL1_pivot)
                except Exception as e:
                    print(f"Errore: {e}")            
                # Stampa anteprima del dataframe
                print("---- Stampa df: df_ZPMR_CONTROL_FL1_pivot ----")
                print(df_ZPMR_CONTROL_FL1_pivot)
                ## ------------Salvo il DF in un file---------------
                nome_file = os.path.join(constants.A_ScriptDir, "df_ZPMR_CONTROL_FL1_pivot") + ".csv"
                result, error = self.df_utils.save_dataframe_to_csv(df_ZPMR_CONTROL_FL1_pivot, 
                                                                    nome_file)
                # Verifica del risultato
                if result is True:
                    print(f"File {nome_file} salvato correttamente")
                    self.log_message("File " + nome_file + " creato correttamente!", 'success')
                elif error is not None:
                    print(f"Si è verificato un errore nella creazione del file: " + nome_file + ": {error}")
                    self.log_message("Errore nella creazione del file: " + nome_file, 'error')
            # ----------------------------------------------------
            # creo DF per ZPMR_CONTROL_FL2
            # ----------------------------------------------------
                # Pulisce i nomi delle colonne
                df_ZPMR_CONTROL_FL2 = self.df_utils.clean_data(string_ZPMR_CONTROL_FL2)
                # Verifica che il DataFrame sia valido
                if not(self.df_utils.check_dataframe(df_ZPMR_CONTROL_FL2, name="ZPM4R_GL_T_FL2")):
                    print("Errore nella verifica del DataFrame")
                    sys.exit(1)
                else:
                    # Stampa anteprima del dataframe
                    self.df_utils.analyze_data(df_ZPMR_CONTROL_FL2)
                # creo un nuovo DF facendo pivot sulla colonna <Liv.Sede>
                try:
                    df_ZPMR_CONTROL_FL2_pivot = self.df_utils.pivot_hierarchy(df_ZPMR_CONTROL_FL2, "Valore Livello", "Liv.Sede")
                    print("---- df_ZPMR_CONTROL_FL2_pivot ----")
                    print(df_ZPMR_CONTROL_FL2_pivot)
                except Exception as e:
                    print(f"Errore: {e}")            
                # Stampa anteprima del dataframe
                print("---- Stampa df: df_ZPMR_CONTROL_FL2_pivot ----")
                print(df_ZPMR_CONTROL_FL2_pivot)
                ## ------------Salvo il DF in un file---------------
                nome_file = os.path.join(constants.A_ScriptDir, "df_ZPMR_CONTROL_FL2_pivot") + ".csv"
                result, error = self.df_utils.save_dataframe_to_csv(df_ZPMR_CONTROL_FL2_pivot, 
                                                                    nome_file)
                # Verifica del risultato
                if result is True:
                    print(f"File {nome_file} salvato correttamente")
                    self.log_message("File " + nome_file + " creato correttamente!", 'success')
                elif error is not None:
                    print(f"Si è verificato un errore nella creazione del file: " + nome_file + ": {error}")
                    self.log_message("Errore nella creazione del file: " + nome_file, 'error')                    
            # ----------------------------------------------------
            # creo DF per ZPM4R_GL_T_FL
            # ----------------------------------------------------
                # Pulisce i nomi delle colonne
                df_ZPM4R_GL_T_FL = self.df_utils.clean_data(string_ZPM4R_GL_T_FL)
                # Verifica che il DataFrame sia valido
                if not(self.df_utils.check_dataframe(df_ZPM4R_GL_T_FL, name="ZPM4R_GL_T_FL")):
                    print("Errore nella verifica del DataFrame")
                    sys.exit(1)
                else:
                    # Aggiunge la colonna per la verifica
                    df_ZPM4R_GL_T_FL = self.df_utils.add_concatenated_column_SAP(df_ZPM4R_GL_T_FL, "Valore Livello", "Valore Liv. Superiore", "Valore Liv. Superiore_1", "Liv.Sede")
                    # Stampa anteprima del dataframe
                    print("---- Stampa df: df_ZPM4R_GL_T_FL ----")                
                    print(df_ZPM4R_GL_T_FL)                
                    ## ------------Salvo il DF in un file---------------
                    nome_file = os.path.join(constants.A_ScriptDir, "df_ZPM4R_GL_T_FL") + ".csv"
                    result, error = self.df_utils.save_dataframe_to_csv(df_ZPM4R_GL_T_FL, 
                                                                        nome_file)
                    # Verifica del risultato
                    if result is True:
                        print(f"File {nome_file} salvato correttamente")
                        self.log_message("File " + nome_file + " creato correttamente!", 'success')
                    elif error is not None:
                        print(f"Si è verificato un errore nella creazione del file: " + nome_file + ": {error}")
                        self.log_message("Errore nella creazione del file: " + nome_file, 'error')
            # ----------------------------------------------------
            # creo DF per ZPMR_CTRL_ASS
            # ----------------------------------------------------
                # Pulisce i nomi delle colonne
                df_ZPMR_CTRL_ASS = self.df_utils.clean_data(string_ZPMR_CTRL_ASS)
                # Verifica che il DataFrame sia valido
                if not(self.df_utils.check_dataframe(df_ZPMR_CTRL_ASS, name="ZPMR_CTRL_ASS")):
                    print("Errore nella verifica del DataFrame")
                    sys.exit(1)
                else:
                    # Aggiunge la colonna per la verifica
                    df_ZPMR_CTRL_ASS = self.df_utils.add_concatenated_column_SAP(df_ZPMR_CTRL_ASS, "Valore Livello", "Valore Liv. Superiore", "Valore Liv. Superiore_1", "Liv.Sede")
                    # Stampa anteprima del dataframe
                    print("---- Stampa df: df_ZPMR_CTRL_ASS ----")                
                    print(df_ZPMR_CTRL_ASS)                
                    ## ------------Salvo il DF in un file---------------
                    nome_file = os.path.join(constants.A_ScriptDir, "df_ZPMR_CTRL_ASS") + ".csv"
                    result, error = self.df_utils.save_dataframe_to_csv(df_ZPMR_CTRL_ASS, 
                                                                        nome_file)
                    # Verifica del risultato
                    if result is True:
                        print(f"File {nome_file} salvato correttamente")
                        self.log_message("File " + nome_file + " creato correttamente!", 'success')
                    elif error is not None:
                        print(f"Si è verificato un errore nella creazione del file: " + nome_file + ": {error}")
                        self.log_message("Errore nella creazione del file: " + nome_file, 'error')
            # ----------------------------------------------------
            except Exception as e:
                self.log_message(f"Creazione DF: Errore: {str(e)}", 'error')
                return  
            # ------------fine creazione DF-----------------------

        
        # ----------------------------------------------------
        # verifica degli elementi della FL nelle tabelle
        # ----------------------------------------------------        
        # verifico la presenza degli elementi del primo livello nella tabella globale
        risultato_ZPMR_CONTROL_FL1_lev_1, error = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL1_pivot, 'Livello_1', 'Livello_1')
        # Verifica del risultato
        if ((error is None) and (risultato_ZPMR_CONTROL_FL1_lev_1 is not None)):
            self.log_risultato_differenze("Livello_1", risultato_ZPMR_CONTROL_FL1_lev_1)
        elif (error is not None):
            print(f"Si è verificato un errore nella creazione della lista: risultato_ZPMR_CONTROL_FL1")
            self.log_message("Errore nella creazione della lista: risultato_ZPMR_CONTROL_FL1", 'error')

        
        # verifico la presenza degli elementi del secondo livello nella tabella globale
        risultato_ZPMR_CONTROL_FL1_lev_2, error = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL1_pivot, 'Livello_2', 'Livello_2')
        # Verifica del risultato
        if ((error is None) and (risultato_ZPMR_CONTROL_FL1_lev_2 is not None)):
            self.log_risultato_differenze("Livello_1", risultato_ZPMR_CONTROL_FL1_lev_2)
        elif (error is not None):
            print(f"Si è verificato un errore nella creazione della lista: risultato_ZPMR_CONTROL_FL1_lev_2")
            self.log_message("Errore nella creazione della lista: risultato_ZPMR_CONTROL_FL1_lev_2", 'error')
        
        # verifico la presenza degli elementi del terzo livello nella tabella globale
        risultato_ZPMR_CONTROL_FL2_lev_3, error = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL2_pivot, 'Livello_3', 'Livello_3')
        # Verifica del risultato
        if ((error is None) and (risultato_ZPMR_CONTROL_FL2_lev_3 is not None)):
            self.log_risultato_differenze("Livello_1", risultato_ZPMR_CONTROL_FL2_lev_3)
        elif (error is not None):
            print(f"Si è verificato un errore nella creazione della lista: risultato_ZPMR_CONTROL_FL2_lev_3")
            self.log_message("Errore nella creazione della lista: risultato_ZPMR_CONTROL_FL2_lev_3", 'error')

        # verifico la presenza degli elementi del quarto livello nella tabella globale
        risultato_ZPMR_CONTROL_FL2_lev_4, error = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL2_pivot, 'Livello_4', 'Livello_4')
        # Verifica del risultato
        if ((error is None) and (risultato_ZPMR_CONTROL_FL2_lev_4 is not None)):
            self.log_risultato_differenze("Livello_1", risultato_ZPMR_CONTROL_FL2_lev_4)
        elif (error is not None):
            print(f"Si è verificato un errore nella creazione della lista: risultato_ZPMR_CONTROL_FL2_lev_4")
            self.log_message("Errore nella creazione della lista: risultato_ZPMR_CONTROL_FL2_lev_4", 'error')

        # verifico la presenza degli elementi del quinto livello nella tabella globale
        risultato_ZPMR_CONTROL_FL2_lev_5, error = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL2_pivot, 'Livello_5', 'Livello_5')
        # Verifica del risultato
        if ((error is None) and (risultato_ZPMR_CONTROL_FL2_lev_5 is not None)):
            self.log_risultato_differenze("Livello_1", risultato_ZPMR_CONTROL_FL2_lev_5)
        elif (error is not None):
            print(f"Si è verificato un errore nella creazione della lista: risultato_ZPMR_CONTROL_FL2_lev_5")
            self.log_message("Errore nella creazione della lista: risultato_ZPMR_CONTROL_FL2_lev_5", 'error')

        # verifico la presenza degli elementi del sesto livello nella tabella globale
        risultato_ZPMR_CONTROL_FL2_lev_6, error = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL2_pivot, 'Livello_6', 'Livello_6')
        # Verifica del risultato
        if ((error is None) and (risultato_ZPMR_CONTROL_FL2_lev_6 is not None)):
            self.log_risultato_differenze("Livello_1", risultato_ZPMR_CONTROL_FL2_lev_6)
        elif (error is not None):
            print(f"Si è verificato un errore nella creazione della lista: risultato_ZPMR_CONTROL_FL2_lev_6")
            self.log_message("Errore nella creazione della lista: risultato_ZPMR_CONTROL_FL2_lev_6", 'error')  

        # verifico la presenza degli elementi della tabella df_ZPMR_CTRL_ASS
        risultato_ZPMR_CTRL_ASS, error = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CTRL_ASS, 'Check', 'Check')
        # Verifica del risultato
        if ((error is None) and (risultato_ZPMR_CTRL_ASS is not None)):
            self.log_risultato_differenze("risultato_ZPMR_CTRL_ASS", risultato_ZPMR_CTRL_ASS)
        elif (error is not None):
            print(f"Si è verificato un errore nella creazione della lista: risultato_ZPMR_CTRL_ASS")
            self.log_message("Errore nella creazione della lista: risultato_ZPMR_CTRL_ASS", 'error')              

        # verifico la presenza degli elementi della tabella df_ZPM4R_GL_T_FL
        risultato_ZPM4R_GL_T_FL, error = self.df_utils.trova_differenze(self.df_FL, df_ZPM4R_GL_T_FL, 'Check', 'Check')
        # Verifica del risultato
        if ((error is None) and (risultato_ZPM4R_GL_T_FL is not None)):
            self.log_risultato_differenze("risultato_ZPM4R_GL_T_FL", risultato_ZPM4R_GL_T_FL)
        elif (error is not None):
            print(f"Si è verificato un errore nella creazione della lista: risultato_ZPM4R_GL_T_FL")
            self.log_message("Errore nella creazione della lista: risultato_ZPM4R_GL_T_FL", 'error')

        # ----------------------------------------------------
        # creo i file per eseguire l'aggiornamento delle tabelle 
        # ----------------------------------------------------        
        
        # verifico che ci siano almeno una lista che contiene un elemento

        liste_ZPMR_CONTROL_FL2 = [
            risultato_ZPMR_CONTROL_FL1_lev_1,
            risultato_ZPMR_CONTROL_FL1_lev_2
        ]

        liste_ZPMR_CONTROL_FLn = [
            risultato_ZPMR_CONTROL_FL2_lev_3,
            risultato_ZPMR_CONTROL_FL2_lev_4,
            risultato_ZPMR_CONTROL_FL2_lev_5,
            risultato_ZPMR_CONTROL_FL2_lev_6
        ]

        # ------------verifico lista liste_ZPMR_CONTROL_FL2 prima di procedere -----------------------
        # Tratta None come lista vuota (lunghezza 0)
        if any(len(lista) > 0 if lista is not None else False for lista in liste_ZPMR_CONTROL_FL2):
            self.log_message("Creo file per aggiornamento tabelle ZPMR_CONTROL_FL2", 'info') # se esiste almeno una lista contenente elementi allora creo i file
            
            # ------------creo DF per ZPMR_CONTROL_FL2-----------------------
            df, error = self.df_utils.create_df_from_lists_ZPMR_CONTROL_FL2(constants.intestazione_ZPMR_FL_2,
                                                                    liste_ZPMR_CONTROL_FL2,
                                                                    tech_code,
                                                                    country_code)
            # Verifica del risultato
            if error is None:
                print(f"Dataframe creato con successo!")
                self.log_message("DF ZPMR_CONTROL_FL2 creato correttamente!", 'success')
                # ------------salvo il DF in un file csv-----------------------
                result, error = self.df_utils.save_dataframe_to_csv(df, 
                                constants.file_ZPMR_FL_2_UpLoad)
                # Verifica del risultato
                if result is True:
                    print(f"File {constants.file_ZPMR_FL_2_UpLoad} salvato correttamente")
                    self.log_message("File ZPMR_CONTROL_FL2 creato correttamente!", 'success')
                elif error is not None:
                    print(f"Si è verificato un errore nella creazione del file: {error}")
                    self.log_message("Errore nella creazione del file ZPMR_CONTROL_FL2", 'error')
            else:
                print(f"Si è verificato un errore nella creazione del DF: {error}")
                self.log_message("Errore nella creazione del DF ZPMR_CONTROL_FL2", 'error')
            print(df)
        
        # ------------verifico lista liste_ZPMR_CONTROL_FLn prima di procedere -----------------------
        # Tratta None come lista vuota (lunghezza 0)
        if any(len(lista) > 0 if lista is not None else False for lista in liste_ZPMR_CONTROL_FLn):        
        #if any(len(lista) > 0 for lista in liste_ZPMR_CONTROL_FLn):
            self.log_message("Creo file per aggiornamento tabelle ZPMR_CONTROL_FL2", 'info') # se esiste almeno una lista contenente elementi allora creo i file
            # ------------creo DF per ZPMR_CONTROL_FLn-----------------------
            df, error = self.df_utils.create_df_from_lists_ZPMR_CONTROL_FLn(constants.intestazione_ZPMR_FL_n,
                                                            liste_ZPMR_CONTROL_FLn,
                                                            tech_code)
            # Verifica del risultato
            if error is None:
                print(f"Dataframe creato con successo!")
                self.log_message("DF ZPMR_CONTROL_FLn creato correttamente!", 'success')
                # ------------salvo il DF in un file csv-----------------------
                result, error = self.df_utils.save_dataframe_to_csv(df, 
                                constants.file_ZPMR_FL_n_UpLoad)
                # Verifica del risultato
                if result is True:
                    print(f"File {constants.file_ZPMR_FL_n_UpLoad} salvato correttamente")
                    self.log_message("File ZPMR_CONTROL_FLn creato correttamente!", 'success')
                elif error is not None:
                    print(f"Si è verificato un errore nella creazione del file: {error}")
                    self.log_message("Errore nella creazione del file ZPMR_CONTROL_FLn", 'error')
            else:
                print(f"Si è verificato un errore nella creazione del DF: {error}")
                self.log_message("Errore nella creazione del DF ZPMR_CONTROL_FLn", 'error')
            print(df)   
        
        # ------------verifico lista risultato_ZPMR_CTRL_ASS prima di procedere -----------------------            
        # Verifico che la lista contenga elementi e che non sia None
        if len(risultato_ZPMR_CTRL_ASS) > 0 if risultato_ZPMR_CTRL_ASS is not None else False:
            # creo un df a partire dalla lista 
            df, error = self.re_utils.validate_and_create_df_from_CTRL_ASS_codes(risultato_ZPMR_CTRL_ASS, 
                                                                                constants.intestazione_CTRL_ASS, 
                                                                                df_regex, 
                                                                                tech_code)
            # Verifica del risultato
            if error is None:
                print(f"Dataframe creato con successo!")
                self.log_message("DF ZPMR_CTRL_ASS creato correttamente!", 'success')
                # ------------salvo il DF in un file csv-----------------------
                result, error = self.df_utils.save_dataframe_to_csv(df, 
                                constants.file_ZPMR_CTRL_ASS_UpLoad)
                # Verifica del risultato
                if result is True:
                    print(f"File {constants.file_ZPMR_CTRL_ASS_UpLoad} salvato correttamente")
                    self.log_message("File upload ZPMR_CTRL_ASS creato correttamente!", 'success')
                elif error is not None:
                    print(f"Si è verificato un errore nella creazione del file: {error}")
                    self.log_message("Errore nella creazione del file ZPMR_CTRL_ASS", 'error')
            else:
                print(f"Si è verificato un errore nella creazione del DF: {error}")
                self.log_message("Errore nella creazione del DF ZPMR_CTRL_ASS", 'error')
            print(df)             
        
        # ------------verifico lista risultato_ZPM4R_GL_T_FL prima di procedere -----------------------            
        # Verifico che la lista contenga elementi e che non sia None
        if len(risultato_ZPM4R_GL_T_FL) > 0 if risultato_ZPM4R_GL_T_FL is not None else False:
            # creo un df a partire dalla lista 
            df, error = self.re_utils.validate_and_create_df_from_ZPM4R_GL_T_FL_codes(risultato_ZPM4R_GL_T_FL, 
                                                                                constants.intestazione_TECH_OBJ, 
                                                                                df_regex, 
                                                                                tech_code)
            # Verifica del risultato
            if error is None:
                print(f"Dataframe creato con successo!")
                self.log_message("DF ZPM4R_GL_T_FL creato correttamente!", 'success')
                # ------------salvo il DF in un file csv-----------------------
                result, error = self.df_utils.save_dataframe_to_csv(df, 
                                constants.file_ZPMR_TECH_OBJ_UpLoad)
                # Verifica del risultato
                if result is True:
                    print(f"File {constants.file_ZPMR_CTRL_ASS_UpLoad} salvato correttamente")
                    self.log_message("File upload ZPM4R_GL_T_FL creato correttamente!", 'success')
                elif error is not None:
                    print(f"Si è verificato un errore nella creazione del file: {error}")
                    self.log_message("Errore nella creazione del file ZPM4R_GL_T_FL", 'error')
            else:
                print(f"Si è verificato un errore nella creazione del DF: {error}")
                self.log_message("Errore nella creazione del DF ZPM4R_GL_T_FL", 'error')
            print(df)          

        # ----------------------------------------------------
        # ripristino il tasto di estrazione dei dati
        # ---------------------------------------------------- 
        self.extract_button.setEnabled(True)

    def upload_data(self):
        self.log_message("Funzionalità di upload non ancora implementata", 'info')

# Classe per gestire la selezione del tipo di inverter
class InverterSelectionDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Selezione Tipo di Inverter")
        self.resize(400, 200)
        
        # Layout principale
        layout = QVBoxLayout(self)
        
        # Label di istruzione
        instruction_label = QLabel("Seleziona il tipo di inverter utilizzato per questo impianto solare:")
        layout.addWidget(instruction_label)
        
        # Gruppo di radio button
        self.button_group = QButtonGroup(self)
        
        # Opzione 1: Central Inverter
        self.rb_central = QRadioButton("Central Inverter")
        self.button_group.addButton(self.rb_central, 1)
        layout.addWidget(self.rb_central)
        
        # Opzione 2: String Inverter
        self.rb_string = QRadioButton("String Inverter")
        self.button_group.addButton(self.rb_string, 2)
        layout.addWidget(self.rb_string)
        
        # Opzione 3: Inverter Module
        self.rb_module = QRadioButton("Inverter Module")
        self.button_group.addButton(self.rb_module, 3)
        layout.addWidget(self.rb_module)
        
        # Button box standard (OK/Cancel)
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
        
        # Seleziona il primo radio button di default
        self.rb_central.setChecked(True)
    
    def get_selected_inverter_type(self):
        """Restituisce il tipo di inverter selezionato"""
        id = self.button_group.checkedId()
        if id == 1:
            return "Central Inverter"
        elif id == 2:
            return "String Inverter"
        elif id == 3:
            return "Inverter Module"
        else:
            return None  # Nessuna selezione (non dovrebbe accadere)

def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()