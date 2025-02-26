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
            
            # Funzione per splittare e padare a 6 elementi
            def split_and_pad(x):
                parts = x.split('-')
                # Converte ogni elemento in stringa
                parts = [str(part).strip() for part in parts]                
                # Estende la lista a 6 elementi aggiungendo stringhe vuote
                parts.extend([''] * (6 - len(parts)))
                return pd.Series(parts[:6])
            
            # Crea le colonne numerate da 1 a 6
            df[['Livello_1', 'Livello_2', 'Livello_3', 'Livello_4', 'Livello_5', 'Livello_6']] = df['FL'].apply(split_and_pad)
            
            # Aggiunge la colonna FL_Lunghezza con il numero di elementi dopo lo split
            df['FL_Lunghezza'] = df['FL'].apply(lambda x: len(x.split('-')))
            df = self.df_utils.add_concatenated_column_FL(df, "Livello_6", "Livello_5", "Livello_4", "Livello_3", "FL_Lunghezza")
            
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

        # Costruisce il percorso relativo
        current_dir = os.path.dirname(os.path.abspath(__file__))
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
        file_tech = os.path.join(current_dir, 'Config', 'Technology.csv')
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
        if tech_code == 'E':
            # Creo una lista con i file delle guideLine da utilizzare per la tecnologia
            File_guideLine_list = [constants.file_FL_B_SubStation, constants.file_FL_Bess]

        elif tech_code == 'W':
            # Creo una lista con i file delle guideLine da utilizzare per la tecnologia
            File_guideLine_list = [constants.file_FL_W_SubStation, constants.file_FL_Wind]

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
            else:
                # L'utente ha annullato la selezione dell'inverter
                self.log_message("Selezione tipo inverter annullata", 'warning')
                self.extract_button.setEnabled(True)
                return
            
        elif tech_code == 'H':
            # Creo una lista con i file delle guideLine da utilizzare per la tecnologia
            File_guideLine_list = [constants.file_FL_B_SubStation, constants.file_FL_Bess]
        else:
            self.log_message("Errore: Tecnologia non riconosciuta", 'error')
            return

        # Genera il DataFrame con le espressioni regolari a partire dai file di regole e guideLine
        try:
            df_regex = RegularExpressionsTools.Make_DF_RE_list(constants.file_Rules, File_guideLine_list)
        except Exception as e:
            print(f"Errore durante il processing dei file: {str(e)}")

        print(df_regex)
        # salvo il df in un file csv
        df_regex.to_csv('df_re_completo.csv', index=False)

        # Verifica le FL
        try:
            result_df = RegularExpressionsTools.verifica_fl_con_regex(self.df_FL, df_regex)
        except Exception as e:
            print(f"Errore durante il processing dei file: {str(e)}")

        # salvo il df in un file csv
        result_df.to_csv('df_result_completo.csv', index=False)
        print(result_df)

        # Trova le FL non valide
        fl_non_valide = result_df[result_df['Check_Result'] != True]['FL'].tolist()

        # Mostra la lista delle FL non valide
        if fl_non_valide:
            # Mostra il numero di FL non valide
            # Mostra il numero di FL non valide con singolare o plurale
            self.log_message(f"Errore: {len(fl_non_valide)} FL non valid{'a' if len(fl_non_valide) == 1 else 'e'}", 'error')
            self.log_message(f"{"FL non valida" if len(fl_non_valide) == 1 else "Lista delle FL non valide"}:", 'warning')
            for fl in fl_non_valide:
                # Ottieni il messaggio di errore specifico per questa FL
                error_msg = result_df[result_df['FL'] == fl]['Check_Result'].values[0]
                self.log_message(f"{fl}: {error_msg}", 'error')
        else:
            self.log_message("Tutte le FL sono valide", 'success')

      
        # ----------------------------------------------------
        # estraggo i dati da SAP
        # ----------------------------------------------------
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
            self.df_utils.analyze_data(df_ZPMR_CONTROL_FL1_pivot)
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
                print("#---- df_ZPMR_CONTROL_FL2_pivot ----#")
                print(df_ZPMR_CONTROL_FL2_pivot)
            except Exception as e:
                print(f"Errore: {e}")            
            # Stampa anteprima del dataframe
            self.df_utils.analyze_data(df_ZPMR_CONTROL_FL2_pivot)             
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
                print("#---- df_ZPM4R_GL_T_FL ----#")
                print(df_ZPM4R_GL_T_FL)                
                self.df_utils.analyze_data(df_ZPM4R_GL_T_FL)
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
                print("#---- df_ZPMR_CTRL_ASS ----#")
                print(df_ZPMR_CTRL_ASS)                
                self.df_utils.analyze_data(df_ZPMR_CTRL_ASS)
        # ----------------------------------------------------
        except Exception as e:
            self.log_message(f"Creazione DF: Errore: {str(e)}", 'error')
            return  
        # ------------fine creazione DF-----------------------
        
        # ----------------------------------------------------
        # verifica degli elementi della FL nelle tabelle
        # ----------------------------------------------------        
        # verifico la presenza degli elementi del primo livello nella tabella globale
        try:
            risultato = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL1_pivot, 'Livello_1', 'Livello_1')
            self.log_risultato_differenze("Livello_1", risultato)
        except Exception as e:
            self.log_message(f"Errore: {e}", 'error')
            print(f"Errore: {e}")
        
        # verifico la presenza degli elementi del secondo livello nella tabella globale
        try:
            risultato = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL1_pivot, 'Livello_2', 'Livello_2')
            self.log_risultato_differenze("Livello_2", risultato)
        except Exception as e:
            self.log_message(f"Errore: {e}", 'error')
            print(f"Errore: {e}")
        
        # verifico la presenza degli elementi del terzo livello nella tabella globale
        try:
            risultato = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL2_pivot, 'Livello_3', 'Livello_3')
            self.log_risultato_differenze("Livello_3", risultato)
        except Exception as e:
            self.log_message(f"Errore: {e}", 'error')
            print(f"Errore: {e}")

        # verifico la presenza degli elementi del quarto livello nella tabella globale
        try:
            risultato = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL2_pivot, 'Livello_4', 'Livello_4')
            self.log_risultato_differenze("Livello_4", risultato)
        except Exception as e:
            self.log_message(f"Errore: {e}", 'error')
            print(f"Errore: {e}")

        # verifico la presenza degli elementi del quinto livello nella tabella globale
        try:
            risultato = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL2_pivot, 'Livello_5', 'Livello_5')
            self.log_risultato_differenze("Livello_5", risultato)
        except Exception as e:
            self.log_message(f"Errore: {e}", 'error')
            print(f"Errore: {e}")

        # verifico la presenza degli elementi del sesto livello nella tabella globale
        try:
            risultato = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL2_pivot, 'Livello_6', 'Livello_6')
            self.log_risultato_differenze("Livello_6", risultato)
        except Exception as e:
            self.log_message(f"Errore: {e}", 'error')
            print(f"Errore: {e}")   

        # verifico la presenza degli elementi della tabella df_ZPMR_CTRL_ASS
        try:
            risultato = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CTRL_ASS, 'Check', 'Check')
            self.log_risultato_differenze("ZPMR_CTRL_ASS", risultato)
        except Exception as e:
            self.log_message(f"Errore: {e}", 'error')
            print(f"Errore: {e}")               

        # verifico la presenza degli elementi della tabella df_ZPM4R_GL_T_FL
        try:
            risultato = self.df_utils.trova_differenze(self.df_FL, df_ZPM4R_GL_T_FL, 'Check', 'Check')
            self.log_risultato_differenze("ZPM4R_GL_T_FL", risultato)
        except Exception as e:
            self.log_message(f"Errore: {e}", 'error')
            print(f"Errore: {e}") 

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