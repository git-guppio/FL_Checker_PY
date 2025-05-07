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
                           QDialog, QRadioButton, QButtonGroup, QDialogButtonBox, QListWidgetItem, QStyle, QMenu, QAction)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QCursor
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
        self.setWindowTitle("FL Validator")
        self.setGeometry(100, 100, 1000, 600)
        self.init_ui()
        # Ottiene il percorso della directory del file Python corrente
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        # Definisco un dizionario da utilizzare per memorizzare i file di aggiornamento creati
        self.FileGenerated = {
            "Total_files": 0,
            "ZPMR_CONTROL_FL2": {"generated": False, "path": constants.file_ZPMR_FL_2_UpLoad},
            "ZPMR_CONTROL_FLn": {"generated": False, "path": constants.file_ZPMR_FL_n_UpLoad},
            "ZPMR_CTRL_ASS": {"generated": False, "path": constants.file_ZPMR_CTRL_ASS_UpLoad},
            "ZPM4R_GL_T_FL": {"generated": False, "path": constants.file_ZPMR_TECH_OBJ_UpLoad}
        }      


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

        # Attiva il menu contestuale per il widget dei log
        self.log_list.setContextMenuPolicy(Qt.CustomContextMenu)
        self.log_list.customContextMenuRequested.connect(self.show_context_menu)        
        
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
        self.extract_button = QPushButton('Verifica Dati')
        self.extract_button.clicked.connect(self.extract_data)
        button_layout.addWidget(self.extract_button)
        
        # Bottone Upload
        self.upload_button = QPushButton('Upload Dati')
        self.upload_button.clicked.connect(self.upload_data)
        self.upload_button.setEnabled(False)  # Disabilitato finché non implementato
        button_layout.addWidget(self.upload_button)
        
        # Aggiungi il layout dei bottoni al layout principale
        main_layout.addLayout(button_layout)
    
    # ----------------------------------------------------
    # Funzioni per mostrare un menu contestuale x copiare i dati
    # ----------------------------------------------------
    def show_context_menu(self, position):
        # Crea menu contestuale
        context_menu = QMenu()
        
        # Aggiungi l'azione "Copia"
        copy_action = QAction("Copia elemento", self)
        copy_action.triggered.connect(self.copy_selected_items)
        context_menu.addAction(copy_action)
        
        # Aggiungi l'azione "Copia tutto"
        copy_all_action = QAction("Copia tutto", self)
        copy_all_action.triggered.connect(self.copy_all_items)
        context_menu.addAction(copy_all_action)
        
        # Mostra il menu contestuale alla posizione corrente del cursore
        context_menu.exec_(QCursor.pos())

    def copy_selected_items(self):
        # Copia solo gli elementi selezionati
        selected_items = self.log_list.selectedItems()
        if selected_items:
            text = "\n".join(item.text() for item in selected_items)
            QApplication.clipboard().setText(text)
            print("Elementi selezionati copiati negli appunti")        

    def copy_all_items(self):
        # Copia tutti gli elementi
        all_items = []
        for i in range(self.log_list.count()):
            all_items.append(self.log_list.item(i).text())
        
        text = "\n".join(all_items)
        QApplication.clipboard().setText(text)
        print("Tutti gli elementi copiati negli appunti")        


    def log_message(self, message, icon_type='info'):
        """
        Aggiunge un messaggio al log con un'icona Qt
        """
        item = QListWidgetItem(message)
        
        # Imposta l'icona in base al tipo
        if icon_type == 'info':
            item.setIcon(self.style().standardIcon(QStyle.SP_MessageBoxInformation))
        elif icon_type == 'error':
            item.setIcon(self.style().standardIcon(QStyle.SP_MessageBoxCritical))
        elif icon_type == 'success':
            item.setIcon(self.style().standardIcon(QStyle.SP_DialogApplyButton))
        elif icon_type == 'warning':
            item.setIcon(self.style().standardIcon(QStyle.SP_MessageBoxWarning))
        elif icon_type == 'loading':
            item.setIcon(self.style().standardIcon(QStyle.SP_BrowserReload))
        
        self.log_list.addItem(item)
        self.log_list.scrollToBottom()


    """ 
        def log_message(self, message, icon_type='info'):
            
            #Aggiunge un messaggio al log con un'emoji come icona
            

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
    """
    def log_risultato_differenze(self, nomeTabella, risultato):
        try:
            if not isinstance(risultato, list):
                raise TypeError(f"L'argomento 'risultato' deve essere di tipo list, anzichè {type(risultato)}")
            if risultato is not None:
                # Il risultato contiene elementi
                num_elementi = len(risultato)
                self.log_message(nomeTabella + " - Elementi non presenti: " + str(num_elementi), 'error')
                #tabella = self.crea_tabella(risultato)
                separatore = "; "
                tabella = ""
                for i in range(0, num_elementi, 10):
                    riga = risultato[i:i+10]
                    tabella += separatore.join(riga) + "\n"            
                self.log_message(tabella, 'warning')
            else:
                # Il risultato è None
                self.log_message(nomeTabella + ": Tutti gli elementi presenti", 'success')
        
        except Exception as e:
            raise Exception(f"Errore durante la stampa dei risultati: {str(e)}")      

    def clear_windows(self):
        self.clipboard_area.clear()
        self.log_list.clear()
        self.extract_button.setEnabled(True)
        self.upload_button.setEnabled(False)
        self.log_message("Finestre pulite")

    def validate_clipboard_data(self):
        """Valida i dati nella finestra di testo sinistra (clipboard_area)"""
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
                    error_msg = (f"Errore riga {i}: la FL: {line} non rispetta le maschere FL")
                    self.log_message(error_msg, 'error')
                    QMessageBox.warning(self, "Errore di Validazione", error_msg)
                    return False                 

            except Exception as e:
                self.log_message(f"Errore nel processare la riga {i}: {str(e)}", 'error')
                return False
                
        self.log_message("Validazione dati completata con successo", 'success')
        return True  

    def validate_Mask(self, technology, dataframe, nome_colonna_fl):
        """ Valida i dati in base alla maschera specifica della tecnologia ricavata nei controlli precedenti """
        # Ottiene il nome del file che contiene le maschere di validazione
        file_Mask = constants.file_Mask
        
        # Cerca il pattern regex associato alla tecnologia specificata nel file delle maschere
        regex_pattern = self.file_utils.trova_valore(file_Mask, 
                                    valore_da_cercare=technology, 
                                    colonna_da_cercare="Tech", 
                                    colonna_da_restituire="Regex")
        
        # Verifica se è stato trovato un pattern regex per la tecnologia specificata
        if (regex_pattern == None):
            # Log di errore se non è stata trovata una maschera per la tecnologia
            self.log_message(f"Errore: Valore maschera per tecnologia {technology} non trovata", 'error')
            return
        else:
            # Cerca la maschera associata alla tecnologia specificata nel file delle maschere
            regex_mask = self.file_utils.trova_valore(file_Mask, 
                                        valore_da_cercare=technology, 
                                        colonna_da_cercare="Tech", 
                                        colonna_da_restituire="Mask")
            # Verifica se è stata trovata una maschera valida per la tecnologia specificata
            try:
                compiled_regex = re.compile(regex_pattern)
                # La regex è valida
            except re.error as e:
                self.log_message(f"Errore nella regex: {str(e)}", 'error')
                return False
            
            # Log di successo se la maschera è stata trovata
            self.log_message(f"Mask: {technology} = {regex_mask}", 'success')
        
        # Itera su tutti i valori nella colonna FL del dataframe
        for valore in dataframe[nome_colonna_fl]:
            try:
                # Verifica se il valore corrente rispetta il pattern regex
                if not compiled_regex.match(valore):
                    # Crea un messaggio di errore dettagliato
                    error_msg = (f"Errore il valore {valore} non rispetta la maschera {regex_mask}")
                    # Log dell'errore
                    self.log_message(error_msg, 'error')
                    # Mostra una finestra di dialogo all'utente con l'errore
                    QMessageBox.warning(self, "Errore di Validazione", error_msg)
                    # Interrompe la validazione al primo errore trovato
                    return False                 
            except Exception as e:
                # Gestisce eventuali eccezioni durante il processo di validazione
                self.log_message(f"Errore nel processare il valore {valore}: {str(e)}", 'error')
                # Interrompe la validazione in caso di eccezione
                return False
                
        # Se tutti i valori rispettano la maschera, log di successo
        #self.log_message("Validazione maschera completata con successo", 'success')
        # Ritorna True per indicare che la validazione è avvenuta con successo
        return True

    def create_dataframe(self):
        """Crea un DataFrame dai dati della finestra di testo sinistra (clipboard_area) e aggiunge le colonne richieste"""
        try:
            # Ottiene i dati validati dalla finestra di testo sinistra (clipboard_area)
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

    def VerificaParent(self, df):
        """
        Crea un dizionario con 6 chiavi (1-6) a partire da un DataFrame
        che contiene le colonne 'FL' e 'FL_Lunghezza'.
        Verifica anche che ogni elemento del livello n abbia un genitore nel livello n-1.
        
        Parameters:
        df (pandas.DataFrame): DataFrame con colonne 'FL' e 'FL_Lunghezza'
        
        Returns:
        list: contenente gli elementi senza Parent
        """
        # Verifica che il DataFrame contenga le colonne necessarie
        if 'FL' not in df.columns or 'FL_Lunghezza' not in df.columns:
            raise ValueError("Il DataFrame deve contenere le colonne 'FL' e 'FL_Lunghezza'")
        
        # Inizializza il dizionario con le 7 chiavi richieste
        dizionario = {
            1: [],
            2: [],
            3: [],
            4: [],
            5: [],
            6: []
        }
        
        # Prima passa: popoliamo il dizionario in base alla lunghezza FL
        for i in range(1, 7):
            filtered_df = df[df['FL_Lunghezza'] == i]
            dizionario[i] = filtered_df['FL'].tolist()
        
        # Identificare gli elementi senza genitori corretti
        parent_mancanti = []
        
        # Seconda passa: controlliamo che ogni elemento abbia un genitore corretto
        for livello in range(2, 7):  # Partiamo dal livello 2 fino al 6
            # Per ogni elemento nel livello corrente
            for elemento in dizionario[livello][:]:  # Usiamo una copia della lista con [:]
                # Troviamo il potenziale genitore rimuovendo l'ultimo segmento
                ultima_occorrenza = elemento.rfind('-')
                if ultima_occorrenza != -1:
                    potenziale_genitore = elemento[:ultima_occorrenza]
                    
                    # Verifichiamo se il genitore è presente nel livello n-1
                    if potenziale_genitore not in dizionario[livello-1]:
                        # Se non è presente, lo spostiamo in NoParent
                        self.append_unique(parent_mancanti, potenziale_genitore)
 
        return parent_mancanti
    
    # Funzione helper per aggiungere elementi univoci
    def append_unique(self, lista, elemento):
        if elemento not in lista:
            lista.append(elemento)
            return True
        return False     

    # ----------------------------------------------------
    # Routine associata al tasto <Estrai Dati>
    # ----------------------------------------------------
    def extract_data(self):

        # disabilito il tasto
        self.extract_button.setEnabled(False)
        # Inizializzo la struttura dati per la memorizzazione dei file prodotti 
        self.FileGenerated["Total_files"] = 0
        self.FileGenerated["ZPMR_CONTROL_FL2"]["generated"] = False
        self.FileGenerated["ZPMR_CONTROL_FLn"]["generated"] = False
        self.FileGenerated["ZPMR_CTRL_ASS"]["generated"] = False
        self.FileGenerated["ZPM4R_GL_T_FL"]["generated"] = False

        # ----------------------------------------------------
        # Validazione dati con maschera generica
        # ----------------------------------------------------        
        if constants.Check_validazione:
            # Prima verifica i dati nella finestra di testo sinistra (clipboard_area)
            if not self.validate_clipboard_data():
                return
            # Creo un DF con i dati contenuti nella finestra
            if not self.create_dataframe():
                return


        # ----------------------------------------------------
        # Verifico che i dati della prima e seconda colonna siano univoci
        # ----------------------------------------------------

        if constants.Check_univoci:
            # Verifica se tutti i valori sono uguali in Livello_1
            unique_values_lev1 = self.df_FL['Livello_1'].nunique()  # restituisce il numero di valori unici in una colonna.
            if unique_values_lev1 > 1:  # più di un valore unico riscontrato
                # Ci sono valori diversi
                different_values_lev1 = self.df_FL['Livello_1'].unique()
                different_values_str_lev1 = "\n".join([str(val) for val in different_values_lev1])
                
                self.log_message(f"Errore: Trovati {unique_values_lev1} valori diversi nella prima colonna.\nValori diversi:\n{different_values_str_lev1}", 'error')
                return
            else:
                # Tutti i valori sono uguali (univoci)
                self.log_message("Check: Tutti i valori nella prima colonna sono uguali", 'success')
            
            # Verifica se tutti i valori sono uguali in Livello_2, esclusi i valori con FL_Lunghezza < 1
            # Crea un subset del DataFrame con solo le righe in cui FL_Lunghezza > 1
            filtered_df = self.df_FL[self.df_FL['FL_Lunghezza'] > 1]
            # Verifica se ci sono righe che soddisfano la condizione
            if not filtered_df.empty:
                # Verifica se tutti i valori sono uguali in Livello_2 (solo per le righe filtrate)
                unique_values_lev2 = filtered_df['Livello_2'].nunique()
                if unique_values_lev2 > 1:
                    # Ci sono valori diversi
                    different_values_lev2 = filtered_df['Livello_2'].unique()
                    different_values_str_lev2 = "\n".join([str(val) for val in different_values_lev2])
                    
                    self.log_message(f"Errore: Trovati {unique_values_lev2} valori diversi nella seconda colonna.\nValori diversi:\n{different_values_str_lev2}", 'error')
                    return
                else:
                    # Tutti i valori sono uguali (univoci)
                    self.log_message("Check: Tutti i valori nella seconda colonna sono uguali", 'success')
            else:
                # Non ci sono righe con FL_Lunghezza > 1
                self.log_message("Check: Nessuna riga con FL_Lunghezza > 1, controllo sulla seconda colonna non necessario", 'info')
                return

        # ----------------------------------------------------
        # Verifico che non ci siano valori duplicati nella lista delle FL
        # ----------------------------------------------------

        if constants.Check_duplicati:
            # Verifica se ci sono valori duplicati
            if self.df_FL['FL'].duplicated().any():
                # Ci sono duplicati
                duplicate_count_FL = self.df_FL['FL'].duplicated().sum()
                
                # Trova i valori duplicati
                duplicate_values = self.df_FL['FL'][self.df_FL['FL'].duplicated(keep=False)]
                duplicate_values_unique = duplicate_values.unique()
                
                # Formatta il messaggio di errore con i valori duplicati
                duplicate_values_str = "\n".join([str(val) for val in duplicate_values_unique])
                
                self.log_message(f"Errore: Trovati {duplicate_count_FL} valori duplicati nella colonna FL.\nValori duplicati:\n{duplicate_values_str}", 'error')
                return
            else:
                # Tutti i valori sono univoci
                self.log_message("Check: Valori nella colonna FL univoci", 'success')             

        # ----------------------------------------------------
        # ricavo codice Country 
        # ----------------------------------------------------
        if constants.Check_country:
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
        if constants.Check_tecnologia:
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

        # ----------------------------------------------------    
        # verifico coerenza con la maschera della tecnolgia
        # ----------------------------------------------------
        if constants.Check_mask:
            # Verifica se la tecnologia è stata trovata
            if (description_techno == None):
                self.log_message("Errore: Valore tecnologia non trovato", 'error')
                return
            else:
                # Verifica se la maschera è valida per la tecnologia
                if not self.validate_Mask(tech_code, self.df_FL, "FL"):
                    self.log_message("Errore nella verifica maschera della tecnologia", 'error')
                    return
                else:
                    self.log_message("Check: Verifica maschera della tecnologia", 'success')

        # ----------------------------------------------------    
        # verifico i parent
        # ----------------------------------------------------
        if constants.Check_parent:        
            try:
                NoParentList = self.VerificaParent(self.df_FL)
                if not NoParentList:
                    self.log_message("Check: Verifica Parent", 'success')
                else:
                    self.log_message(f"Errore: {len(NoParentList)} Parent mancant{'e' if len(NoParentList) == 1 else 'i'}.", 'error')
                for element in NoParentList:
                    # Ottieni il messaggio di dettaglio
                    self.log_message(f"Parent: {element} mancante.", 'warning')
                    
            except Exception as e:
                print(f"Errore durante la verifica dei parent: {str(e)}")
                return 
       
        # ----------------------------------------------------    
        # verifico con linee guida in base alla tecnologia
        # ----------------------------------------------------
        if constants.Check_lineeGuida:
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
                File_guideLine_list = [constants.file_FL_Hydro]
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
                self.log_message(f"Errore: {len(fl_non_valide)} FL non valid{'a' if len(fl_non_valide) == 1 else 'e'}:", 'error')
                #self.log_message(f"{"FL non valida" if len(fl_non_valide) == 1 else "Lista delle FL non valide"}:", 'warning')
                for fl in fl_non_valide:
                    # Ottieni il messaggio di errore specifico per questa FL
                    error_msg = result_df[result_df['FL'] == fl]['Check_Result'].values[0]
                    self.log_message(f"{fl}: {error_msg}", 'warning')
                return
            else:
                self.log_message("Check: Guide Line", 'success')

      
        # ----------------------------------------------------
        # verifico tabella globali in SAP
        # ----------------------------------------------------
        if constants.Check_TabGlobaliSAP:
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
                self.log_risultato_differenze("Livello_2", risultato_ZPMR_CONTROL_FL1_lev_2)
            elif (error is not None):
                print(f"Si è verificato un errore nella creazione della lista: risultato_ZPMR_CONTROL_FL1_lev_2")
                self.log_message("Errore nella creazione della lista: risultato_ZPMR_CONTROL_FL1_lev_2", 'error')
            
            # verifico la presenza degli elementi del terzo livello nella tabella globale
            risultato_ZPMR_CONTROL_FL2_lev_3, error = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL2_pivot, 'Livello_3', 'Livello_3')
            # Verifica del risultato
            if ((error is None) and (risultato_ZPMR_CONTROL_FL2_lev_3 is not None)):
                self.log_risultato_differenze("Livello_3", risultato_ZPMR_CONTROL_FL2_lev_3)
            elif (error is not None):
                print(f"Si è verificato un errore nella creazione della lista: risultato_ZPMR_CONTROL_FL2_lev_3")
                self.log_message("Errore nella creazione della lista: risultato_ZPMR_CONTROL_FL2_lev_3", 'error')

            # verifico la presenza degli elementi del quarto livello nella tabella globale
            risultato_ZPMR_CONTROL_FL2_lev_4, error = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL2_pivot, 'Livello_4', 'Livello_4')
            # Verifica del risultato
            if ((error is None) and (risultato_ZPMR_CONTROL_FL2_lev_4 is not None)):
                self.log_risultato_differenze("Livello_4", risultato_ZPMR_CONTROL_FL2_lev_4)
            elif (error is not None):
                print(f"Si è verificato un errore nella creazione della lista: risultato_ZPMR_CONTROL_FL2_lev_4")
                self.log_message("Errore nella creazione della lista: risultato_ZPMR_CONTROL_FL2_lev_4", 'error')

            # verifico la presenza degli elementi del quinto livello nella tabella globale
            risultato_ZPMR_CONTROL_FL2_lev_5, error = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL2_pivot, 'Livello_5', 'Livello_5')
            # Verifica del risultato
            if ((error is None) and (risultato_ZPMR_CONTROL_FL2_lev_5 is not None)):
                self.log_risultato_differenze("Livello_5", risultato_ZPMR_CONTROL_FL2_lev_5)
            elif (error is not None):
                print(f"Si è verificato un errore nella creazione della lista: risultato_ZPMR_CONTROL_FL2_lev_5")
                self.log_message("Errore nella creazione della lista: risultato_ZPMR_CONTROL_FL2_lev_5", 'error')

            # verifico la presenza degli elementi del sesto livello nella tabella globale
            risultato_ZPMR_CONTROL_FL2_lev_6, error = self.df_utils.trova_differenze(self.df_FL, df_ZPMR_CONTROL_FL2_pivot, 'Livello_6', 'Livello_6')
            # Verifica del risultato
            if ((error is None) and (risultato_ZPMR_CONTROL_FL2_lev_6 is not None)):
                self.log_risultato_differenze("Livello_6", risultato_ZPMR_CONTROL_FL2_lev_6)
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
                    self.FileGenerated["Total_files"] += 1
                    self.FileGenerated["ZPMR_CONTROL_FL2"]["generated"] = True
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
                    self.FileGenerated["Total_files"] += 1
                    self.FileGenerated["ZPMR_CONTROL_FLn"]["generated"] = True
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
                        self.FileGenerated["Total_files"] += 1
                        self.FileGenerated["ZPMR_CTRL_ASS"]["generated"] = True
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
                        self.FileGenerated["Total_files"] += 1
                        self.FileGenerated["ZPM4R_GL_T_FL"]["generated"] = True
                    elif error is not None:
                        print(f"Si è verificato un errore nella creazione del file: {error}")
                        self.log_message("Errore nella creazione del file ZPM4R_GL_T_FL", 'error')
                else:
                    print(f"Si è verificato un errore nella creazione del DF: {error}")
                    self.log_message("Errore nella creazione del DF ZPM4R_GL_T_FL", 'error')
                print(df)
            
            # ------------Verifico creazione file per abilitare tasto UpLoad----------------------- 
            if (self.FileGenerated["Total_files"]>0):
                self.upload_button.setEnabled(True)
            else:
                print(f"Le tabelle SAP risultano aggiornate")
                self.log_message("Le tabelle SAP risultano aggiornate", 'success')            
                self.upload_button.setEnabled(False)

        # ----------------------------------------------------
        # Verifica completata - ripristino il tasto di estrazione dei dati
        # ---------------------------------------------------- 
        self.extract_button.setEnabled(True)

    def upload_data(self):
        # ------------Verifico che ci siano file da caricare----------------------- 
        if (self.FileGenerated["Total_files"] == 0):
            # Crea la finestra di dialogo con messaggio
            msg_box_info = QMessageBox()
            msg_box_info.setWindowTitle("Attenzione")  # Titolo della finestra
            msg_box_info.setText("Non ci sono file da caricare.")  # Testo del messaggio
            msg_box_info.setIcon(QMessageBox.Warning)  # Icona informativa
            msg_box_info.setStandardButtons(QMessageBox.Ok)  # Solo pulsante OK

            # Mostra la finestra e attendi che l'utente prema OK
            msg_box_info.exec_()
            self.upload_button.setEnabled(False)
            return
        else:
        # ----------------------------------------------------
        # Carico i file in SAP 
        # ---------------------------------------------------- 
            # ------------Creo una finestra di dialogo (yes/no) per richiedere conferma al caricamento dei file----------------------- 
            # Crea la finestra di dialogo
            msg_box_YesNo = QMessageBox()
            msg_box_YesNo.setWindowTitle("Aggiornamento Tabelle SAP")
            msg_box_YesNo.setText("Caricare i file di aggiornamento delle tabelle global SAP?")
            msg_box_YesNo.setIcon(QMessageBox.Question)

            # Imposta i pulsanti
            msg_box_YesNo.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            msg_box_YesNo.setDefaultButton(QMessageBox.No)  # Default è No per evitare click accidentali

            # Personalizza il testo dei pulsanti (opzionale)
            msg_box_YesNo.button(QMessageBox.Yes).setText("Sì")
            msg_box_YesNo.button(QMessageBox.No).setText("No")

            # Mostra la finestra di dialogo e ottieni la risposta
            response = msg_box_YesNo.exec_()

            # Verifica la risposta e procedi di conseguenza
            if (response == QMessageBox.Yes):
                # Codice per caricare i file di aggiornamento
                self.log_message("Avvio caricamento degli aggiornamenti...", 'info')
                print("Avvio caricamento degli aggiornamenti...")
                try:
                    with SAP_Connection.SAPGuiConnection() as sap:
                        if sap.is_connected():
                            session = sap.get_session()
                            if session:
                                self.log_message("Connessione SAP attiva", 'success')
                                loader = SAP_Transactions.SAPDataUpLoader(session)
                                # Itera su tutte le chiavi del dizionario
                                for key, value in self.FileGenerated.items():
                                    # Verifica se la chiave inizia con "ZPM" e che si riferisca ad un file generato
                                    if isinstance(key, str) and key.startswith("ZPM") and value["generated"]:
                                        # Verifica quale routine specifica chiamare in base al nome
                                        if key == "ZPMR_CONTROL_FL2":
                                            result = loader.UpLoadLivello_2_SAP(value["path"])
                                            # Verifica del risultato
                                            if result is True:
                                                print(f"Tabella ZPMR_CONTROL_FL2 aggiornata correttamente!")
                                                self.log_message("Tabella ZPMR_CONTROL_FL2 aggiornata correttamente!", 'success')
                                            else:
                                                print(f"Si è verificato un errore nel caricamento del file:\n\t {value["path"]}")
                                                self.log_message("Errore nel caricamento del file: ZPMR_CONTROL_FL2", 'error')
                                        elif key == "ZPMR_CONTROL_FLn":
                                            result = loader.UpLoadLivello_n_SAP(value["path"])
                                            # Verifica del risultato
                                            if result is True:
                                                print(f"Tabella ZPMR_CONTROL_FLn aggiornata correttamente!")
                                                self.log_message("Tabella ZPMR_CONTROL_FLn aggiornata correttamente!", 'success')
                                            else:
                                                print(f"Si è verificato un errore nel caricamento del file:\n\t {value["path"]}")
                                                self.log_message("Errore nel caricamento del file: ZPMR_CONTROL_FLn", 'error')
                                        elif key == "ZPMR_CTRL_ASS":
                                            result = loader.UpLoadCTRL_ASS(value["path"])
                                            # Verifica del risultato
                                            if result is True:
                                                print(f"Tabella ZPMR_CTRL_ASS aggiornata correttamente!")
                                                self.log_message("Tabella ZPMR_CTRL_ASS aggiornata correttamente!", 'success')
                                            else:
                                                print(f"Si è verificato un errore nel caricamento del file:\n\t {value["path"]}")
                                                self.log_message("Errore nel caricamento del file: ZPMR_CTRL_ASS", 'error')                                        
                                        elif key == "ZPM4R_GL_T_FL":
                                            result = loader.UpLoadTECH_OBJ(value["path"])
                                            # Verifica del risultato
                                            if result is True:
                                                print(f"Tabella ZPM4R_GL_T_FL aggiornata correttamente!")
                                                self.log_message("Tabella ZPM4R_GL_T_FL aggiornata correttamente!", 'success')
                                            else:
                                                print(f"Si è verificato un errore nel caricamento del file:\n\t {value["path"]}")
                                                self.log_message("Errore nel caricamento del file: ZPM4R_GL_T_FL", 'error')                                                  
                                        else:
                                            pass  
                                print("Aggiornamento terminato.")
                                self.log_message("Aggiornamento tabelle SAP terminato.", 'info')                                                                           
                        else:
                            self.log_message("Connessione SAP NON attiva", 'error')
                            return
                except Exception as e:
                    self.log_message(f"Caricamento aggiornamenti SAP: Errore: {str(e)}", 'error')
                    return             
            else:
                # Codice da eseguire se l'utente ha selezionato "No"
                self.log_message("Aggiornamento annullato dall'utente.", 'warning')
                print("Aggiornamento annullato dall'utente.")
                return

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