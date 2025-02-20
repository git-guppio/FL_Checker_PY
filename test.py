import os
import sys
import pandas as pd
import re
import DF_Tools
import File_tools
from PyQt5.QtWidgets import (QApplication, QMainWindow, QPushButton, QVBoxLayout, 
                           QHBoxLayout, QWidget, QTextEdit, QListWidget, QLabel, QMessageBox)
import SAP_Connection
import SAP_Transactions
import DF_Tools

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        # Inizializza l'istanza di DataFrameTools come attributo della classe
        self.df_utils = DF_Tools.DataFrameTools()
        self.file_utils = File_tools.FileTools()
        self.setWindowTitle("SAP Data Manager")
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
            'info': 'ℹ',
            'error': '❌',
            'success': '✅',
            'warning': '⚠️',
            'loading': '⏳'
        }
        
        icon = icons.get(icon_type, '')
        self.log_list.addItem(f"{icon} {message}")
        self.log_list.scrollToBottom()

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
            df[['1', '2', '3', '4', '5', '6']] = df['FL'].apply(split_and_pad)
            
            # Aggiunge la colonna Lun_FL con il numero di elementi dopo lo split
            df['Lun_FL'] = df['FL'].apply(lambda x: len(x.split('-')))
            df = self.df_utils.add_concatenated_column(df, "4", "5", "6", "Lun_FL")
            
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
                self.log_message(f"  Lunghezza: {row['Lun_FL']}")
                self.log_message("---") """
            print("\nPrime 5 righe:")
            print(df.head())
            
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
        # Verifico che i dati della prima e sconda colonna siano univoci
        # ----------------------------------------------------
        if not (self.df_FL['1'].nunique()):
            self.log_message("Errore: Valori nella prima colonna non univoci", 'error')
            return
        else:
            self.log_message("Check: Valori nella prima colonna univoci", 'success')

        if not (self.df_FL['2'].nunique()):
            self.log_message("Errore: Valori nella seconda colonna non univoci", 'error')
            return
        else:
            self.log_message("Check: Valori nella seconda colonna univoci", 'success')

        # Costruisce il percorso relativo
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # ----------------------------------------------------
        # ricavo codice Country 
        # ----------------------------------------------------
        file_country = os.path.join(current_dir, 'Config', 'Country.csv')
        country_code = self.df_utils.get_first_two_chars(self.df_FL, "1")
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
        tech_code = self.df_utils.get_last_char(self.df_FL, "1")
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
        self.log_message("Avvio estrazione...")
        
        try:
            with SAP_Connection.SAPGuiConnection() as sap:
                if sap.is_connected():
                    session = sap.get_session()
                    if session:
                        self.log_message("Connessione SAP attiva", 'success')
                        extractor = SAP_Transactions.SAPDataExtractor(session)
                        
                        self.log_message("Estrazione dati tabella ZPM4R_GL_T_FL", 'loading')
                        string_ZPM4R_GL_T_FL = extractor.extract_ZPM4R_GL_T_FL("S")
                        
                        self.log_message("Estrazione dati tabella ZPMR_CTRL_ASS", 'loading')
                        string_ZPMR_CTRL_ASS = extractor.extract_ZPMR_CTRL_ASS("S")
                        
                        self.log_message("Estrazione completata con successo", 'success')
                    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------    
                        # Pulisce i nomi delle colonne
                        df_ZPM4R_GL_T_FL = self.df_utils.clean_data(string_ZPM4R_GL_T_FL)
                        # Verifica che il DataFrame sia valido
                        if not(self.df_utils.check_dataframe(df_ZPM4R_GL_T_FL, name="ZPM4R_GL_T_FL")):
                            print("Errore nella verifica del DataFrame")
                            sys.exit(1)
                        else:
                            # Aggiunge la colonna per la verifica
                            df_ZPM4R_GL_T_FL = self.df_utils.add_concatenated_column(df_ZPM4R_GL_T_FL, "Valore Livello", "Valore Liv. Superiore", "Valore Liv. Superiore_1", "Liv.Sede")
                            # Stampa anteprima del dataframe
                            self.df_utils.analyze_data(df_ZPM4R_GL_T_FL)
                    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------        
                        # Pulisce i nomi delle colonne
                        df_ZPMR_CTRL_ASS = self.df_utils.clean_data(string_ZPMR_CTRL_ASS)
                        # Verifica che il DataFrame sia valido
                        if not(self.df_utils.check_dataframe(df_ZPMR_CTRL_ASS, name="ZPMR_CTRL_ASS")):
                            print("Errore nella verifica del DataFrame")
                            sys.exit(1)
                        else:
                            # Aggiunge la colonna per la verifica
                            df_ZPMR_CTRL_ASS = self.df_utils.add_concatenated_column(df_ZPMR_CTRL_ASS, "Valore Livello", "Valore Liv. Superiore", "Valore Liv. Superiore_1", "Liv.Sede")
                            # Stampa anteprima del dataframe
                            self.df_utils.analyze_data(df_ZPMR_CTRL_ASS)
                    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------        

                        
        except Exception as e:
            self.log_message(f"Errore: {str(e)}", 'error')
            
        finally:
            self.extract_button.setEnabled(True)

    def upload_data(self):
        self.log_message("Funzionalità di upload non ancora implementata", 'info')

def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()