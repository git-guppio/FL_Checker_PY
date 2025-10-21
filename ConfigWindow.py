import json
import os
from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QCheckBox, 
                           QPushButton, QGroupBox, QMessageBox, QLabel)
from PyQt5.QtCore import Qt

class ConfigWindow(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent = parent
        
        # File di configurazione
        self.config_file = "validation_config.json"
        
        # Dizionario per memorizzare le checkbox
        self.checkboxes = {}
        
        # Configurazione predefinita
        self.default_config = {
            "Check_validazione": True,
            "Check_univoci": True,
            "Check_duplicati": True,
            "Check_country": True,
            "Check_tecnologia": True,
            "Check_mask": True,
            "Check_parent": True,
            "Check_lineeGuida": True,
            "Check_TabGlobaliSAP": True
        }
        
        # Descrizioni user-friendly per ogni check
        self.check_descriptions = {
            "Check_validazione": "Validazione con maschera generica",
            "Check_univoci": "Verifica univocità prima e seconda colonna",
            "Check_duplicati": "Verifica FL duplicate",
            "Check_country": "Verifica Country",
            "Check_tecnologia": "Verifica Tecnologia",
            "Check_mask": "Verifica maschera specifica per tecnologia",
            "Check_parent": "Verifica parent",
            "Check_lineeGuida": "Verifica linee guida",
            "Check_TabGlobaliSAP": "Verifica tabelle globali SAP"
        }
        
        self.init_ui()
        self.load_config()
    
    def init_ui(self):
        """Inizializza l'interfaccia utente"""
        self.setWindowTitle("Configurazione Parametri di Validazione")
        self.setModal(True)
        self.resize(500, 450)
        
        # Layout principale
        main_layout = QVBoxLayout(self)
        
        # Label di intestazione
        header_label = QLabel("Seleziona i controlli da eseguire durante la validazione:")
        header_label.setStyleSheet("font-weight: bold; font-size: 12px; padding: 10px;")
        main_layout.addWidget(header_label)
        
        # GroupBox per le checkbox
        group_box = QGroupBox("Opzioni di Validazione")
        group_layout = QVBoxLayout()
        
        # Crea le checkbox per ogni parametro
        for key, description in self.check_descriptions.items():
            checkbox = QCheckBox(description)
            checkbox.setObjectName(key)  # Imposta il nome oggetto per identificarla
            
            # Stile per le checkbox
            checkbox.setStyleSheet("""
                QCheckBox {
                    padding: 5px;
                    font-size: 11px;
                }
                QCheckBox::indicator {
                    width: 18px;
                    height: 18px;
                }
            """)
            
            self.checkboxes[key] = checkbox
            group_layout.addWidget(checkbox)
        
        group_box.setLayout(group_layout)
        main_layout.addWidget(group_box)
        
        # Spazio flessibile
        main_layout.addStretch()
        
        # Layout per i pulsanti di controllo globale
        control_layout = QHBoxLayout()
        
        # Pulsante Seleziona Tutto
        self.select_all_button = QPushButton("Seleziona Tutto")
        self.select_all_button.clicked.connect(self.select_all)
        control_layout.addWidget(self.select_all_button)
        
        # Pulsante Deseleziona Tutto
        self.deselect_all_button = QPushButton("Deseleziona Tutto")
        self.deselect_all_button.clicked.connect(self.deselect_all)
        control_layout.addWidget(self.deselect_all_button)
        
        # Pulsante Ripristina Default
        self.default_button = QPushButton("Ripristina Default")
        self.default_button.clicked.connect(self.restore_defaults)
        control_layout.addWidget(self.default_button)
        
        main_layout.addLayout(control_layout)
        
        # Separatore
        main_layout.addWidget(QLabel(""))
        
        # Layout per i pulsanti principali
        button_layout = QHBoxLayout()
        
        # Pulsante Salva
        self.save_button = QPushButton("💾 Salva")
        self.save_button.clicked.connect(self.save_config)
        self.save_button.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                font-weight: bold;
                padding: 8px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
        """)
        button_layout.addWidget(self.save_button)
        
        # Pulsante Annulla
        self.cancel_button = QPushButton("❌ Annulla")
        self.cancel_button.clicked.connect(self.reject)
        self.cancel_button.setStyleSheet("""
            QPushButton {
                background-color: #f44336;
                color: white;
                font-weight: bold;
                padding: 8px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #da190b;
            }
        """)
        button_layout.addWidget(self.cancel_button)
        
        main_layout.addLayout(button_layout)
    
    def load_config(self):
        """Carica la configurazione dal file JSON"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                    
                # Applica la configurazione alle checkbox
                for key, checkbox in self.checkboxes.items():
                    if key in config:
                        checkbox.setChecked(config[key])
                    else:
                        # Se la chiave non esiste, usa il valore predefinito
                        checkbox.setChecked(self.default_config.get(key, True))
                        
                self.show_message("Configurazione caricata", 
                                "Configurazione caricata correttamente dal file.", 
                                QMessageBox.Information)
            else:
                # Se il file non esiste, usa la configurazione predefinita
                self.restore_defaults()
                
        except json.JSONDecodeError:
            self.show_message("Errore", 
                            "Errore nel leggere il file di configurazione. Verranno usati i valori predefiniti.", 
                            QMessageBox.Warning)
            self.restore_defaults()
        except Exception as e:
            self.show_message("Errore", 
                            f"Errore durante il caricamento della configurazione: {str(e)}", 
                            QMessageBox.Critical)
            self.restore_defaults()
    
    def save_config(self):
        """Salva la configurazione nel file JSON e aggiorna constants.py"""
        try:
            # Crea il dizionario con lo stato attuale delle checkbox
            config = {}
            for key, checkbox in self.checkboxes.items():
                config[key] = checkbox.isChecked()
            
            # Salva nel file JSON
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=4)
            
            # Aggiorna anche il modulo constants se è disponibile
            if self.update_constants_module(config):
                self.show_message("Successo", 
                                "Configurazione salvata correttamente!\n" +
                                f"File: {self.config_file}\n" +
                                "Le modifiche saranno applicate alla prossima validazione.", 
                                QMessageBox.Information)
            else:
                self.show_message("Successo parziale", 
                                "Configurazione salvata nel file JSON.\n" +
                                "Le costanti verranno aggiornate al prossimo avvio.", 
                                QMessageBox.Warning)
            
            # Chiudi la finestra dopo il salvataggio
            self.accept()
            
        except Exception as e:
            self.show_message("Errore", 
                            f"Errore durante il salvataggio della configurazione: {str(e)}", 
                            QMessageBox.Critical)
    
    def update_constants_module(self, config):
        """Aggiorna dinamicamente il modulo constants con i nuovi valori"""
        try:
            # Importa il modulo constants se disponibile
            import Config.constants as constants
            
            # Aggiorna i valori nel modulo
            for key, value in config.items():
                if hasattr(constants, key):
                    setattr(constants, key, value)
            
            # Log nel parent se disponibile
            if self.parent and hasattr(self.parent, 'log_message'):
                self.parent.log_message("Configurazione aggiornata in memoria", 'success')
            
            return True
            
        except ImportError:
            print("Modulo constants non trovato. I valori verranno letti dal file JSON.")
            return False
        except Exception as e:
            print(f"Errore nell'aggiornamento del modulo constants: {str(e)}")
            return False
    
    def restore_defaults(self):
        """Ripristina i valori predefiniti"""
        for key, checkbox in self.checkboxes.items():
            checkbox.setChecked(self.default_config.get(key, True))
        
        self.show_message("Default ripristinati", 
                        "I valori predefiniti sono stati ripristinati.\n" +
                        "Clicca su 'Salva' per rendere permanenti le modifiche.", 
                        QMessageBox.Information)
    
    def select_all(self):
        """Seleziona tutte le checkbox"""
        for checkbox in self.checkboxes.values():
            checkbox.setChecked(True)
    
    def deselect_all(self):
        """Deseleziona tutte le checkbox"""
        for checkbox in self.checkboxes.values():
            checkbox.setChecked(False)
    
    def show_message(self, title, message, icon):
        """Mostra un messaggio all'utente"""
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle(title)
        msg_box.setText(message)
        msg_box.setIcon(icon)
        msg_box.exec_()
    
    def get_current_config(self):
        """Restituisce la configurazione attuale come dizionario"""
        config = {}
        for key, checkbox in self.checkboxes.items():
            config[key] = checkbox.isChecked()
        return config


# Funzione helper per caricare la configurazione senza GUI
def load_config_from_file(config_file="validation_config.json", default_config=None):
    """
    Carica la configurazione dal file JSON.
    
    Args:
        config_file: percorso del file di configurazione
        default_config: dizionario con i valori predefiniti
    
    Returns:
        dict: configurazione caricata dal file o valori predefiniti
    """
    if default_config is None:
        default_config = {
            "Check_validazione": True,
            "Check_univoci": True,
            "Check_duplicati": True,
            "Check_country": True,
            "Check_tecnologia": True,
            "Check_mask": True,
            "Check_parent": True,
            "Check_lineeGuida": True,
            "Check_TabGlobaliSAP": True
        }
    
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                return json.load(f)
        else:
            return default_config
    except:
        return default_config