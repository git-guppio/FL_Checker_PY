import sys
import time
from PyQt5.QtWidgets import (QApplication, QMainWindow, QPushButton, QVBoxLayout, 
                           QWidget, QLabel, QProgressBar, QStatusBar)
from PyQt5.QtCore import QTimer, Qt

class StatusBarDemo(QMainWindow):
    def __init__(self):
        super().__init__()
        
        # Configurazione della finestra principale
        self.setWindowTitle("Status Bar Demo")
        self.setGeometry(100, 100, 600, 400)
        
        # Widget centrale
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Layout principale
        layout = QVBoxLayout(central_widget)
        
        # Etichetta informativa
        info_label = QLabel("Questo è un esempio di utilizzo della status bar in PyQt5.")
        info_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(info_label)
        
        # Pulsanti per dimostrare diverse funzionalità della status bar
        self.start_button = QPushButton("Avvia Operazione Simulata")
        self.start_button.clicked.connect(self.simulate_operation)
        layout.addWidget(self.start_button)
        
        clear_button = QPushButton("Pulisci Status Bar")
        clear_button.clicked.connect(self.clear_status)
        layout.addWidget(clear_button)
        
        # Configurazione della status bar
        self.setup_status_bar()
        
        # Timer per simulare un'operazione lunga
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_progress)
        self.progress_value = 0
    
    def setup_status_bar(self):
        """Configura la status bar con tutti i componenti"""
        # Ottieni la status bar (creata automaticamente da QMainWindow)
        self.status_bar = self.statusBar()
        
        # Imposta il messaggio iniziale
        self.status_bar.showMessage("Pronto")
        
        # Aggiungi un'etichetta permanente per lo stato
        self.status_label = QLabel("Stato: Inattivo")
        self.status_bar.addPermanentWidget(self.status_label)
        
        # Aggiungi una progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setFixedWidth(150)  # Larghezza fissa
        self.status_bar.addPermanentWidget(self.progress_bar)
    
    def update_status(self, message):
        """Aggiorna il messaggio nella status bar"""
        self.status_bar.showMessage(message)
    
    def clear_status(self):
        """Pulisce il messaggio della status bar"""
        self.status_bar.clearMessage()
        self.status_label.setText("Stato: Inattivo")
        self.progress_bar.setValue(0)
    
    def simulate_operation(self):
        """Simula un'operazione lunga con aggiornamenti della status bar"""
        self.start_button.setEnabled(False)
        self.update_status("Operazione in corso...")
        self.status_label.setText("Stato: Attivo")
        self.progress_value = 0
        self.progress_bar.setValue(0)
        self.timer.start(100)  # Aggiorna ogni 100ms
    
    def update_progress(self):
        """Aggiorna la progress bar durante l'operazione simulata"""
        self.progress_value += 1
        self.progress_bar.setValue(self.progress_value)
        
        # Aggiorna la status bar con diverse informazioni durante l'avanzamento
        if self.progress_value <= 30:
            self.update_status(f"Fase 1: Elaborazione dati... ({self.progress_value}%)")
        elif self.progress_value <= 60:
            self.update_status(f"Fase 2: Analisi in corso... ({self.progress_value}%)")
        elif self.progress_value <= 90:
            self.update_status(f"Fase 3: Finalizzazione... ({self.progress_value}%)")
        
        # Quando l'operazione è completata
        if self.progress_value >= 100:
            self.timer.stop()
            self.update_status("Operazione completata con successo")
            self.status_label.setText("Stato: Completato")
            self.start_button.setEnabled(True)

def main():
    app = QApplication(sys.argv)
    window = StatusBarDemo()
    window.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()