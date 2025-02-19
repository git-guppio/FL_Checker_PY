import time
import win32clipboard
import pandas as pd

from collections import Counter
from typing import List, Dict, Optional

class SAPDataExtractor:
    """
    Classe per eseguire estrazioni dati da SAP utilizzando una sessione esistente
    """
    
    def __init__(self, session):
        """
        Inizializza la classe con una sessione SAP attiva
        
        Args:
            session: Oggetto sessione SAP attiva
        """
        self.session = session

    def extract_ZPM4R_GL_T_FL(self, fltechnology: str) -> List[Dict]:
        """
        Estrae dati relativi alla tabella ZPM4R_GL_T_FL utilizzando la transazione SE16
        
        Args:
            fltechnology: Tecnologia ricavate dalle FL
            
        Returns:
            True se la transazione va a buon fine, False altrimenti
        """
        try:
            # Svuota la clipboard prima dell'estrazione
            win32clipboard.OpenClipboard()
            win32clipboard.EmptyClipboard()
            win32clipboard.CloseClipboard()
            # Naviga alla transazione SE16
            self.session.findById("wnd[0]/tbar[0]/okcd").text = "/nSE16"
            self.session.findById("wnd[0]").sendVKey(0)
            self.session.findById("wnd[0]/usr/ctxtDATABROWSE-TABLENAME").text = "ZPM4R_GL_T_FL"
            self.session.findById("wnd[0]").sendVKey(0)
            time.sleep(0.5)
            # filtro in base alla tecnologia                
            self.session.findById("wnd[0]/usr/ctxtI4-LOW").text = "Z-R" + fltechnology + "S"
            self.session.findById("wnd[0]/usr/ctxtI5-LOW").text = fltechnology
            # filtro in base al livello della FL
            self.session.findById("wnd[0]/usr/btn%_I6_%_APP_%-VALU_PUSH").press
            time.sleep(0.5)          
            # modifico il numero massimo di risultati
            self.session.findById("wnd[0]/usr/txtMAX_SEL").text = "9999999"
            self.session.findById("wnd[0]").sendVKey(0)
            # avvio la transazione
            self.session.findById("wnd[0]").sendVKey(8)
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)    
            # esporto i valori nella clipboard
            self.session.findById("wnd[0]/mbar/menu[0]/menu[10]/menu[3]/menu[2]").select()
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)                          
            self.session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[4,0]").select()
            self.session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[4,0]").setFocus()
            self.session.findById("wnd[1]/tbar[0]/btn[0]").press()
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)
            # Attendi che la clipboard sia riempita
            if not self.wait_for_clipboard_data(30):
                # Gestisci il caso in cui non sono stati trovati dati
                print("Nessun dato trovato nella clipboard")
                # Eventuali azioni di fallback
            # Leggo il contenuto della clipboard
            return self.clipboard_data()
            
        except Exception as e:
            print(f"Errore nell'estrazione dei materiali: {str(e)}")
            return False

    def wait_for_sap(self, timeout: int = 30):  # timeout in secondi
        """
        Attende che SAP finisca le operazioni in corso
        
        Args:
            timeout: Tempo massimo di attesa in secondi
        
        Returns:
            bool: True se SAP è diventato disponibile, False se è scaduto il timeout
        """
        start_time = time.time()
        
        try:
            while self.session.Busy:
                # Verifica timeout
                if time.time() - start_time > timeout:
                    print(f"Timeout dopo {timeout} secondi di attesa")
                    return False
                    
                time.sleep(0.5)
                print("SAP is busy")
            
            return True
            
        except Exception as e:
            print(f"Errore durante l'attesa: {str(e)}")
            return False
            
    def wait_for_clipboard_data(self, timeout: int = 30) -> bool:
        """
        Attende che la clipboard contenga dei dati
        
        Args:
            timeout: Tempo massimo di attesa in secondi
            
        Returns:
            bool: True se sono stati trovati dati, False se è scaduto il timeout
        """
        start_time = time.time()
        last_print_time = 0  # Per limitare i messaggi di log
        print_interval = 2   # Intervallo in secondi tra i messaggi di log
        
        while True:
            current_time = time.time()
            
            # Verifica timeout
            if current_time - start_time > timeout:
                print(f"Timeout: nessun dato trovato nella clipboard dopo {timeout} secondi")
                return False
            
            try:
                # Controlla il contenuto della clipboard
                win32clipboard.OpenClipboard()
                try:
                    # Verifica se c'è del testo nella clipboard
                    if win32clipboard.IsClipboardFormatAvailable(win32clipboard.CF_UNICODETEXT):
                        data = win32clipboard.GetClipboardData(win32clipboard.CF_UNICODETEXT)
                        if data and data.strip():
                            print("Dati trovati nella clipboard")
                            return True
                finally:
                    win32clipboard.CloseClipboard()
                
                # Stampa il messaggio di attesa solo ogni print_interval secondi
                if current_time - last_print_time >= print_interval:
                    print("In attesa dei dati nella clipboard...")
                    last_print_time = current_time
                
                # Aspetta prima del prossimo controllo
                time.sleep(0.1)  # Ridotto il tempo di attesa per una risposta più veloce
                
            except win32clipboard.error as we:
                print(f"Errore Windows Clipboard: {str(we)}")
                time.sleep(0.5)  # Attesa più lunga in caso di errore
                continue
            except Exception as e:
                print(f"Errore durante il controllo della clipboard: {str(e)}")
                return False  

    def clipboard_data(self) -> Optional[pd.DataFrame]:
        """
        Legge i dati dalla clipboard, rimuove le righe di separazione e le colonne vuote,
        e gestisce le intestazioni duplicate.
        
        Returns:
            DataFrame Pandas pulito o None in caso di errore
        """
        try:
            # Legge il contenuto della clipboard
            win32clipboard.OpenClipboard()
            try:
                data = win32clipboard.GetClipboardData(win32clipboard.CF_UNICODETEXT)
            finally:
                win32clipboard.CloseClipboard()

            if not data:
                print("Nessun dato trovato nella clipboard")
                return None
            else:
                 return data

        except Exception as e:
            print(f"Errore durante lettura dei dati dalla clipboard: {str(e)}")
            return None

""" 
def main():

#   Esempio di utilizzo combinato delle classi SAPGuiConnection e SAPDataExtractor

    from sap_gui_connection import SAPGuiConnection  # Importa la classe creata precedentemente
    
    try:
        # Utilizzo con context manager per la connessione
        with SAPGuiConnection() as sap:
            if sap.is_connected():
                # Ottieni la sessione
                session = sap.get_session()
                if session:
                    # Crea l'estrattore
                    extractor = SAPDataExtractor(session)
                    
                    # Estrai i materiali
                    print("Estrazione materiali...")
                    materials = extractor.extract_materials("1000")  # Plant code esempio
                    for material in materials:
                        print(f"Materiale: {material['material_code']} - {material['description']}")
                    
                    # Estrai gli ordini
                    print("\nEstrazione ordini...")
                    orders = extractor.extract_orders("20240101", "20240131")
                    for order in orders:
                        print(f"Ordine: {order['order_number']} - Cliente: {order['customer']}")
    
    except Exception as e:
        print(f"Errore generale: {str(e)}")


if __name__ == "__main__":
    main()
"""