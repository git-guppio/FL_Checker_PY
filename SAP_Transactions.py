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
            self.session.findById("wnd[0]/tbar[1]/btn[8]").press
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)    
            # esporto i valori nella clipboard
            self.session.findById("wnd[0]/mbar/menu[0]/menu[10]/menu[3]/menu[2]").select
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)                          
            self.session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[4,0]").select
            self.session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[4,0]").setFocus
            self.session.findById("wnd[1]/tbar[0]/btn[0]").press
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)
            # Attendi che la clipboard sia riempita
            self.wait_for_clipboard_data(30)
            # Leggo il contenuto della clipboard
            return self.clean_clipboard_data()
            
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
        
    def wait_for_clipboard_data(timeout: int = 30) -> bool:
        """
        Attende che la clipboard contenga dei dati
        
        Args:
            timeout: Tempo massimo di attesa in secondi
            
        Returns:
            bool: True se sono stati trovati dati, False se è scaduto il timeout
        """
        start_time = time.time()
        
        while True:
            try:
                # Verifica timeout
                if time.time() - start_time > timeout:
                    print(f"Timeout: nessun dato trovato nella clipboard dopo {timeout} secondi")
                    return False
                
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
                
                # Aspetta prima del prossimo controllo
                time.sleep(0.5)
                print("In attesa dei dati nella clipboard...")
                
            except Exception as e:
                print(f"Errore durante il controllo della clipboard: {str(e)}")
                return False     


    def handle_duplicate_headers(headers: List[str]) -> List[str]:
        """
        Gestisce le intestazioni duplicate aggiungendo un postfisso numerico
        
        Args:
            headers: Lista delle intestazioni originali
            
        Returns:
            Lista delle intestazioni con postfissi per i duplicati
        """
        # Conta le occorrenze di ogni header
        header_counts = Counter()
        unique_headers = []
        
        for header in headers:
            # Se l'header è già stato visto
            if header in header_counts:
                # Incrementa il contatore e aggiungi il postfisso
                header_counts[header] += 1
                unique_headers.append(f"{header}-{header_counts[header]}")
            else:
                # Prima occorrenza dell'header
                header_counts[header] = 0
                unique_headers.append(header)
        
        return unique_headers

    def clean_clipboard_data() -> Optional[pd.DataFrame]:
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

            # Divide in righe
            lines = data.strip().split('\n')
            
            # Filtra le righe, escludendo quelle che contengono solo trattini
            clean_lines = []
            for line in lines:
                # Rimuove spazi bianchi iniziali e finali
                line = line.strip()
                # Verifica se la riga è composta solo da trattini
                if line and not all(c == '-' for c in line.replace(' ', '')):
                    clean_lines.append(line)

            if not clean_lines:
                print("Nessuna riga valida trovata dopo la pulizia")
                return None

            # Dividi le righe in colonne usando il tab come separatore
            data_rows = [line.split('\t') for line in clean_lines]
            
            # Prendi la prima riga come header
            original_headers = data_rows[0]
            
            # Gestisci gli header duplicati
            unique_headers = handle_duplicate_headers(original_headers)
            
            # Se sono stati trovati duplicati, stampalo
            duplicates = [header for header, count in Counter(original_headers).items() if count > 1]
            if duplicates:
                print("\nTrovate colonne con nomi duplicati:")
                for dup in duplicates:
                    print(f"- '{dup}' (rinominate con postfissi numerici)")

            # Crea il DataFrame con i nuovi header
            df = pd.DataFrame(data_rows[1:], columns=unique_headers)

            # Rimuove le colonne completamente vuote
            df = df.dropna(axis=1, how='all')
            
            # Rimuove le colonne dove tutti i valori sono stringhe vuote
            df = df.loc[:, ~(df == '').all()]
            
            # Reset dell'indice
            df = df.reset_index(drop=True)
            
            return df

        except Exception as e:
            print(f"Errore durante la pulizia dei dati: {str(e)}")
            return None

    def analyze_cleaned_data(df: pd.DataFrame) -> None:
        """
        Analizza il DataFrame pulito e mostra informazioni utili
        
        Args:
            df: DataFrame da analizzare
        """
        print("\nAnalisi del DataFrame pulito:")
        print(f"Dimensioni: {df.shape}")
        print("\nColonne presenti:")
        for col in df.columns:
            print(f"- {col}")
        print("\nPrime 5 righe:")
        print(df.head())

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