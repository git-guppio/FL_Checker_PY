import time
import win32clipboard
import pandas as pd

from collections import Counter
from typing import List, Dict, Optional
import Config.constants as constants
import os
from typing import Dict, Any, Optional


class SAPDataUpLoader:
    """ 
    Classe: SAPDataUpLoader
    Descrizione: Classe contenente i metodi per l' aggiornamento delle tabelle globali in SAP 
    """
    def __init__(self, session):
        """
        Inizializza la classe con una sessione SAP attiva
        
        Args:
            session: Oggetto sessione SAP attiva
        """
        self.session = session

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


    def is_csv_file(self,file_path):
        """
        Determina se un file è in formato CSV (Comma-Separated Values) verificando
        sia l'estensione del file sia il suo contenuto.
        
        Args:
            file_path (str): Il percorso completo del file da analizzare.
            
        Returns:
            bool: True se il file è probabilmente un CSV, False altrimenti.
                Ritorna True se:
                - L'estensione è '.csv' AND il contenuto contiene delimitatori tipici
                (virgola, punto e virgola, tab) AND il file ha più righe.
                - L'estensione è '.csv' AND si è verificato un errore nella lettura del file.
                Ritorna False se:
                - L'estensione non è '.csv'.
                - L'estensione è '.csv' ma il contenuto non ha delimitatori o non ha più righe.
                
        Raises:
            Non solleva direttamente eccezioni, ma converte gli errori di lettura
            del file in un valore di ritorno False.
        """    
        # Verifica basata sull'estensione
        if not file_path.lower().endswith('.csv'):
            return False
        
        # Verifica basata sul contenuto (optional)
        try:
            # Leggi le prime righe per verificare la struttura CSV
            with open(file_path, 'r', newline='') as f:
                start = f.read(1024)
                # Verifica se contiene virgole o altri delimitatori tipici
                has_delimiters = ',' in start or ';' in start or '\t' in start
                # Verifica se contiene più righe
                has_lines = start.count('\n') > 0
                
                return has_delimiters and has_lines
        except Exception as e:
            # In caso di errore, consideriamo il file non valido
            print(f"Errore durante la verifica del file csv {file_path}: {str(e)}")
            return False

    def analyze_file_path(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Analizza un percorso di file e restituisce un dizionario con informazioni sul percorso.
        
        Args:
            file_path (str): Il percorso del file da analizzare.
            
        Returns:
            Dict[str, Any]: Un dizionario contenente:
                - folderPath: Il percorso della cartella contenente il file
                - fileName: Il nome del file con estensione
                - isCSV: True se il file è un CSV, False altrimenti
                
        Raises:
            TypeError: Se file_path non è una stringa
            ValueError: Se il percorso è vuoto o non valido
        """
        try:
            # Verifico che l'input sia una stringa
            if not isinstance(file_path, str):
                raise TypeError("Il percorso del file deve essere una stringa")
            
            # Verifico che l'input non sia vuoto
            if not file_path or file_path.isspace():
                raise ValueError("Il percorso del file non può essere vuoto")
            
            # Normalizzo il percorso (gestisce / e \ in modo cross-platform)
            normalized_path = os.path.normpath(file_path)
            
            # Estraggo il nome del file e il percorso della cartella
            folder_path = os.path.dirname(normalized_path)
            file_name = os.path.basename(normalized_path)
            
            # Se non c'è un nome di file valido, sollevo un'eccezione
            if not file_name:
                raise ValueError("Impossibile estrarre un nome di file valido dal percorso fornito")
            
            # Verifico se il file è un CSV (controllando l'estensione)
            # Converto in minuscolo per rendere il controllo case-insensitive
            is_csv = self.is_csv_file(file_path)
            
            # Aggiungo qualche informazione extra che potrebbe essere utile
            _, file_extension = os.path.splitext(file_name)
            
            # Creo e restituisco il dizionario con i risultati
            result = {
                'folderPath': folder_path,
                'fileName': file_name,
                'isCSV': is_csv,
                # Informazioni aggiuntive opzionali
                'extension': file_extension.lower(),
                'fullPath': normalized_path,
                'fileNameWithoutExt': os.path.splitext(file_name)[0]
            }
            
            return result
            
        except Exception as e:
            # In caso di errore, consideriamo il file non valido
            print(f"Errore durante la verifica del file csv {file_path}: {str(e)}")
            return False

    def UpLoadLivello_2_SAP(self, FilePath):
        """ 
        Metodo: UpLoadLivello_2_SAP
        Descrizione: Esegue l'UpLoad in SAP del valore del secondo livello nella tabella globale
        Parametri: file contenente gli elementi da caricare
        Restituisce:
        - true se l'operazione va a buon fine
        - false altrimenti
        Esempio: UpLoadSecondoLivello_SAP("USS8")
        """           
        result = self.analyze_file_path(self,FilePath)
        if not(result):
            return False            
        print(f"Eseguo UpLoad: \n\tFolder: {result['folderPath']}\n\tFile name: {result['fileName']}\n\tCheck csv: {result['isCSV']}")

        try:
            self.session.findById("wnd[0]/tbar[0]/okcd").text = "/nZPM4R_UPL_FL_FILE"
            self.session.findById("wnd[0]").sendVKey(0)
            # attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)
            # seleziono il bottone <Tabella per 1 e 2 livello>
            self.session.findById("wnd[0]/usr/radR_BUT1").select()
            # seleziono il radio button <Con intestazione?>
            self.session.findById("wnd[0]/usr/chkP_INT").selected = True
            # apro finestra dialogo per selezione file
            self.session.findById("wnd[0]/usr/ctxtP_FILE").caretPosition = 0
            self.session.findById("wnd[0]").sendVKey(4)
            time.sleep(0.5)
            # imposto path e nome file
            self.session.findById("wnd[1]/usr/ctxtDY_PATH").text = result['folderPath']
            self.session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = result['fileName']
            self.session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 15
            self.session.findById("wnd[1]/tbar[0]/btn[0]").press()
            # eseguo upload del file
            self.session.findById("wnd[0]/tbar[1]/btn[8]").press()
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)
            return True
        
        except Exception as e:
            print(f"Errore nell'esecuzione di UpLoadLivello_2_SAP : {str(e)}")
            return False            

    def UpLoadLivello_n_SAP(self, FilePath):
        """         
        Metodo: UpLoadLivello_n_SAP
        Descrizione: Esegue l'UpLoad in SAP del valore dei livelli 3,4,5 e 6
        Parametri: file contenente gli elementi da caricare
        Restituisce:
        - true se l'operazione va a buon fine
        - false altrimenti
        Esempio: UpLoadSecondoLivello_SAP("USS8")   
        """
        result = self.analyze_file_path(FilePath)
        if not(result):
            return False            
        print(f"Eseguo UpLoad: \n\tFolder: {result['folderPath']}\n\tFile name: {result['fileName']}\n\tCheck csv: {result['isCSV']}")

        try:
            self.session.findById("wnd[0]/tbar[0]/okcd").text = "/nZPM4R_UPL_FL_FILE"
            self.session.findById("wnd[0]").sendVKey(0)
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)
            # seleziono il bottone <Tabella per 3,4,5 e 6 livello
            self.session.findById("wnd[0]/usr/radR_BUT2").select()
            # seleziono il radio button <Con intestazione?>
            self.session.findById("wnd[0]/usr/chkP_INT").selected = True
            # apro finestra dialogo per selezione file
            self.session.findById("wnd[0]/usr/ctxtP_FILE").caretPosition = 0
            self.session.findById("wnd[0]").sendVKey(4)
            time.sleep(0.5)
            # imposto path e nome file
            self.session.findById("wnd[1]/usr/ctxtDY_PATH").text = result['folderPath']
            self.session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = result['fileName']
            self.session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 15
            self.session.findById("wnd[1]/tbar[0]/btn[0]").press()
            # eseguo upload del file
            self.session.findById("wnd[0]/tbar[1]/btn[8]").press()
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)
            return True
        
        except Exception as e:
            print(f"Errore nell'esecuzione di UpLoadLivello_2_SAP : {str(e)}")
            return False  


    def UpLoadCTRL_ASS(self, FilePath):
        """         
        Metodo: UpLoadCTRL_ASS
        Descrizione: Esegue l'UpLoad in SAP del file per l'aggiornamento della tabella CTRL_ASS
        Parametri:
        Restituisce:
            - true se l'operazione va a buon fine
            - false altrimenti
        Esempio: UpLoadSecondoLivello_SAP(NomeFile)                
        """
        result = self.analyze_file_path(FilePath)
        if not(result):
            return False            
        print(f"Eseguo UpLoad: \n\tFolder: {result['folderPath']}\n\tFile name: {result['fileName']}\n\tCheck csv: {result['isCSV']}")  

        try:
            self.session.findById("wnd[0]/tbar[0]/okcd").text = "/nZPM4R_UPL_FL_FILE"
            self.session.findById("wnd[0]").sendVKey(0)
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)                  
            #self.session.findById("wnd[0]/usr/radR_BUT3").setFocus()
            self.session.findById("wnd[0]/usr/radR_BUT3").select()
            self.session.findById("wnd[0]/usr/chkP_INT").selected = True
            #self.session.findById("wnd[0]/usr/ctxtP_FILE").setFocus()
            self.session.findById("wnd[0]/usr/ctxtP_FILE").caretPosition = 0
            self.session.findById("wnd[0]").sendVKey(4)
            time.sleep(0.5)   
            self.session.findById("wnd[1]/usr/ctxtDY_PATH").text = result['folderPath']
            self.session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = result['fileName']
            self.session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 17
            self.session.findById("wnd[1]/tbar[0]/btn[0]").press()
            time.sleep(0.5)   
            # eseguo upload del file                   
            self.session.findById("wnd[0]/tbar[1]/btn[8]").press()
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5) 
            return True
        
        except Exception as e:
            print(f"Errore nell'esecuzione di UpLoadCTRL_ASS : {str(e)}")
            return False  

    def UpLoadTECH_OBJ(self, FilePath):
        """ 
        Metodo: UpLoadTECH_OBJ
        Descrizione: Esegue l'UpLoad in SAP del file per l'aggiornamento della tabella TECH_OBJ
        Parametri:
        Restituisce:
            - true se l'operazione va a buon fine
            - false altrimenti
        Esempio: UpLoadSecondoLivello_SAP(NomeFile)
        """            
        result = self.analyze_file_path(FilePath)
        if not(result):
            return False            
        print(f"Eseguo UpLoad: \n\tFolder: {result['folderPath']}\n\tFile name: {result['fileName']}\n\tCheck csv: {result['isCSV']}")   

        try:
            self.session.findById("wnd[0]/tbar[0]/okcd").text = "/nZPM4R_UPL_FL_FILE"
            self.session.findById("wnd[0]").sendVKey(0)
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)                       
            #self.session.findById("wnd[0]/usr/radR_BUT4").setFocus()
            self.session.findById("wnd[0]/usr/radR_BUT4").select()
            self.session.findById("wnd[0]/usr/chkP_INT").selected = True
            self.session.findById("wnd[0]/usr/ctxtP_FILE").setFocus()
            self.session.findById("wnd[0]/usr/ctxtP_FILE").caretPosition = 0
            self.session.findById("wnd[0]").sendVKey(4)
            time.sleep(0.5)
            self.session.findById("wnd[1]/usr/ctxtDY_PATH").text = result['folderPath']
            self.session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = result['fileName']
            self.session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 17
            self.session.findById("wnd[1]/tbar[0]/btn[0]").press()
            time.sleep(0.5)
            # eseguo upload del file 
            self.session.findById("wnd[0]/tbar[1]/btn[8]").press()
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)   
            return True
        
        except Exception as e:
            print(f"Errore nell'esecuzione di UpLoadCTRL_ASS : {str(e)}")
            return False         

# ----Fine Classe SAPDataUpLoader----------------------------------------------------------

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


    def extract_ZPMR_CONTROL_FL1(self, fltechnology: str) -> List[Dict]:
        """
        Estrae dati relativi alla tabella ZPMR_CTRL_ASS utilizzando la transazione SE16
        
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
            self.session.findById("wnd[0]/usr/ctxtDATABROWSE-TABLENAME").text = "ZPMR_CONTROL_FL1"
            self.session.findById("wnd[0]").sendVKey(0)
            time.sleep(0.5)
            self.session.findById("wnd[0]/usr/ctxtI2-LOW").text = "Z-R" + fltechnology + "M"
            self.session.findById("wnd[0]/usr/ctxtI4-LOW").text = fltechnology
            self.session.findById("wnd[0]/usr/txtMAX_SEL").text = "9999999"
            self.session.findById("wnd[0]/usr/ctxtI4-LOW").setFocus()
            self.session.findById("wnd[0]/usr/ctxtI4-LOW").caretPosition = 1
            self.session.findById("wnd[0]/tbar[1]/btn[8]").press()
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)
            self.session.findById("wnd[0]/mbar/menu[0]/menu[10]/menu[3]/menu[2]").select()
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
            print(f"Errore nell'estrazione ZPMR_CONTROL_FL1: {str(e)}")
            return False  


    def extract_ZPMR_CONTROL_FL2(self, fltechnology: str) -> List[Dict]:
        """
        Estrae dati relativi alla tabella ZPMR_CTRL_ASS utilizzando la transazione SE16
        
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
            self.session.findById("wnd[0]/usr/ctxtDATABROWSE-TABLENAME").text = "ZPMR_CONTROL_FL2"
            self.session.findById("wnd[0]").sendVKey(0)
            time.sleep(0.5)
            self.session.findById("wnd[0]/usr/ctxtI2-LOW").text = "Z-RLS" if fltechnology == "H" else "Z-R" + fltechnology + "S"
            self.session.findById("wnd[0]/usr/ctxtI4-LOW").text = fltechnology
            self.session.findById("wnd[0]/usr/txtMAX_SEL").text = "9999999"
            self.session.findById("wnd[0]/usr/ctxtI4-LOW").setFocus()
            self.session.findById("wnd[0]/usr/ctxtI4-LOW").caretPosition = 1
            self.session.findById("wnd[0]/tbar[1]/btn[8]").press()
            # Attendi che SAP sia pronto
            if not self.wait_for_sap(30):
                print(f"Timeout durante l'esecuzione della transazione")
                return False
            time.sleep(0.5)
            self.session.findById("wnd[0]/mbar/menu[0]/menu[10]/menu[3]/menu[2]").select()
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
            print(f"Errore nell'estrazione ZPMR_CONTROL_FL2: {str(e)}")
            return False
        

    def extract_ZPMR_CTRL_ASS(self, fltechnology: str) -> List[Dict]:
        """
        Estrae dati relativi alla tabella ZPMR_CTRL_ASS utilizzando la transazione SE16
        
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
            self.session.findById("wnd[0]/usr/ctxtDATABROWSE-TABLENAME").text = "ZPMR_CTRL_ASS"
            self.session.findById("wnd[0]").sendVKey(0)
            time.sleep(0.5)
            # filtro in base alla tecnologia                
            self.session.findById("wnd[0]/usr/txtI4-LOW").text = "Z-RLS" if fltechnology == "H" else "Z-R" + fltechnology + "S"
            self.session.findById("wnd[0]/usr/txtI5-LOW").text = fltechnology      
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
            print(f"Errore nell'estrazione ZPMR_CTRL_ASS: {str(e)}")
            return False        
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

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
            self.session.findById("wnd[0]/usr/ctxtI4-LOW").text = "Z-RLS" if fltechnology == "H" else "Z-R" + fltechnology + "S"
            self.session.findById("wnd[0]/usr/ctxtI5-LOW").text = fltechnology    
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
            print(f"Errore nell'estrazione ZPM4R_GL_T_FL: {str(e)}")
            return False
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

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