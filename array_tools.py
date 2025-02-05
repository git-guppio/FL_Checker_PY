import win32clipboard
import time
from typing import List, Optional

def read_clipboard_to_array(max_retries: int = 3, retry_delay: float = 0.5) -> Optional[List[str]]:
    """
    Legge il contenuto della clipboard e lo converte in un array di stringhe.
    Ogni riga della clipboard diventa un elemento dell'array.
    
    Args:
        max_retries: Numero massimo di tentativi di lettura della clipboard
        retry_delay: Ritardo in secondi tra i tentativi
        
    Returns:
        Lista di stringhe dal contenuto della clipboard, o None in caso di errore
    """
    for attempt in range(max_retries):
        try:
            # Apre la clipboard
            win32clipboard.OpenClipboard()
            
            try:
                # Verifica se c'è del testo nella clipboard
                if not win32clipboard.IsClipboardFormatAvailable(win32clipboard.CF_TEXT):
                    if not win32clipboard.IsClipboardFormatAvailable(win32clipboard.CF_UNICODETEXT):
                        print("Nessun testo trovato nella clipboard")
                        return None
                
                # Prova a leggere come testo Unicode
                try:
                    clipboard_data = win32clipboard.GetClipboardData(win32clipboard.CF_UNICODETEXT)
                except:
                    # Se fallisce, prova a leggere come testo normale
                    clipboard_data = win32clipboard.GetClipboardData(win32clipboard.CF_TEXT)
                    if isinstance(clipboard_data, bytes):
                        clipboard_data = clipboard_data.decode('utf-8', errors='replace')
                
                # Divide il testo in righe e rimuove spazi iniziali/finali
                result = [line.strip() for line in clipboard_data.splitlines() if line.strip()]
                
                return result
                
            finally:
                # Chiude sempre la clipboard
                win32clipboard.CloseClipboard()
                
        except win32clipboard.error as e:
            print(f"Errore di Windows Clipboard (tentativo {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            continue
            
        except Exception as e:
            print(f"Errore imprevisto durante la lettura della clipboard: {str(e)}")
            return None
            
    print(f"Impossibile leggere la clipboard dopo {max_retries} tentativi")
    return None

""" 
def main():
    # Esempio di utilizzo
    try:
        # Leggi dalla clipboard
        clipboard_array = read_clipboard_to_array()
        
        if clipboard_array is not None:
            print("Contenuto della clipboard:")
            for i, line in enumerate(clipboard_array, 1):
                print(f"{i}. {line}")
            
            print(f"\nTotale elementi letti: {len(clipboard_array)}")
        else:
            print("Non è stato possibile leggere dalla clipboard")
            
    except Exception as e:
        print(f"Errore generale: {str(e)}")


if __name__ == "__main__":
    main()

 """    