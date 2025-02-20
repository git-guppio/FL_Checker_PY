import pandas as pd

class FileTools:
    """
    Classe di utility per la manipolazione dei file
    """
    
    def __init__(self):
        pass
    
    @staticmethod
    def trova_valore(file_csv, valore_da_cercare, colonna_da_cercare, colonna_da_restituire):
        """
        Cerca un valore in una colonna e restituisce il valore corrispondente in un'altra colonna
        
        Parametri:
        file_csv: file contenente i dati
        valore_da_cercare: il valore da trovare
        colonna_da_cercare: nome della colonna in cui cercare
        colonna_da_restituire: nome della colonna da cui restituire il valore
        """
        try:

            df = pd.read_csv(file_csv, sep=';')
            """
            # Pulisco i nomi delle colonne ed i valori da possibili spazi dovuti al caricamento del file
            df.columns = df.columns.str.strip()
            for col in df.columns:
                print(f"'{col}'")  # Le virgolette ci faranno vedere gli spazi
            df[colonna_da_cercare] = df[colonna_da_cercare].str.strip()
            """

            # Trova la riga che contiene il valore
            riga = df[df[colonna_da_cercare] == valore_da_cercare]
            
            if len(riga) == 0:
                return None  # Nessuna corrispondenza trovata
            elif len(riga) > 1:
                print("Attenzione: trovate multiple corrispondenze")
                
            # Restituisce il valore della colonna richiesta
            return riga[colonna_da_restituire].iloc[0]
            
        except Exception as e:
            print(f"Errore nella ricerca: {str(e)}")
            return None