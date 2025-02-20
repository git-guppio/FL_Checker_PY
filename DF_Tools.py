import pandas as pd
from collections import Counter
from typing import List, Dict, Optional

class DataFrameTools:
    """
    Classe di utility per la manipolazione dei DataFrame pandas
    """
    
    def __init__(self):
        pass


    @staticmethod
    def get_last_char(df, colonna):
        """
        Restituisce i primi due caratteri di una colonna contenente dati univoci
        
        Args:
            df: DataFrame da verificare
            colonna: nome della colonna in cui analizzare il dato
            
        Returns:
            none : se sono presenti solo valori nulli
            i primi due caratteri della colonna indicata
        """        
        try:
            # Prende il primo valore non nullo
            primo_valore = df[colonna].dropna().iloc[0]
            if pd.notna(primo_valore):  # verifica che non sia nullo
                return primo_valore[2:3]
            else:
                return None
        except Exception as e:
            print(f"Errore nell'elaborazione della colonna {colonna}: {str(e)}")
            return None

    @staticmethod
    def get_first_two_chars(df, colonna):
        """
        Restituisce i primi due caratteri di una colonna contenente dati univoci
        
        Args:
            df: DataFrame da verificare
            colonna: nome della colonna in cui analizzare il dato
            
        Returns:
            none : se sono presenti solo valori nulli
            i primi due caratteri della colonna indicata
        """        
        try:
            # Prende il primo valore non nullo
            primo_valore = df[colonna].dropna().iloc[0]
            if pd.notna(primo_valore):  # verifica che non sia nullo
                return primo_valore[:2]
            else:
                return None
        except Exception as e:
            print(f"Errore nell'elaborazione della colonna {colonna}: {str(e)}")
            return None
        
    @staticmethod
    def check_dataframe(df, name="DataFrame"):
        """
        Esegue un controllo completo su un DataFrame
        
        Args:
            df: DataFrame da verificare
            name: Nome del DataFrame per i messaggi di errore
            
        Returns:
            bool: True se il DataFrame è valido
        """
        try:
            # Verifica se è None
            if df is None:
                print(f"{name} è None")
                return False
                
            # Verifica se è un DataFrame
            if not isinstance(df, pd.DataFrame):
                print(f"{name} non è un DataFrame valido")
                return False
                
            # Verifica se è vuoto
            if df.empty:
                print(f"{name} è vuoto")
                return False
                
            # Verifica se ha righe e colonne
            if df.shape[0] == 0 or df.shape[1] == 0:
                print(f"{name} non ha righe o colonne")
                return False
                
            return True
            
        except Exception as e:
            print(f"Errore durante la verifica di {name}: {str(e)}")
            return False

    @staticmethod
    def clean_data(data: pd.DataFrame) -> pd.DataFrame:
        """
        Legge i dati dalla clipboard, rimuove le righe di separazione e le colonne vuote,
        e gestisce le intestazioni duplicate.
        
        Returns:
            DataFrame Pandas pulito o None in caso di errore
        """
        try:
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
            data_rows = [line.split('|') for line in clean_lines]
            
            # Prendi la prima riga come header
            original_headers = [header.strip() for header in data_rows[0]]
            
            # Gestisci gli header duplicati
            unique_headers = DataFrameTools.handle_duplicate_headers(original_headers)
            
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
    
    @staticmethod        
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
                unique_headers.append(f"{header}_{header_counts[header]}")
            else:
                # Prima occorrenza dell'header
                header_counts[header] = 0
                unique_headers.append(header)
        
        return unique_headers        
    
    @staticmethod
    def add_concatenated_column(df: pd.DataFrame, 
                            col1: str, 
                            col2: str, 
                            col3: str, 
                            col4: str,
                            new_column_name: str = 'Check',
                            separator: str = '_') -> pd.DataFrame:
        """
        Aggiunge una nuova colonna che concatena i valori di 4 colonne specificate usando un separatore
        
        Args:
            df: DataFrame di input
            col1: Nome della prima colonna -> Valore Livello
            col2: Nome della seconda colonna -> Valore Liv. Superiore
            col3: Nome della terza colonna -> Valore Liv. Superiore_1
            col4: Nome della quarta colonna -> colonna contenente la lunghezza della FL
            new_column_name: Nome della nuova colonna da creare (default: 'Check')
            separator: Carattere/i da usare come separatore (default: '_')
                
        Returns:
            DataFrame con la nuova colonna contenente i valori concatenati
        """
        # Verifica che tutte le colonne esistano nel DataFrame
        required_cols = [col1, col2, col3, col4]
        if not all(col in df.columns for col in required_cols):
            raise ValueError("Una o più colonne specificate non esistono nel DataFrame")

        def create_concatenated_value(row):
            # Verifico se sto utilizzando il DF delle FL o i DF delle estrazioni da SAP
            if (col1 == "4"): # DF FL
                # Verifica che col4 non sia nullo
                if (str(row[col4]).strip(' \t\n\r') == ""):
                    return None

                if (row[col3].strip(' \t\n\r') != ""):
                    result = f"{str(row[col3].strip(' \t\n\r'))}{separator}{str(row[col2].strip(' \t\n\r'))}{separator}{str(row[col1].strip(' \t\n\r'))}{separator}{str(str(row[col4]).strip(' \t\n\r'))}"
                    return result
                elif (row[col2].strip(' \t\n\r') != ""):
                    result = f"{str(row[col2].strip(' \t\n\r'))}{separator}{str(row[col1].strip(' \t\n\r'))}{separator}{str(str(row[col4]).strip(' \t\n\r'))}"
                    return result
                elif (row[col1].strip(' \t\n\r') != ""):
                    result = f"{str(row[col1].strip(' \t\n\r'))}{separator}{str(str(row[col4]).strip(' \t\n\r'))}"
                    return result
                else:
                    return None                
            else: # DF SAP
                # Verifica che col4 non sia nullo
                if (str(row[col4]).strip(' \t\n\r') == ""):
                    return None

                if (row[col3].strip(' \t\n\r') != ""):
                    result = f"{str(row[col1].strip(' \t\n\r'))}{separator}{str(row[col2].strip(' \t\n\r'))}{separator}{str(row[col3].strip(' \t\n\r'))}{separator}{str(str(row[col4]).strip(' \t\n\r'))}"
                    return result
                elif (row[col2].strip(' \t\n\r') != ""):
                    result = f"{str(row[col1].strip(' \t\n\r'))}{separator}{str(row[col2].strip(' \t\n\r'))}{separator}{str(str(row[col4]).strip(' \t\n\r'))}"
                    return result
                elif (row[col1].strip(' \t\n\r') != ""):
                    result = f"{str(row[col1].strip(' \t\n\r'))}{separator}{str(str(row[col4]).strip(' \t\n\r'))}"
                    return result
                else:
                    return None    
        
        df_copy = df.copy()
        df_copy[new_column_name] = df_copy.apply(create_concatenated_value, axis=1)
        return df_copy
    
    @staticmethod
    def analyze_data(df: pd.DataFrame) -> None:
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
    
    @staticmethod
    def strip_column_headers(df: pd.DataFrame) -> pd.DataFrame:
        """
        Rimuove gli spazi iniziali e finali dai nomi delle colonne
        
        Args:
            df: DataFrame di input
            
        Returns:
            DataFrame con i nomi delle colonne puliti
        """
        df_copy = df.copy()
        df_copy.columns = df_copy.columns.str.strip()
        return df_copy
    
    @staticmethod
    def combine_columns(df: pd.DataFrame, col1: str, col2: str, new_col: str, separator: str = '-') -> pd.DataFrame:
        """
        Crea una nuova colonna combinando due colonne esistenti
        
        Args:
            df: DataFrame di input
            col1: Nome della prima colonna
            col2: Nome della seconda colonna
            new_col: Nome della nuova colonna
            separator: Separatore da usare nella concatenazione
            
        Returns:
            DataFrame con la nuova colonna combinata
        """
        df_copy = df.copy()
        df_copy[new_col] = df_copy[col1] + separator + df_copy[col2]
        return df_copy