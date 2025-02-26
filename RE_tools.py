import pandas as pd
import re
import os
from pathlib import Path

class RegularExpressionsTools:
    """
    Classe di utility per la manipolazione dei DataFrame pandas e verifica regex
    """
    
    def __init__(self):
        pass    

    @staticmethod
    def Make_DF_RE_list(rules_file_path, guideline_files_list):
        """
        Carica il file delle regole e una lista di file guideline, aggiungendo a ciascun dataframe
        due nuove colonne: FL_RE (espressioni regolari basate su Rules.csv) e 
        FL_Lunghezza (numero di elementi separati da trattini).
        
        Args:
            rules_file_path (str): Percorso al file Rules.csv
            guideline_files_list (list): Lista di percorsi ai file guideline da processare
                
        Returns:
            pandas.DataFrame: Dataframe unificato con le nuove colonne
            None: In caso di errore
        
        Raises:
            FileNotFoundError: Se i file non vengono trovati
            ValueError: Se i file non sono nel formato atteso
            Exception: Per altri errori generici
        """
        try:
            # Verifica che il percorso del file delle regole esista
            from pathlib import Path
            
            if not Path(rules_file_path).exists():
                raise FileNotFoundError(f"Il file delle regole non esiste: {rules_file_path}")
            
            # Verifica che guideline_files_list sia una lista
            if not isinstance(guideline_files_list, list):
                raise TypeError("Il parametro guideline_files_list deve essere una lista di percorsi")
            
            if len(guideline_files_list) == 0:
                raise ValueError("La lista dei file guideline è vuota")
                
            # 1. Carica il file Rules.csv e crea un dizionario di regole
            try:
                rules_df = pd.read_csv(rules_file_path, sep=';', header=0, encoding='utf-8')
                
                # Verifica che il file Rules.csv abbia almeno due colonne
                if rules_df.shape[1] < 2:
                    raise ValueError("Il file Rules.csv deve avere almeno due colonne (Code e Rules Description)")
                
                # Verifica che non ci siano valori nulli nelle prime due colonne
                if rules_df.iloc[:, 0:2].isna().any().any():
                    print("Attenzione: Il file Rules.csv contiene valori nulli nelle colonne Code o Rules Description")
                    
                # Crea il dizionario, filtrando eventuali righe con valori nulli
                valid_rules = rules_df.dropna(subset=rules_df.columns[0:2])
                rules_dict = dict(zip(valid_rules.iloc[:, 0], valid_rules.iloc[:, 1]))
                
                if not rules_dict:
                    raise ValueError("Non è stato possibile estrarre regole valide dal file Rules.csv")
                    
            except pd.errors.ParserError as e:
                raise ValueError(f"Errore nel parsing del file Rules.csv: {str(e)}")
            except Exception as e:
                raise ValueError(f"Errore nel caricamento del file Rules.csv: {str(e)}")
            
            # Dataframe per contenere i risultati finali
            combined_df = None
            
            # 2. Processa ogni file nella lista
            for guideline_file_path in guideline_files_list:
                try:
                    if not Path(guideline_file_path).exists():
                        print(f"Attenzione: Il file guideline non esiste: {guideline_file_path}, verrà saltato")
                        continue
                    
                    # Carica il file guideline
                    guideline_df = pd.read_csv(guideline_file_path, sep=';', encoding='utf-8')
                    
                    # Verifica che ci sia una colonna FL
                    if 'FL' not in guideline_df.columns:
                        print(f"Attenzione: Il file {guideline_file_path} non contiene una colonna 'FL', verrà saltato")
                        continue
                    
                    # Verifica che non ci siano valori nulli nella colonna FL
                    if guideline_df['FL'].isna().any():
                        print(f"Attenzione: La colonna FL nel file {guideline_file_path} contiene valori nulli, che verranno ignorati")
                        guideline_df = guideline_df.dropna(subset=['FL'])
                        
                    if guideline_df.empty:
                        print(f"Attenzione: Nessun dato valido trovato nel file {guideline_file_path}, verrà saltato")
                        continue
                    
                    # Aggiungi una colonna che indica il file di origine
                    guideline_df['Source_File'] = os.path.basename(guideline_file_path)
                    
                    # 3. Calcola la colonna FL_Lunghezza (numero di occorrenze di "-" + 1)
                    # Verifica che la colonna FL contenga stringhe
                    if not pd.api.types.is_string_dtype(guideline_df['FL']):
                        print(f"Attenzione: La colonna FL nel file {guideline_file_path} non contiene solo stringhe, conversione forzata a stringhe")
                        guideline_df['FL'] = guideline_df['FL'].astype(str)
                        
                    guideline_df['FL_Lunghezza'] = guideline_df['FL'].apply(lambda x: x.count('-') + 1)
                    
                    # 4. Costruisce la colonna FL_RE con le espressioni regolari
                    def create_regex(fl_value):
                        if pd.isna(fl_value) or not isinstance(fl_value, str):
                            print(f"Attenzione: Valore non valido nella colonna FL: {fl_value}")
                            return ""
                        
                        # Copia il valore originale
                        fl_regex = fl_value
                        
                        # Ordina le chiavi per lunghezza decrescente per evitare sostituzioni parziali
                        ordered_keys = sorted(rules_dict.keys(), key=len, reverse=True)
                        
                        # Sostituisci ogni codice con la corrispondente espressione regolare
                        for key in ordered_keys:
                            fl_regex = fl_regex.replace(key, rules_dict[key])
                            
                        return fl_regex
                    
                    # Applica la funzione a ogni valore della colonna FL
                    guideline_df['FL_RE'] = guideline_df['FL'].apply(create_regex)
                    
                    # Aggiungi questo dataframe al dataframe combinato
                    if combined_df is None:
                        combined_df = guideline_df
                    else:
                        combined_df = pd.concat([combined_df, guideline_df], ignore_index=True)
                    
                except pd.errors.ParserError as e:
                    print(f"Errore nel parsing del file {guideline_file_path}: {str(e)}, verrà saltato")
                    continue
                except Exception as e:
                    print(f"Errore nel caricamento del file {guideline_file_path}: {str(e)}, verrà saltato")
                    continue
            
            # Verifica se abbiamo processato almeno un file con successo
            if combined_df is None or combined_df.empty:
                raise ValueError("Nessun file guideline è stato processato con successo")
            
            # Gestione delle righe duplicate
            if combined_df is not None and not combined_df.empty:
                # Trova i duplicati prima di rimuoverli
                duplicates_df = combined_df[combined_df.duplicated(subset=['FL_RE'], keep='first')]
                
                if not duplicates_df.empty:
                    print(f"\nRimosse {len(duplicates_df)} righe duplicate:")
                    print(duplicates_df[['FL', 'FL_RE', 'Source_File']].to_string())
                
                # Rimuovi duplicati
                combined_df = combined_df.drop_duplicates(subset=['FL_RE'], keep='first')        
                
            return combined_df
                
        except FileNotFoundError as e:
            print(f"Errore: {str(e)}")
            raise
        except ValueError as e:
            print(f"Errore: {str(e)}")
            raise
        except Exception as e:
            print(f"Errore imprevisto: {str(e)}")
            raise

    @staticmethod
    def Make_DF_RE(rules_file_path, gl_file_path):
        """
        Carica il file Bess_FL_GuideLine.csv in un dataframe e aggiunge 
        due nuove colonne: FL_RE (espressioni regolari basate su Rules.csv) e 
        FL_Lunghezza (numero di elementi separati da trattini).
        
        Args:
            rules_file_path (str): Percorso al file Rules.csv
            gl_file_path (str): Percorso al file Bess_FL_GuideLine.csv
            
        Returns:
            pandas.DataFrame: Dataframe elaborato con le nuove colonne
            None: In caso di errore
        
        Raises:
            FileNotFoundError: Se i file non vengono trovati
            ValueError: Se i file non sono nel formato atteso
            Exception: Per altri errori generici
        """
        try:
            if not Path(rules_file_path).exists():
                raise FileNotFoundError(f"Il file delle regole non esiste: {rules_file_path}")
            
            if not Path(gl_file_path).exists():
                raise FileNotFoundError(f"Il file Bess FL GuideLine non esiste: {gl_file_path}")
            
            # 1. Carica il file Rules.csv e crea un dizionario di regole
            try:
                rules_df = pd.read_csv(rules_file_path, sep=';', header=0, encoding='utf-8')
                
                # Verifica che il file Rules.csv abbia almeno due colonne
                if rules_df.shape[1] < 2:
                    raise ValueError("Il file Rules.csv deve avere almeno due colonne (Code e Rules Description)")
                
                # Verifica che non ci siano valori nulli nelle prime due colonne
                if rules_df.iloc[:, 0:2].isna().any().any():
                    print("Attenzione: Il file Rules.csv contiene valori nulli nelle colonne Code o Rules Description")
                    
                # Crea il dizionario, filtrando eventuali righe con valori nulli
                valid_rules = rules_df.dropna(subset=rules_df.columns[0:2])
                rules_dict = dict(zip(valid_rules.iloc[:, 0], valid_rules.iloc[:, 1]))
                
                if not rules_dict:
                    raise ValueError("Non è stato possibile estrarre regole valide dal file Rules.csv")
                
            except pd.errors.ParserError as e:
                raise ValueError(f"Errore nel parsing del file Rules.csv: {str(e)}")
            except Exception as e:
                raise ValueError(f"Errore nel caricamento del file Rules.csv: {str(e)}")
            
            # 2. Carica il file GuideLine.csv
            try:
                re_df = pd.read_csv(gl_file_path, sep=';', encoding='utf-8')
                
                # Verifica che ci sia una colonna FL
                if 'FL' not in re_df.columns:
                    raise ValueError("Il file Bess_FL_GuideLine.csv non contiene una colonna 'FL'")
                
                # Verifica che non ci siano valori nulli nella colonna FL
                if re_df['FL'].isna().any():
                    print("Attenzione: La colonna FL contiene valori nulli, che verranno ignorati")
                    re_df = re_df.dropna(subset=['FL'])
                    
                if re_df.empty:
                    raise ValueError("Nessun dato valido trovato nel file Bess_FL_GuideLine.csv")
                
            except pd.errors.ParserError as e:
                raise ValueError(f"Errore nel parsing del file Bess_FL_GuideLine.csv: {str(e)}")
            except Exception as e:
                raise ValueError(f"Errore nel caricamento del file Bess_FL_GuideLine.csv: {str(e)}")
            
            # 3. Calcola la colonna FL_Lunghezza (numero di occorrenze di "-" + 1)
            try:
                # Verifica che la colonna FL contenga stringhe
                if not pd.api.types.is_string_dtype(re_df['FL']):
                    print("Attenzione: La colonna FL non contiene solo stringhe, conversione forzata a stringhe")
                    re_df['FL'] = re_df['FL'].astype(str)
                    
                re_df['FL_Lunghezza'] = re_df['FL'].apply(lambda x: x.count('-') + 1)
                
            except Exception as e:
                raise ValueError(f"Errore nel calcolo della colonna FL_Lunghezza: {str(e)}")
            
            # 4. Costruisce la colonna FL_RE con le espressioni regolari
            try:
                def create_regex(fl_value):
                    if pd.isna(fl_value) or not isinstance(fl_value, str):
                        print(f"Attenzione: Valore non valido nella colonna FL: {fl_value}")
                        return ""
                    
                    # Copia il valore originale
                    fl_regex = fl_value
                    
                    # Ordina le chiavi per lunghezza decrescente per evitare sostituzioni parziali
                    ordered_keys = sorted(rules_dict.keys(), key=len, reverse=True)
                    
                    # Sostituisci ogni codice con la corrispondente espressione regolare
                    for key in ordered_keys:
                        fl_regex = fl_regex.replace(key, rules_dict[key])
                        
                    return fl_regex
                
                # Applica la funzione a ogni valore della colonna FL
                re_df['FL_RE'] = re_df['FL'].apply(create_regex)
                
            except Exception as e:
                raise ValueError(f"Errore nella creazione delle espressioni regolari: {str(e)}")
            
            return re_df
            
        except FileNotFoundError as e:
            print(f"Errore: {str(e)}")
            raise
        except ValueError as e:
            print(f"Errore: {str(e)}")
            raise
        except Exception as e:
            print(f"Errore imprevisto: {str(e)}")
            raise
    
    @staticmethod
    def verifica_fl_con_regex(df_fl, df_regex):
        """
        Verifica ciascuna riga della colonna FL del primo DataFrame con le espressioni regolari
        del secondo DataFrame, controllando solo quelle con pari lunghezza.
        
        Args:
            df_fl (pd.DataFrame): DataFrame contenente una colonna 'FL' e una colonna 'FL_Lunghezza'
            df_regex (pd.DataFrame): DataFrame contenente le espressioni regolari nella colonna 'FL_RE'
                                   e la loro lunghezza nella colonna 'FL_Lunghezza'
        
        Returns:
            pd.DataFrame: DataFrame originale con una colonna aggiuntiva 'Check_Result' che contiene
                        True se la FL è valida, o il messaggio di errore se non lo è
                        
        Raises:
            ValueError: Se mancano colonne richieste nei DataFrame
            TypeError: Se i tipi di dati non sono corretti
            Exception: Per altri errori imprevisti
        """
        try:
            # Verifichiamo i tipi di input
            if not isinstance(df_fl, pd.DataFrame):
                raise TypeError("df_fl deve essere un DataFrame pandas")
            if not isinstance(df_regex, pd.DataFrame):
                raise TypeError("df_regex deve essere un DataFrame pandas")
            
            # Verifichiamo che le colonne necessarie esistano
            required_cols_fl = ['FL', 'FL_Lunghezza']
            required_cols_regex = ['FL_RE', 'FL_Lunghezza']
            
            missing_cols_fl = [col for col in required_cols_fl if col not in df_fl.columns]
            if missing_cols_fl:
                raise ValueError(f"Colonne mancanti in df_fl: {', '.join(missing_cols_fl)}")
            
            missing_cols_regex = [col for col in required_cols_regex if col not in df_regex.columns]
            if missing_cols_regex:
                raise ValueError(f"Colonne mancanti in df_regex: {', '.join(missing_cols_regex)}")
            
            # Verifichiamo che i DataFrame non siano vuoti
            if df_fl.empty:
                raise ValueError("Il DataFrame df_fl è vuoto")
            if df_regex.empty:
                raise ValueError("Il DataFrame df_regex è vuoto")
            
            # Verifichiamo che i tipi di dati siano corretti
            if not pd.api.types.is_numeric_dtype(df_fl['FL_Lunghezza']):
                raise TypeError("La colonna 'FL_Lunghezza' deve contenere valori numerici")
            if not pd.api.types.is_numeric_dtype(df_regex['FL_Lunghezza']):
                raise TypeError("La colonna 'FL_Lunghezza' deve contenere valori numerici")
            
            # Verifichiamo che le espressioni regolari siano valide
            invalid_patterns = []
            for idx, pattern in enumerate(df_regex['FL_RE']):
                try:
                    re.compile(pattern)
                except re.error as e:
                    invalid_patterns.append((pattern, str(e)))
            
            if invalid_patterns:
                error_details = "; ".join([f"'{p}': {e}" for p, e in invalid_patterns])
                raise ValueError(f"Espressioni regolari non valide: {error_details}")
                
            # Creiamo una copia del DataFrame per non modificare l'originale
            result_df = df_fl.copy()
            result_df['Check_Result'] = None
            
            # Dizionario per memorizzare le espressioni regolari compilate
            compiled_patterns = {}
            
            # Per ogni riga nel DataFrame delle FL
            for idx, row in result_df.iterrows():
                try:
                    fl_value = row['FL']
                    lunghezza = row['FL_Lunghezza']
                    
                    # Filtriamo le espressioni regolari con la stessa lunghezza
                    regex_match = df_regex[df_regex['FL_Lunghezza'] == lunghezza]
                    
                    if regex_match.empty:
                        result_df.at[idx, 'Check_Result'] = f"Nessuna espressione regolare disponibile per lunghezza {lunghezza}"
                        continue
                    
                    # Verifichiamo se la FL corrisponde ad almeno una delle espressioni regolari
                    matching_patterns_count = 0
                    matching_patterns = []

                    for _, regex_row in regex_match.iterrows():
                        pattern = regex_row['FL_RE']
                        
                        # Compiliamo l'espressione regolare solo una volta e la riutilizziamo
                        if pattern not in compiled_patterns:
                            try:
                                compiled_patterns[pattern] = re.compile(pattern)
                            except re.error as e:
                                # Questo non dovrebbe mai accadere dato il controllo iniziale
                                raise ValueError(f"Errore nella compilazione di '{pattern}': {e}")
                        
                        # Usiamo l'espressione regolare compilata e contiamo le corrispondenze
                        if compiled_patterns[pattern].fullmatch(fl_value):
                            matching_patterns_count += 1
                            matching_patterns.append(pattern)

                    # Aggiorniamo il risultato in base al numero di pattern corrispondenti
                    if matching_patterns_count == 0:
                        result_df.at[idx, 'Check_Result'] = "Non corrisponde a nessuna espressione regolare valida"
                    elif matching_patterns_count == 1:
                        result_df.at[idx, 'Check_Result'] = True
                    else:
                        # Più di un pattern corrisponde, questo è un errore
                        patterns_str = ", ".join(matching_patterns)
                        result_df.at[idx, 'Check_Result'] = f"Errore: la FL corrisponde a {matching_patterns_count} espressioni regolari: {patterns_str}"
                
                except Exception as e:
                    # Catturiamo eventuali errori durante l'elaborazione della singola riga
                    error_msg = f"Errore nell'elaborazione della FL '{row.get('FL', 'N/A')}': {str(e)}"
                    result_df.at[idx, 'Check_Result'] = error_msg
                    print(error_msg)  # Log dell'errore ma continuiamo con le altre righe
            
            return result_df
        
        except (ValueError, TypeError) as e:
            # Rilanciamo queste eccezioni per essere gestite dal chiamante
            print(f"Errore: {str(e)}")
            raise
        
        except Exception as e:
            # Cattura e rilancia altre eccezioni impreviste
            print(f"Errore imprevisto durante la validazione: {str(e)}")
            raise Exception(f"Errore imprevisto durante la validazione: {str(e)}") from e