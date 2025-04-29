import pandas as pd
import re
import os
from pathlib import Path
from DF_Tools import DataFrameTools
import logging
from utils.decorators import error_logger
from typing import Dict, List, Union, Optional, Tuple

# Logger specifico per questo modulo
logger = logging.getLogger("RegularExpressionsTools")

class RegularExpressionsTools:
    """
    Classe di utility per la manipolazione dei DataFrame pandas e verifica regex
    """
    
    def __init__(self):
        pass    

    @staticmethod
    def filter_dataframe_by_regex(
                    df: pd.DataFrame,
                    regex_dict: Dict[str, List[str]],
                    column_name: str = 'FL'
                    ) -> Dict[str, pd.DataFrame]:
        """
        Filtra un DataFrame in sottoinsiemi disgiunti basati su espressioni regolari.
        
        Parameters:
        -----------
        df : pd.DataFrame
            Il DataFrame da filtrare.
        regex_dict : Dict[str, List[str]]
            Dizionario contenente le chiavi 'SubStation', 'Common' con liste
            di espressioni regolari da applicare in OR tra loro.
        column_name : str, default='FL'
            Nome della colonna su cui applicare i filtri.
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            Dizionario contenente i DataFrame filtrati con le chiavi originali più
            una chiave 'Others' per le righe che non corrispondono a nessun pattern.
            
        Raises:
        -------
        ValueError
            Se il DataFrame è vuoto o non contiene la colonna specificata.
        TypeError
            Se i parametri non sono del tipo corretto.
        KeyError
            Se il dizionario non contiene le chiavi richieste.
        """
        # Validazione input
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Il parametro 'df' deve essere un pandas DataFrame")
        
        if df.empty:
            raise ValueError("Il DataFrame non può essere vuoto")
        
        if column_name not in df.columns:
            raise ValueError(f"La colonna '{column_name}' non esiste nel DataFrame")
        
        if not isinstance(regex_dict, dict):
            raise TypeError("Il parametro 'regex_dict' deve essere un dizionario")
        
        required_keys = ['SubStation', 'Common']
        for key in required_keys:
            if key not in regex_dict:
                raise KeyError(f"La chiave '{key}' deve essere presente nel dizionario")
            if not isinstance(regex_dict[key], list):
                raise TypeError(f"Il valore per la chiave '{key}' deve essere una lista")
        
        # Inizializzo il dizionario di risultati e mappe delle maschere
        result_dict = {}
        mask_map = {}
        
        # Creo una maschera principale inizialmente tutta False
        # Questa maschera terrà traccia di tutte le righe già classificate
        master_mask = pd.Series(False, index=df.index)
        
        # Per ogni categoria nel dizionario
        for category, patterns in regex_dict.items():
            # Salto la categoria se non ci sono pattern
            if not patterns:
                result_dict[category] = pd.DataFrame(columns=df.columns)
                continue
            
            # Combino tutte le regex in OR per questa categoria
            category_mask = pd.Series(False, index=df.index)
            
            for pattern in patterns:
                try:
                    # Compilo l'espressione regolare
                    regex = re.compile(pattern)
                    # Aggiorno la maschera della categoria con OR
                    pattern_mask = df[column_name].str.contains(regex, regex=True)
                    category_mask = category_mask | pattern_mask
                except re.error:
                    print(f"Avviso: '{pattern}' non è un'espressione regolare valida, verrà ignorata")
            
            # Filtro solo le righe che non sono già state classificate
            unique_mask = category_mask & ~master_mask
            
            # Aggiorno la maschera principale
            master_mask = master_mask | unique_mask
            
            # Salvo la maschera e il DataFrame filtrato
            mask_map[category] = unique_mask
            result_dict[category] = df[unique_mask].copy()
        
        # Creo la categoria "Others" per righe non classificate
        result_dict['Others'] = df[~master_mask].copy()
        
        # Verifico che tutti i sottoinsiemi siano disgiunti e la loro unione sia il df originale
        total_rows = sum(len(subset) for subset in result_dict.values())
        if total_rows != len(df):
            print("Avviso: La somma delle righe nei sottoinsiemi non corrisponde al DataFrame originale")
        
        return result_dict

    @staticmethod
    def validate_filtering_result(
                    original_df: pd.DataFrame,
                    filtered_dict: Dict[str, pd.DataFrame]
                    ) -> Tuple[bool, Optional[str]]:
        """
        Verifica che i DataFrame filtrati siano disgiunti e la loro unione sia uguale al DataFrame originale.

        Parameters:
        -----------
        original_df : pd.DataFrame
            Il DataFrame originale.
        filtered_dict : Dict[str, pd.DataFrame]
            Dizionario con i DataFrame filtrati.
            
        Returns:
        --------
        Tuple[bool, Optional[str]]
            (True, None) se la validazione ha successo,
            (False, messaggio di errore) in caso contrario.
        """
        # Calcolo il numero totale di righe
        total_rows = sum(len(df) for df in filtered_dict.values())
        
        if total_rows != len(original_df):
            return False, f"Numero di righe non corrispondente: originale={len(original_df)}, filtrato={total_rows}"
        else:
            print("Il numero totale di righe nei sottoinsiemi corrisponde al DataFrame originale")
        
        # Ricostruisco il DataFrame unendo solo i sottoinsiemi non vuoti
        non_empty_dfs = [df for df in filtered_dict.values() if df is not None and not df.empty]
        

        """ 
        # Escludi colonne vuote o con tutti NA prima della concatenazione
        for i, df in enumerate(non_empty_dfs):
            # Identifica colonne che sono tutte NA
            all_na_cols = df.columns[df.isna().all()].tolist()
            if all_na_cols:
                # Rimuovi colonne tutte NA per preservare i dtypes
                non_empty_dfs[i] = df.drop(columns=all_na_cols)
        """        

        # Concatena i dataframe filtrati, mantenendo i tipi di dati originali
        reconstructed_df = pd.concat(non_empty_dfs, ignore_index=True) if non_empty_dfs else pd.DataFrame(columns=original_df.columns)

        print("#----------- reconstructed_df ---------#")
        print(reconstructed_df)

        # Verifico che non ci siano righe duplicate
        if len(reconstructed_df) != len(reconstructed_df.drop_duplicates()):
            return False, "Sono presenti righe duplicate nei sottoinsiemi"
        
        # Ordino i DataFrame per un confronto preciso
        sort_cols = list(original_df.columns)
        original_sorted = original_df.sort_values(sort_cols).reset_index(drop=True)
        reconstructed_sorted = reconstructed_df.sort_values(sort_cols).reset_index(drop=True)
        
        # Confronto i DataFrame
        if not original_sorted.equals(reconstructed_sorted):
            return False, "I DataFrame ricostruito e originale non sono identici"
        
        return True, None


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
                rules_df = pd.read_csv(rules_file_path, sep=';', header=0, encoding='utf-8', dtype=str)
                
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
                    guideline_df = pd.read_csv(guideline_file_path, sep=';', encoding='utf-8', dtype=str)
                    
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

            # Crea la colonna ['Check'] per la costruzione delle tabelle di aggiornamento ZPMR_CTRL_ASS e ZPM4R_GL_T_FL
            if (DataFrameTools.Add_Column_Check_ZPMR(combined_df)):
                # Applica la funzione a ogni valore della colonna FL
                combined_df['Check_RE'] = combined_df['Check'].apply(create_regex)
            else:
                print("Errore nella creazione della colonna Check nel DF guideline_df")
            # Aggiungi questo dataframe al dataframe combinato     
                
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
    @error_logger(logger=logger)
    def validate_and_create_df_from_CTRL_ASS_codes(code_list: list, 
                                        header_string: str, 
                                        regex_df: pd.DataFrame, 
                                        technology: str) -> pd.DataFrame:
        """
        Analizza una lista di codici, verifica la corrispondenza con espressioni regolari 
        e crea un DataFrame con i dati estratti.
        
        Args:
            code_list: Lista di codici da analizzare, il cui ultimo carattere specifica la lunghezza
            header_string: Stringa contenente i nomi delle colonne separati da delimitatore
            regex_df: DataFrame contenente espressioni regolari nella colonna 'Check_RE'
            technology: Codice della tecnologia
        
        Returns:
            DataFrame contenente i dati estratti dai codici validi
            
        Raises:
            ValueError: Se gli input non sono validi o se non ci sono codici validi
            TypeError: Se i tipi di dati non sono corretti
            Exception: Per altri errori imprevisti
        """
        try:
            # Verifico i tipi di input
            if not isinstance(code_list, list):
                raise TypeError("code_list deve essere una lista")
            if not isinstance(header_string, str):
                raise TypeError("header_string deve essere una stringa")
            if not isinstance(regex_df, pd.DataFrame):
                raise TypeError("regex_df deve essere un DataFrame pandas")
            if not isinstance(technology, str) or not technology.strip():
                raise ValueError("technology deve essere una stringa valida")
                
            # Verifico che gli input non siano vuoti
            if not code_list:
                raise ValueError("La lista di codici non può essere vuota")
            if not header_string.strip():
                raise ValueError("header_string non può essere vuota")
            if regex_df.empty:
                raise ValueError("Il DataFrame delle espressioni regolari è vuoto")
                
            # Verifico la presenza della colonna delle espressioni regolari
            if 'Check_RE' not in regex_df.columns:
                raise ValueError("La colonna 'Check_RE' non è presente nel DataFrame delle espressioni regolari")
            
            # Verifico la presenza delle colonne necessarie nel DataFrame regex
            required_regex_cols = ['AM Section', 'AM Part', 'AM Component']
            missing_cols = [col for col in required_regex_cols if col not in regex_df.columns]
            if missing_cols:
                raise ValueError(f"Colonne mancanti in regex_df: {', '.join(missing_cols)}")

            # Verifico la presenza della colonna FL_Lunghezza
            if 'FL_Lunghezza' not in regex_df.columns:
                # Se non esiste genero un errore
                raise ValueError("La colonna 'FL_Lunghezza' non è presente nel DataFrame delle espressioni regolari")
            
            # Parsing dell'intestazione per ottenere i nomi delle colonne
            column_names = [col.strip() for col in header_string.split(';')]
            
            # Dizionario per memorizzare le espressioni regolari compilate, organizzate per lunghezza
            # Questo ottimizza la ricerca successiva
            compiled_patterns_by_length = {}

            for _, row in regex_df.iterrows():
                pattern = row['Check_RE']

                # Estraggo la lunghezza della FL associata a questa regex
                try:
                    fl_length = row['FL_Lunghezza']
                    
                    # Converto in modo sicuro la lunghezza in intero
                    if fl_length is not None:
                        if isinstance(fl_length, (int, float)):
                            fl_length = int(fl_length)
                        elif isinstance(fl_length, str) and fl_length.strip().isdigit():
                            fl_length = int(fl_length)
                        else:
                            # Se non convertibile, uso None come chiave
                            fl_length = None
                except:
                    fl_length = None

                try:
                    regex_data = {
                        'regex': re.compile(pattern),
                        'AM Section': row['AM Section'],
                        'AM Part': row['AM Part'],
                        'AM Component': row['AM Component'],
                        'Element Type': row['Element Type']
                    }
                    # Inizializzo il dizionario per questa lunghezza se non esiste
                    if fl_length not in compiled_patterns_by_length:
                        compiled_patterns_by_length[fl_length] = {}
                        
                    # Aggiungo il pattern al dizionario per questa lunghezza
                    compiled_patterns_by_length[fl_length][pattern] = regex_data
                
                except re.error as e:
                    raise ValueError(f"Espressione regolare non valida '{pattern}': {str(e)}")

            # Lista per raccogliere le righe valide
            valid_rows = []
            
            # Analizzo ogni codice nella lista
            for code in code_list:
                # Verifico che il codice sia valido
                if not code or len(code) < 1:
                    continue
                    
                # Estraggo l'ultimo carattere che indica la lunghezza della FL
                fl_length_char = code[-1]
                
                # Verifico che sia un numero
                if not fl_length_char.isdigit():
                    raise ValueError(f"La lunghezza {fl_length_char} non corrisponde a un valore numerico valido")
                
                # Converto in intero
                fl_length = int(fl_length_char)
                
                # Verifico se il codice corrisponde a qualche espressione regolare
                # Usando solo quelle con la lunghezza corrispondente
                matching_patterns = []
                matching_pattern_data = {}
                
                # Ottengo il sottoinsieme di pattern per questa lunghezza
                patterns_for_length = compiled_patterns_by_length.get(fl_length, {})
                
                # Se non ci sono pattern per questa lunghezza, provo con tutte le regex
                if not patterns_for_length:
                    # Cerco in tutti i pattern (fallback)
                    for length, patterns in compiled_patterns_by_length.items():
                        for pattern, data in patterns.items():
                            if data['regex'].fullmatch(code):
                                matching_patterns.append(pattern)
                                matching_pattern_data = data
                else:
                    # Verifico solo i pattern con la lunghezza corrispondente
                    for pattern, data in patterns_for_length.items():
                        if data['regex'].fullmatch(code):
                            matching_patterns.append(pattern)
                            matching_pattern_data = data
                
                # Verifico i risultati della corrispondenza
                if not matching_patterns:
                    raise ValueError(f"Il codice {code} non corrisponde a nessuna espressione regolare")
                elif len(matching_patterns) > 1:
                    raise ValueError(f"Il codice {code} corrisponde a più espressioni regolari: {', '.join(matching_patterns)}")
                
                # Split del codice in base al separatore "_"
                code_parts = code.split('_')
                
                # Creo una nuova riga con valori None
                new_row = {col: None for col in column_names}
                
                # Imposto i valori in base al numero di parti
                if ((fl_length >= 3) and (fl_length <= 6)):
                    new_row['VALUE'] = code_parts[0]
                    new_row['SUB_VALUE'] = "" if fl_length == 3 else code_parts[1]
                    new_row['SUB_VALUE2'] = "" if fl_length == 4 else code_parts[2]                
                    
                    # Imposto i valori fissi
                    new_row['TPLKZ'] = "Z-RS" + technology
                    new_row['FLTYP'] = technology
                    
                    # Imposto lunghezza della FL
                    new_row['FLLEVEL'] = fl_length

                    # Imposto i valori dal dizionario dell'espressione regolare
                    new_row['CODE_SEZ_PM'] = matching_pattern_data['AM Section']
                    new_row['CODE_SIST'] = matching_pattern_data['AM Part']
                    new_row['CODE_PARTE'] = matching_pattern_data['AM Component']
                    new_row['TIPO_ELEM'] = matching_pattern_data.get('Element Type', '')  # Uso get per gestire colonne mancanti
                    
                    valid_rows.append(new_row)
            
            # Se non ci sono righe valide, genero un errore
            if not valid_rows:
                raise ValueError("Nessun codice valido trovato")
            
            # Creo il DataFrame finale in modo efficiente, specificando le colonne
            result_df = pd.DataFrame(valid_rows, columns=column_names)
                
            return result_df
                
        except Exception as e:
            # Rilanciamo l'eccezione con contesto aggiuntivo
            raise Exception(f"Errore nell'analisi dei codici e creazione del DataFrame: {str(e)}") from e
    
    @staticmethod
    @error_logger(logger=logger)
    def validate_and_create_df_from_ZPM4R_GL_T_FL_codes(code_list: list,
                                        header_string: str, 
                                        regex_df: pd.DataFrame, 
                                        technology: str) -> pd.DataFrame:
        """
        Analizza una lista di codici, verifica la corrispondenza con espressioni regolari 
        e crea un DataFrame con i dati estratti.
        
        Args:
            code_list: Lista di codici da analizzare, il cui ultimo carattere specifica la lunghezza
            header_string: Stringa contenente i nomi delle colonne separati da delimitatore
            regex_df: DataFrame contenente espressioni regolari nella colonna 'Check_RE'
            technology: Codice della tecnologia
        
        Returns:
            DataFrame contenente i dati estratti dai codici validi
            
        Raises:
            ValueError: Se gli input non sono validi o se non ci sono codici validi
            TypeError: Se i tipi di dati non sono corretti
            Exception: Per altri errori imprevisti
        """
        try:
            # Verifico i tipi di input
            if not isinstance(code_list, list):
                raise TypeError("code_list deve essere una lista")
            if not isinstance(header_string, str):
                raise TypeError("header_string deve essere una stringa")
            if not isinstance(regex_df, pd.DataFrame):
                raise TypeError("regex_df deve essere un DataFrame pandas")
            if not isinstance(technology, str) or not technology.strip():
                raise ValueError("technology deve essere una stringa valida")
                
            # Verifico che gli input non siano vuoti
            if not code_list:
                raise ValueError("La lista di codici non può essere vuota")
            if not header_string.strip():
                raise ValueError("header_string non può essere vuota")
            if regex_df.empty:
                raise ValueError("Il DataFrame delle espressioni regolari è vuoto")
                
            # Verifico la presenza della colonna delle espressioni regolari
            if 'Check_RE' not in regex_df.columns:
                raise ValueError("La colonna 'Check_RE' non è presente nel DataFrame delle espressioni regolari")
            
            # Verifico la presenza delle colonne necessarie nel DataFrame regex
            required_regex_cols = ['AM Section', 'AM Part', 'AM Component']
            missing_cols = [col for col in required_regex_cols if col not in regex_df.columns]
            if missing_cols:
                raise ValueError(f"Colonne mancanti in regex_df: {', '.join(missing_cols)}")

            # Verifico la presenza della colonna FL_Lunghezza
            if 'FL_Lunghezza' not in regex_df.columns:
                # Se non esiste genero un errore
                raise ValueError("La colonna 'FL_Lunghezza' non è presente nel DataFrame delle espressioni regolari")
            
            # Parsing dell'intestazione per ottenere i nomi delle colonne
            column_names = [col.strip() for col in header_string.split(';')]
                        
            # Dizionario per memorizzare le espressioni regolari compilate, organizzate per lunghezza
            # Questo ottimizza la ricerca successiva
            compiled_patterns_by_length = {}

            for _, row in regex_df.iterrows():
                pattern = row['Check_RE']

                # Estraggo la lunghezza della FL associata a questa regex
                try:
                    fl_length = row['FL_Lunghezza']
                    
                    # Converto in modo sicuro la lunghezza in intero
                    if fl_length is not None:
                        if isinstance(fl_length, (int, float)):
                            fl_length = int(fl_length)
                        elif isinstance(fl_length, str) and fl_length.strip().isdigit():
                            fl_length = int(fl_length)
                        else:
                            # Se non convertibile, uso None come chiave
                            fl_length = None
                except:
                    fl_length = None

                try:
                    regex_data = {
                        'regex': re.compile(pattern),
                        'Tech.Obj.SAP CODE': row['Tech.Obj.SAP CODE'],
                        'Catalog Profile': row['Catalog Profile']
                    }
                    # Inizializzo il dizionario per questa lunghezza se non esiste
                    if fl_length not in compiled_patterns_by_length:
                        compiled_patterns_by_length[fl_length] = {}
                        
                    # Aggiungo il pattern al dizionario per questa lunghezza
                    compiled_patterns_by_length[fl_length][pattern] = regex_data
                
                except re.error as e:
                    raise ValueError(f"Espressione regolare non valida '{pattern}': {str(e)}")

            # Lista per raccogliere le righe valide
            valid_rows = []
            
            # Analizzo ogni codice nella lista
            for code in code_list:
                # Verifico che il codice sia valido
                if not code or len(code) < 1:
                    continue
                    
                # Estraggo l'ultimo carattere che indica la lunghezza della FL
                fl_length_char = code[-1]
                
                # Verifico che sia un numero
                if not fl_length_char.isdigit():
                    raise ValueError(f"La lunghezza {fl_length_char} non corrisponde a un valore numerico valido")
                
                # Converto in intero
                fl_length = int(fl_length_char)
                
                # Verifico se il codice corrisponde a qualche espressione regolare
                # Usando solo quelle con la lunghezza corrispondente
                matching_patterns = []
                matching_pattern_data = {}
                
                # Ottengo il sottoinsieme di pattern per questa lunghezza
                patterns_for_length = compiled_patterns_by_length.get(fl_length, {})
                
                # Se non ci sono pattern per questa lunghezza, provo con tutte le regex
                if not patterns_for_length:
                    # Cerco in tutti i pattern (fallback)
                    for length, patterns in compiled_patterns_by_length.items():
                        for pattern, data in patterns.items():
                            if data['regex'].fullmatch(code):
                                matching_patterns.append(pattern)
                                matching_pattern_data = data
                else:
                    # Verifico solo i pattern con la lunghezza corrispondente
                    for pattern, data in patterns_for_length.items():
                        if data['regex'].fullmatch(code):
                            matching_patterns.append(pattern)
                            matching_pattern_data = data
                
                # Verifico i risultati della corrispondenza
                if not matching_patterns:
                    raise ValueError(f"Il codice {code} non corrisponde a nessuna espressione regolare")
                elif len(matching_patterns) > 1:
                    raise ValueError(f"Il codice {code} corrisponde a più espressioni regolari: {', '.join(matching_patterns)}")
                
                # Split del codice in base al separatore "_"
                code_parts = code.split('_')
                
                # Creo una nuova riga con valori None
                new_row = {col: None for col in column_names}
                
                # Imposto i valori in base al numero di parti
                if ((fl_length >= 3) and (fl_length <= 6)):
                    new_row['VALUE'] = code_parts[0]
                    new_row['SUB_VALUE'] = "" if fl_length == 3 else code_parts[1]
                    new_row['SUB_VALUE2'] = "" if fl_length == 4 else code_parts[2]
                    
                    # Imposto i valori fissi
                    new_row['TPLKZ'] = "Z-RS" + technology
                    new_row['FLTYP'] = technology
                    
                    # Imposto lunghezza della FL
                    new_row['FLLEVEL'] = fl_length

                    # Imposto i valori dal dizionario dell'espressione regolare
                    new_row['EQART'] = matching_pattern_data['Tech.Obj.SAP CODE']
                    new_row['RBNR'] = matching_pattern_data['Catalog Profile']
                    
                    valid_rows.append(new_row)
            
            # Se non ci sono righe valide, genero un errore
            if not valid_rows:
                raise ValueError("Nessun codice valido trovato")
            
            # Creo il DataFrame finale in modo efficiente, specificando le colonne
            result_df = pd.DataFrame(valid_rows, columns=column_names)
                
            return result_df
                
        except Exception as e:
            # Rilanciamo l'eccezione con contesto aggiuntivo
            raise Exception(f"Errore nell'analisi dei codici e creazione del DataFrame: {str(e)}") from e

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
    
    @staticmethod
    def verifica_fl_con_regex_per_categorie(
        df_fl_completo: pd.DataFrame,
        df_regex_completo: pd.DataFrame,
        categorie_dict: Dict[str, List[str]],
        colonna_fl: str = 'FL'
    ) -> pd.DataFrame:
        """
        Applica la funzione verifica_fl_con_regex a ciascuna categoria di FL
        definita dal dizionario di espressioni regolari, e unisce i risultati.
        
        Args:
            df_fl_completo (pd.DataFrame): DataFrame contenente le FL da verificare
            df_regex_completo (pd.DataFrame): DataFrame contenente le espressioni regolari
            categorie_dict (Dict[str, List[str]]): Dizionario con le categorie e le relative regex
            colonna_fl (str): Nome della colonna contenente le FL
            
        Returns:
            pd.DataFrame: DataFrame unificato con i risultati della verifica
        """
        # Verifica degli input
        if not isinstance(df_fl_completo, pd.DataFrame) or df_fl_completo.empty:
            raise ValueError("df_fl_completo deve essere un DataFrame pandas non vuoto")
        
        if not isinstance(df_regex_completo, pd.DataFrame) or df_regex_completo.empty:
            raise ValueError("df_regex_completo deve essere un DataFrame pandas non vuoto")
        
        if not isinstance(categorie_dict, dict) or not categorie_dict:
            raise ValueError("categorie_dict deve essere un dizionario non vuoto")
        
        if colonna_fl not in df_fl_completo.columns:
            raise ValueError(f"La colonna '{colonna_fl}' non esiste nel DataFrame delle FL")
        
        # Verifica che la colonna FL_lunghezza esista, altrimenti genero un errore
        if 'FL_Lunghezza' not in df_fl_completo.columns:
            raise ValueError("La colonna 'FL_Lunghezza' non esiste nel DataFrame delle FL")
        
        if 'FL_Lunghezza' not in df_regex_completo.columns:
            raise ValueError("La colonna 'FL_Lunghezza' non esiste nel DataFrame delle RegEx")
        
        # Assicuriamoci che i tipi siano corretti
        df_fl_completo['FL_Lunghezza'] = df_fl_completo['FL_Lunghezza'].astype(int)
        df_regex_completo['FL_Lunghezza'] = df_regex_completo['FL_Lunghezza'].astype(int)
        
        # Inizializziamo il risultato
        risultati_categorie = {}
        
        # Eseguiamo la funzione filter_dataframe_by_regex per ottenere i sottoinsiemi
        df_per_categoria = RegularExpressionsTools.filter_dataframe_by_regex(df_fl_completo, categorie_dict, colonna_fl)
        
        # Filtriamo anche il DataFrame delle espressioni regolari per categoria
        df_regex_per_categoria = RegularExpressionsTools.filter_dataframe_by_regex(df_regex_completo, categorie_dict)
        
        # Eseguiamo la verifica per ogni categoria
        for categoria in df_per_categoria.keys():
            print(f"Elaborazione categoria: {categoria}")
            
            # Otteniamo i DataFrame relativi alla categoria corrente
            df_fl_categoria = df_per_categoria[categoria]
            df_regex_categoria = df_regex_per_categoria.get(categoria, pd.DataFrame())
            
            # Verifichiamo che ci siano dati da elaborare
            if df_fl_categoria.empty:
                print(f"Nessun dato FL per la categoria {categoria}")
                continue
            
            if df_regex_categoria.empty:
                # Se non ci sono regex specifiche per questa categoria, usiamo tutte le regex
                print(f"Nessuna regex specifica per {categoria}, utilizzeremo tutte le regex disponibili")
                df_regex_categoria = df_regex_completo.copy()
            
            # Eseguiamo la verifica
            try:
            
                # Aggiungiamo una colonna per indicare la categoria
                df_fl_categoria['Categoria'] = categoria
                
                # Chiamiamo la funzione di verifica
                risultato = RegularExpressionsTools.verifica_fl_con_regex(df_fl_categoria, df_regex_categoria)
                risultati_categorie[categoria] = risultato
                
            except Exception as e:
                print(f"Errore durante la verifica della categoria {categoria}: {str(e)}")
                # Continuiamo con le altre categorie
        
        # Unifichiamo i risultati
        df_risultati = pd.DataFrame()
        
        # Uniamo solo i DataFrame non vuoti
        dfs_da_unire = [df for df in risultati_categorie.values() if df is not None and not df.empty]
        
        if dfs_da_unire:
            df_risultati = pd.concat(dfs_da_unire, ignore_index=True)
        """         
        # Riordiniamo le colonne per una migliore leggibilità
        colonne_ordinate = ['Categoria', 'FL', 'FL_Lunghezza', 'Check_Result']
        altre_colonne = [col for col in df_risultati.columns if col not in colonne_ordinate]
        df_risultati = df_risultati[colonne_ordinate + altre_colonne]
        """    
        return df_risultati