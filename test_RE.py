import pandas as pd
import re
from RE_tools import RegularExpressionsTools

# Esempio di utilizzo:
if __name__ == "__main__":
    # Creo un DataFrame di esempio

    # Creo un DataFrame di esempio
    # Percorso del file CSV da leggere
    #file_path = r"C:\Users\a259046\OneDrive - Enel Spa\SCRIPT AHK e VBA\GITHUB\FL_Checker_PY\df_re_completo.csv"  # Modifica con il percorso effettivo
    file_path = "df_re_completo.csv"  # Modifica con il percorso effettivo

    # Leggi il CSV in un DataFrame
    df = pd.read_csv(file_path)
    print("#----------- df ---------#")
    print(df)

    # Identifica colonne dove tutti i valori sono NA
    colonne_da_eliminare = df.columns[df.isna().all()].tolist()

    # Verifico se ci sono colonne vuote (stringhe vuote o spazi)
    for col in df.columns:
        if df[col].dtype == 'object':  # Solo per colonne di tipo stringa
            if (df[col].str.strip().eq('') | df[col].isna()).all():
                colonne_da_eliminare.append(col)

    # Elimina le colonne identificate
    df_pulito = df.drop(columns=list(set(colonne_da_eliminare)))
    print("#----------- df_pulito ---------#")
    print(df_pulito)

    # Definisco il dizionario di regex
    regex_dict = {
        'SubStation': [r'^[a-zA-Z]{2}S-[a-zA-Z0-9]{4}-0A'],
        'Common': [r'^[a-zA-Z]{2}S-[a-zA-Z0-9]{4}-00',r'^[a-zA-Z]{2}S-[a-zA-Z0-9]{4}-ZZ',r'^[a-zA-Z]{2}S-[a-zA-Z0-9]{4}-9z']
    }

    # Applico la funzione
    try:
        result = RegularExpressionsTools.filter_dataframe_by_regex(df_pulito, regex_dict)
        
        # Stampo i risultati
        for category, filtered_df in result.items():
            print(f"\n{category} ({len(filtered_df)} righe):")
            print(filtered_df)
        
        # Valido i risultati
        valid, error_msg = RegularExpressionsTools.validate_filtering_result(df_pulito, result)
        print(f"\nValidazione: {'Successo' if valid else 'Fallita - ' + error_msg}")
        
    except Exception as e:
        print(f"Errore: {e}")