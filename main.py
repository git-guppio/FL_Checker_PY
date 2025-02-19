# Importing the Libraries
import SAP_Connection
import SAP_Transactions
import DF_Tools
import sys

def main():
    # Esempio di utilizzo della classe
    try:
        # Utilizzo con context manager
        with SAP_Connection.SAPGuiConnection() as sap:
            if sap.is_connected():
                session = sap.get_session()
                if session:
                    print("Connessione attiva nel context manager")
                    # Qui puoi inserire le tue operazioni SAP
                    # Crea l'estrattore
                    extractor = SAP_Transactions.SAPDataExtractor(session)
                    
                    # Estrai i dati da SAP e li memorizzo in un dataframe
                    print("Estrazione dati tabella ZPM4R_GL_T_FL")
                    # Creo un DF con i dati estratti
                    string_ZPM4R_GL_T_FL = extractor.extract_ZPM4R_GL_T_FL("S")  #

    except Exception as e:
        print(f"Errore generale: {str(e)}")
        sys.exit(1)
    
    # Crea un'istanza della classe
    df_utils = DF_Tools.DataFrameTools()
    # Pulisce i nomi delle colonne
    df_ZPM4R_GL_T_FL = df_utils.clean_data(string_ZPM4R_GL_T_FL)
    # Verifica che il DataFrame sia valido
    if not(df_utils.check_dataframe(df_ZPM4R_GL_T_FL, name="ZPM4R_GL_T_FL")):
        print("Errore nella verifica del DataFrame")
        sys.exit(1)
    else:
        # Aggiunge la colonna per la verifica
        df_ZPM4R_GL_T_FL = df_utils.add_concatenated_column(df_ZPM4R_GL_T_FL, "Valore Livello", "Valore Liv. Superiore", "Valore Liv. Superiore_1", "Liv.Sede")
        # Stampa anteprima del dataframe
        df_utils.analyze_data(df_ZPM4R_GL_T_FL)

if __name__ == "__main__":
    main()

    