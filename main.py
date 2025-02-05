# Importing the Libraries
import SAP_Connection
import SAP_Transactions
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
                    df_ZPM4R_GL_T_FL = extractor.extract_ZPM4R_GL_T_FL("S")  #


    except Exception as e:
        print(f"Errore generale: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

    