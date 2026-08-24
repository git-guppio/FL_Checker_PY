"""
Script per rilevare i valori dei campi necessari alla generazione del file di caricamento delle
sedi tecniche, leggendoli direttamente da una sede tecnica esistente in SAP (transazione IL03,
inserire almeno un terzo livello, es. "ESH-BCNA-01").
"""
import time
from typing import Dict, Optional


def wait_for_sap(session, timeout: int = 30) -> bool:
    """
    Attende che SAP finisca le operazioni in corso

    Args:
        session: Oggetto sessione SAP attiva
        timeout: Tempo massimo di attesa in secondi

    Returns:
        bool: True se SAP è diventato disponibile, False se è scaduto il timeout
    """
    start_time = time.time()
    try:
        while session.Busy:
            if time.time() - start_time > timeout:
                print(f"Timeout dopo {timeout} secondi di attesa")
                return False
            time.sleep(0.5)
        return True
    except Exception as e:
        print(f"Errore durante l'attesa: {str(e)}")
        return False


def rileva_dati_sede_tecnica(session, tplnr: str, timeout: int = 30) -> Optional[Dict[str, str]]:
    """
    Legge dalla transazione IL03 i valori dei campi necessari alla generazione del file di
    caricamento delle sedi tecniche, prelevandoli da una sede tecnica esistente
    (SWERK, STORT, ABCKZ, BUKRS, KOSTL, IWERK, INGRP, GEWRK, WERGW, LONGITUDE, LATITUDE).

    Args:
        session: Oggetto sessione SAP attiva
        tplnr: Codice della sede tecnica da cui leggere i valori (almeno 3 livelli, es. "ESH-BCNA-01")
        timeout: Tempo massimo di attesa per le operazioni SAP, in secondi

    Returns:
        Dict[str, str]: dizionario {nome_campo: valore} oppure None in caso di errore
    """
    try:
        # Avvio transazione IL03 Visualizza sede tecnica
        session.findById("wnd[0]/tbar[0]/okcd").text = "/nIL03"
        session.findById("wnd[0]").sendVKey(0)
        if not wait_for_sap(session, timeout):
            print("Timeout durante l'apertura della transazione IL03")
            return None

        # Inserisco la sede tecnica da cui leggere i valori dei campi
        session.findById("wnd[0]/usr/ctxtIFLO-TPLNR").text = tplnr
        session.findById("wnd[0]/usr/ctxtRILO0-TPLKZ").text = ""
        session.findById("wnd[0]").sendVKey(0)
        if not wait_for_sap(session, timeout):
            print("Timeout durante il caricamento della sede tecnica")
            return None

        # Verifico eventuali errori (es. sede tecnica non trovata)
        status_bar = session.findById("wnd[0]/sbar")
        if status_bar.messageType == "E":
            print(f"Errore SAP: {status_bar.text}")
            return None

        values: Dict[str, str] = {}

        # Seleziono i tab e i campi da cui leggere i valori
        session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\01").select()

        # Tab T\02 (Ubicazione)
        session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\02").select()
        values["SWERK"] = session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\02/ssubSUB_DATA:SAPLITO0:0102/subSUB_0102A:SAPLITO0:1050/ctxtITOB-SWERK").text
        values["STORT"] = session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\02/ssubSUB_DATA:SAPLITO0:0102/subSUB_0102A:SAPLITO0:1050/ctxtITOB-STORT").text
        values["ABCKZ"] = session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\02/ssubSUB_DATA:SAPLITO0:0102/subSUB_0102A:SAPLITO0:1050/ctxtITOB-ABCKZ").text
        values["LONGITUDE"] = session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\02/ssubSUB_DATA:SAPLITO0:0102/subSUB_0102D:SAPLITO0:1080/ssubXUSR1080:SAPLXTOB:1002/txtIFLOT-LONGITUDE").text
        values["LATITUDE"] = session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\02/ssubSUB_DATA:SAPLITO0:0102/subSUB_0102D:SAPLITO0:1080/ssubXUSR1080:SAPLXTOB:1002/txtIFLOT-LATITUDE").text

        # Tab T\03 (Organizzazione)
        session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\03").select()
        values["BUKRS"] = session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\03/ssubSUB_DATA:SAPLITO0:0102/subSUB_0102A:SAPLITO0:1052/ctxtITOB-BUKRS").text
        values["KOSTL"] = session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\03/ssubSUB_DATA:SAPLITO0:0102/subSUB_0102A:SAPLITO0:1052/ctxtITOB-KOSTL").text
        values["IWERK"] = session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\03/ssubSUB_DATA:SAPLITO0:0102/subSUB_0102B:SAPLITO0:1062/ctxtITOB-IWERK").text
        values["INGRP"] = session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\03/ssubSUB_DATA:SAPLITO0:0102/subSUB_0102B:SAPLITO0:1062/ctxtITOB-INGRP").text
        values["GEWRK"] = session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\03/ssubSUB_DATA:SAPLITO0:0102/subSUB_0102B:SAPLITO0:1062/ctxtITOBATTR-GEWRK").text
        values["WERGW"] = session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\03/ssubSUB_DATA:SAPLITO0:0102/subSUB_0102B:SAPLITO0:1062/txtITOBATTR-WERGW").text

        # session.findById(r"wnd[0]/usr/tabsTABSTRIP/tabpT\04").select()

        return {k: v.strip() for k, v in values.items()}

    except Exception as e:
        print(f"Errore nella rilevazione dei dati dalla sede tecnica {tplnr}: {str(e)}")
        return None
