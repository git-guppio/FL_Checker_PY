def crea_tabella(lista, separatore=";"):
    tabella = ""
    for i in range(0, len(lista), 10):
        riga = lista[i:i+10]
        tabella += separatore.join(riga) + "\n"
    return tabella

# Esempio di utilizzo
elementi = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v']

# Con punto e virgola
risultato_pv = crea_tabella(elementi, ";")
print("Tabella con punto e virgola:")
print(risultato_pv)

# Con virgola
risultato_v = crea_tabella(elementi, ",")
print("\nTabella con virgola:")
print(risultato_v)


def main():
    # Esempio di utilizzo
    elementi = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v']

    # Con punto e virgola
    risultato_pv = crea_tabella(elementi, "; ")
    print("Tabella con punto e virgola:")
    print(risultato_pv)

    # Con virgola
    risultato_v = crea_tabella(elementi, ", ")
    print("\nTabella con virgola:")
    print(risultato_v)

if __name__ == '__main__':
    main()