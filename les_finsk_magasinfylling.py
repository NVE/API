import datetime as dt
from datetime import date
import requests
import pandas as pd
import numpy as np
import json

def hent_dato(måned=0, dato=0):
    today = dt.date.today()

    idx = (today.weekday() + 1) % 7

    if idx == 0:  # Leser forrige søndag hvis startdato er en søndag. 
        idx = 7
    r = today - dt.timedelta(idx)
    print(r)

    if måned == 0:
        mm = f"{r.month:02d}"
        dd = f"{r.day:02d}"
    else:
        # eller leser fra gitt dato
        mm = f"{måned:02d}"
        dd = f"{dato:02d}"
    return r, mm, dd, today

def les_finske_magasiner():

    link = "http://wwwi2.ymparisto.fi/i2/95/prev_week.txt"
    f = requests.get(link)

    #finner starten av datatabellen
    pos=f.text.find("mmdd")

    liste=f.text[pos:].split(" ")

    #fjerner tomme strings
    liste=[x for x in liste if x]

    mmdd = []
    fylling_år = []
    fylling_forrigeår = []

    for i, num in enumerate(liste):
        if ((i+8) % 8 == 0):
            mmdd.append(num)
        if ((i+6) % 8 == 0):
            fylling_år.append(num)
        if ((i + 4) % 8 == 0):
            fylling_forrigeår.append(num)

    fylling={"mmdd": mmdd, "fylling_prosent": fylling_år,  "fylling_prosent_forrigeår": fylling_forrigeår}
    fylling=pd.DataFrame(fylling)

    fylling["mmdd"]=fylling["mmdd"].astype(str)

    r, mm, dd, today = hent_dato()

    print(f"leser magasin for datoen {r.year}-{mm}-{dd}")

    if r.year != today.year:
        #leser fra forrige år dersom vi har kommet til et annet år enn det vi skal finne magasinfyllingen fra
        fylling=fylling["fylling_prosent_forrigeår"][fylling["mmdd"] == f"{mm}{dd}"].values[0]
    else:
        fylling=fylling["fylling_prosent"][fylling["mmdd"]==f"{mm}{dd}"].values[0]
    fylling=float(fylling)

    # liten sjekk på at verdiene er i prosent
    if not ((0.0 - 0.00001) <= fylling <= (100.0 + 0.00001)):
        print("Fant magasinfylling utenfor [0, 100], gir opp")
        return

    return fylling

if __name__ == "__main__":
    les_finsk_magasinfylling()
