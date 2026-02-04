"""
Collecteur de taux financiers
Simule les données de la Banque de France
"""
from datetime import date

import requests
from bs4 import BeautifulSoup

def collect_rates(**context):
    """
    Collecte les taux d'intérêt actuels via scraping ou simulation améliorée.
    Source: Euribor Rates (ou fallback)
    """
    print("Debut de la collecte des taux financiers (Scraping)...")
    
    euribor_3m = None
    
    # Tentative de scraping (Euribor 3 mois)
    try:
        url = "https://www.euribor-rates.eu/en/current-euribor-rates/3-months-euribor-rate/"
        print(f"  Scraping de {url}...")
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            # Chercher la valeur dans le tableau (structure typique du site)
            # La structure peut changer, c'est fragile, donc on wrap bien dans un try/except
            # Pour l'exemple simple : on cherche un td avec la classe 'text-right' qui contient un %
            
            # Exemple simplifié de parsing (à adapter selon le HTML réel du moment)
            # Souvent c'est dans un tableau
            card_body = soup.find('div', class_='card-body')
            if card_body:
               val_text = card_body.find('table').findAll('td')[-1].text.strip().replace('%', '')
               euribor_3m = float(val_text)
               print(f"  Taux trouve: {euribor_3m}%")
            else:
                 print("    Structure HTML non reconnue")
        else:
            print(f"Erreur HTTP {response.status_code}")
            
    except Exception as e:
        print(f"    Erreur scraping: {str(e)}")

    # Fallback si échec
    if euribor_3m is None:
        print("    Utilisation valeur par defaut (fallback)")
        euribor_3m = 3.5

    # Marge banque (simulée)
    taux_client = round(euribor_3m + 1.2, 2)
    taux_usure = round(taux_client + 2.0, 2) # Est/Ouest approx

    rates = {
        'date_valeur': date.today().isoformat(),
        'taux_euribor_3m': euribor_3m,
        'taux_nominal_20ans': taux_client,
        'taux_usure': taux_usure
    }
    
    print(f"Taux collectes: {rates['taux_nominal_20ans']}%")
    
    # Stocke dans XCom
    context['ti'].xcom_push(key='rates', value=rates)
    
    return rates
