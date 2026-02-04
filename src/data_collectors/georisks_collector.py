"""
Collecteur de donnees de risques geographiques
Appelle l'API Georisques
"""

import requests
import time

# Villes cibles (Codes INSEE)
# 75056: Paris, 69123: Lyon, 13055: Marseille, 33063: Bordeaux, 42218: Saint-Étienne
TARGET_CITIES = ['75056', '69123', '13055', '33063', '42218']

def collect_georisks(**context):
    """
    Collecte les donnees de risques geographiques via l'API Georisques
    API: https://georisques.gouv.fr/api/v1/gaspar/risques
    """
    print("Debut de la collecte des risques geographiques (API Reelle)...")
    
    risks = []
    
    for code_insee in TARGET_CITIES:
        try:
            print(f"   Appel API pour {code_insee}...")
            url = f"https://georisques.gouv.fr/api/v1/gaspar/risques?code_insee={code_insee}"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                # L'API retourne une liste de risques ("data")
                if 'data' in data and len(data['data']) > 0:
                    # On simplifie en prenant le résumé du premier item disponible ou en agrégeant
                    # Ici on regarde juste la présence de certains risques dans la réponse
                    city_risks = data['data'][0] # Un exemple de fiche risque
                    
                    # Extraction simplifiée des booléens de risque
                    item = {
                        'code_insee': code_insee,
                        'risque_inondation': any(r.get('libelle_risque_long') == 'Inondation' for r in data['data']),
                        'risque_industriel': any(r.get('libelle_risque_long') == 'Industriel' for r in data['data']),
                        'risque_sismique': city_risks.get('classement_sismique', 'Mon') # Ex: "2 (Faible)"
                    }
                    risks.append(item)
                else:
                    print(f"    Pas de donnees pour {code_insee}")
            else:
                print(f"    Erreur API {response.status_code} pour {code_insee}")
                
            # Petit délai pour être poli avec l'API
            time.sleep(0.5)

        except Exception as e:
            print(f"    Exception pour {code_insee}: {str(e)}")

    print(f"Risques collectes pour {len(risks)} communes")
    
    # Stocke dans XCom
    context['ti'].xcom_push(key='georisks', value=risks)
    
    return risks
