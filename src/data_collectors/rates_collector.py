"""
Collecteur de taux financiers
Simule les données de la Banque de France
"""
from datetime import date

def collect_rates(**context):
    """
    Collecte les taux d'intérêt actuels
    En production: appel à l'API Banque de France
    """
    print("💰 Début de la collecte des taux financiers...")
    
    rates = {
        'date_valeur': date.today().isoformat(),
        'taux_interet_moyen': 3.5,  # Taux moyen actuel
        'taux_usure': 5.5
    }
    
    print(f"✅ Taux collectés: {rates['taux_interet_moyen']}%")
    
    # Stocke dans XCom
    context['ti'].xcom_push(key='rates', value=rates)
    
    return rates
