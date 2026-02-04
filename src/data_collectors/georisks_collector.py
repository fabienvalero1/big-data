"""
Collecteur de données de risques géographiques
Simule l'appel à l'API Géorisques
"""

# Données de risques par ville (simulées)
GEORISKS_DATA = {
    '75056': {'risque_inondation': True, 'risque_industriel': False, 'risque_sismique': 1},
    '69123': {'risque_inondation': True, 'risque_industriel': True, 'risque_sismique': 2},
    '13055': {'risque_inondation': True, 'risque_industriel': True, 'risque_sismique': 3},
    '33063': {'risque_inondation': True, 'risque_industriel': False, 'risque_sismique': 2},
    '42218': {'risque_inondation': False, 'risque_industriel': True, 'risque_sismique': 2},
}

def collect_georisks(**context):
    """
    Collecte les données de risques géographiques
    En production: appel à https://georisques.gouv.fr/api/v1/gaspar/risques
    """
    print("Début de la collecte des risques géographiques...")
    
    risks = []
    for code_insee, risk_data in GEORISKS_DATA.items():
        risks.append({
            'code_insee': code_insee,
            **risk_data
        })
    
    print(f"✅ Risques collectés pour {len(risks)} communes")
    
    # Stocke dans XCom
    context['ti'].xcom_push(key='georisks', value=risks)
    
    return risks
