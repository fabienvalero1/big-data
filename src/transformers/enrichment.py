"""
Module d'enrichissement des données
Combine les annonces avec les risques et calcule les indicateurs financiers
"""

def enrich_listings(**context):
    """
    Enrichit les annonces avec:
    - Les risques géographiques
    - Les calculs financiers (rentabilité, cashflow, score)
    """
    print("🔄 Début de l'enrichissement des données...")
    
    # Récupère les données des tasks précédentes
    ti = context['ti']
    listings = ti.xcom_pull(task_ids='collect_listings', key='raw_listings')
    georisks = ti.xcom_pull(task_ids='collect_georisks', key='georisks')
    rates = ti.xcom_pull(task_ids='collect_rates', key='rates')
    
    # Crée un dictionnaire de risques par code_insee
    risks_by_insee = {r['code_insee']: r for r in georisks}
    
    # Taux d'intérêt
    interest_rate = rates['taux_interet_moyen'] / 100.0
    months = 240  # 20 ans
    monthly_rate = interest_rate / 12.0
    
    enriched_listings = []
    
    for listing in listings:
        code_insee = listing['code_insee']
        price = listing['price']
        
        # Ajoute les risques
        risk = risks_by_insee.get(code_insee, {})
        listing['risque_inondation'] = risk.get('risque_inondation', False)
        listing['risque_sismique'] = risk.get('risque_sismique', 0)
        
        # Calculs financiers
        # Mensualité d'emprunt
        if interest_rate > 0:
            factor = monthly_rate / (1 - (1 + monthly_rate) ** (-months))
            mensualite = price * factor
        else:
            mensualite = price / months
        
        # Loyer estimé (5% de rendement brut annuel)
        loyer_estime = price * 0.05 / 12
        
        # Cashflow mensuel
        cashflow = loyer_estime - mensualite - 150  # 150€ charges/taxes
        
        # Rentabilité brute
        rentabilite_brute = (loyer_estime * 12 / price) * 100
        
        # Score d'investissement
        if cashflow > 0:
            score = 8.0
        elif rentabilite_brute > 6:
            score = 6.0
        else:
            score = 4.0
        
        # Enrichit l'annonce
        listing['loyer_estime'] = round(loyer_estime, 2)
        listing['mensualite'] = round(mensualite, 2)
        listing['cashflow'] = round(cashflow, 2)
        listing['rentabilite_brute'] = round(rentabilite_brute, 2)
        listing['score_investissement'] = score
        
        enriched_listings.append(listing)
    
    print(f"✅ {len(enriched_listings)} annonces enrichies")
    
    # Stocke pour le loader
    ti.xcom_push(key='enriched_listings', value=enriched_listings)
    
    return enriched_listings
