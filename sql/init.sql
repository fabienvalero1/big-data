-- Schema pour le projet Big Data Buy & Rent

-- Table de dimension: Localisation
CREATE TABLE IF NOT EXISTS dim_location (
    code_insee VARCHAR(10) PRIMARY KEY,
    ville VARCHAR(255),
    code_postal VARCHAR(10),
    departement VARCHAR(5),
    zone_tendu BOOLEAN
);

-- Table de dimension: Propriétés
CREATE TABLE IF NOT EXISTS dim_properties (
    property_id UUID PRIMARY KEY,
    type_bien VARCHAR(50),
    surface FLOAT,
    nb_pieces INT,
    dpe_class VARCHAR(2),
    annee_construction INT
);

-- Table de fait: Annonces enrichies
CREATE TABLE IF NOT EXISTS fact_listings (
    listing_id UUID PRIMARY KEY,
    code_insee VARCHAR(10),
    property_id UUID,
    prix_acquisition FLOAT,
    loyer_estime FLOAT,
    rentabilite_brute FLOAT,
    cashflow FLOAT,
    score_investissement FLOAT,
    date_creation TIMESTAMP,
    metadata JSONB
);

-- Table d'agrégats: Tendances marché
CREATE TABLE IF NOT EXISTS agg_market_trends (
    ville VARCHAR(255),
    date_jour DATE,
    prix_m2_moyen FLOAT,
    nb_annonces INT,
    PRIMARY KEY (ville, date_jour)
);

-- Table de référence: Risques géographiques
CREATE TABLE IF NOT EXISTS ref_georisques (
    code_insee VARCHAR(10) PRIMARY KEY,
    risque_inondation BOOLEAN DEFAULT FALSE,
    risque_industriel BOOLEAN DEFAULT FALSE,
    risque_sismique INT DEFAULT 0
);

-- Table de référence: Taux financiers
CREATE TABLE IF NOT EXISTS ref_taux (
    id SERIAL PRIMARY KEY,
    date_valeur DATE,
    taux_interet_moyen FLOAT,
    taux_usure FLOAT
);

-- Données initiales
INSERT INTO dim_location (code_insee, ville, code_postal, departement, zone_tendu) VALUES
('75056', 'Paris', '75001', '75', TRUE),
('69123', 'Lyon', '69000', '69', TRUE),
('13055', 'Marseille', '13000', '13', TRUE),
('33063', 'Bordeaux', '33000', '33', TRUE),
('42218', 'Saint-Étienne', '42000', '42', FALSE)
ON CONFLICT DO NOTHING;

INSERT INTO ref_taux (date_valeur, taux_interet_moyen, taux_usure) VALUES
(CURRENT_DATE, 3.5, 5.5);

INSERT INTO ref_georisques (code_insee, risque_inondation, risque_industriel, risque_sismique) VALUES
('75056', TRUE, FALSE, 1),
('69123', TRUE, TRUE, 2),
('13055', TRUE, TRUE, 3),
('33063', TRUE, FALSE, 2),
('42218', FALSE, TRUE, 2)
ON CONFLICT DO NOTHING;
