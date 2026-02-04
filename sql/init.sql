-- Creation des tables pour Buy & Rent

CREATE TABLE IF NOT EXISTS dim_location (
    code_insee VARCHAR(10) PRIMARY KEY,
    ville VARCHAR(255),
    code_postal VARCHAR(10),
    departement VARCHAR(5),
    zone_tendu BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_properties (
    property_id UUID PRIMARY KEY,
    type_bien VARCHAR(50),
    surface FLOAT,
    nb_pieces INT,
    dpe_class VARCHAR(2),
    annee_construction INT
);

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
    -- Foreign keys could be enforced but meant for star schema logic
    -- CONSTRAINT fk_location FOREIGN KEY (code_insee) REFERENCES dim_location(code_insee)
    -- Simply simplified for the MVP to a flat fact table or minimal FKs
    metadata JSONB
);

-- Table for market trends (aggregates)
CREATE TABLE IF NOT EXISTS agg_market_trends (
    ville VARCHAR(255),
    date_jour DATE,
    prix_m2_moyen FLOAT,
    nb_annonces INT,
    PRIMARY KEY (ville, date_jour)
);

-- Reference tables matching the "static" data sources
CREATE TABLE IF NOT EXISTS ref_georisques (
    code_insee VARCHAR(10) PRIMARY KEY,
    risque_inondation BOOLEAN DEFAULT FALSE,
    risque_industriel BOOLEAN DEFAULT FALSE,
    risque_sismique INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ref_taux (
    id SERIAL PRIMARY KEY,
    date_valeur DATE,
    taux_interet_moyen FLOAT,
    taux_usure FLOAT
);

-- Insert some dummy data for dim_location if needed
INSERT INTO dim_location (code_insee, ville, code_postal, departement, zone_tendu) VALUES
('75056', 'Paris', '75001', '75', TRUE),
('69123', 'Lyon', '69000', '69', TRUE),
('42218', 'Saint-Étienne', '42000', '42', FALSE)
ON CONFLICT DO NOTHING;

-- Insert default rates
INSERT INTO ref_taux (date_valeur, taux_interet_moyen, taux_usure) VALUES
(CURRENT_DATE, 3.5, 5.5);
