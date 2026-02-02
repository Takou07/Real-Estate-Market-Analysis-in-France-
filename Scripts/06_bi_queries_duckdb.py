import duckdb

# Connexion au Warehouse
con = duckdb.connect('./warehouse/dvf_market.db')

print("--- ANALYSE DU MARCHÉ IMMOBILIER (SQL) ---\n")

# 1 & 2. Disponibilité des données (Janvier 2026)
# On cherche la date maximum présente dans la base
res_date = con.execute("""
    SELECT 
        MAX(annee_mois) as derniere_date,
        COUNT(*) FILTER (WHERE annee_mois = '2026-01-01') as nb_janvier_2026
    FROM fact_monthly_indicators
""").fetchone()

print(f"Q1: Données pour Janvier 2026 disponibles ? {'Oui' if res_date[1] > 0 else 'Non'}")
print(f"Q2: Le mois le plus récent disponible est : {res_date[0].strftime('%B %Y')}\n")

# 3. Prix médian par type de bien (sur toute la période)
res_prix = con.execute("""
    SELECT 
        median(med_prix_m2_appartement) as median_apt,
        median(med_prix_m2_maison) as median_house
    FROM fact_monthly_indicators
""").fetchone()

print(f"Q3a: Prix médian Appartements : {res_prix[0]:.2f} €/m²")
print(f"Q3b: Prix médian Maisons : {res_prix[1]:.2f} €/m²\n")

# 4. Évolution des prix (Year-over-Year)
# On compare le dernier mois (Juin 2025) au même mois l'année d'avant (Juin 2024)
# 4. Évolution des prix (Year-over-Year) corrigée
res_evo = con.execute("""
    SELECT 
        curr.annee_mois,
        curr.med_prix_m2_appartement as prix_curr,
        prev.med_prix_m2_appartement as prix_prev,
        ((curr.med_prix_m2_appartement - prev.med_prix_m2_appartement) / prev.med_prix_m2_appartement) * 100 as evolution
    FROM fact_monthly_indicators curr
    JOIN fact_monthly_indicators prev 
        ON month(curr.annee_mois) = month(prev.annee_mois) 
        AND year(curr.annee_mois) = year(prev.annee_mois) + 1
    ORDER BY curr.annee_mois DESC
    LIMIT 1
""").fetchone()

if res_evo:
    print(f"Q4: Évolution des prix (Appartements) entre {res_evo[0].year-1} et {res_evo[0].year} : {res_evo[3]:.2f}%\n")

# 5. Top 10 Départements
print("Q5a: Top 10 Départements par Volume de Transactions :")
print(con.execute("SELECT libelle_geo, nb_ventes_whole_apt_maison FROM dim_top_departments_volume LIMIT 10").df())

con.close()