import pandas as pd
import os

# 1. Chargement du fichier brut géographique
df_geo_raw = pd.read_csv('./Data_lake/raw/dvf_stats_geographiques_globales.csv')

# 2. Sélection des colonnes utiles pour l'analyse par département
# On prend les indicateurs "whole" (globaux) pour les types de biens demandés
cols_geo_utiles = [
    'code_geo', 
    'libelle_geo', 
    'echelle_geo',
    'nb_ventes_whole_maison', 
    'med_prix_m2_whole_maison',
    'nb_ventes_whole_appartement', 
    'med_prix_m2_whole_appartement',
    'nb_ventes_whole_apt_maison', 
    'med_prix_m2_whole_apt_maison'
]

# On crée une copie avec uniquement ces colonnes
df_geo_stg = df_geo_raw[cols_geo_utiles].copy()

# 3. FILTRAGE CRUCIAL : On ne garde que l'échelle départementale
# Cela évite que le Top 10 soit pollué par la ligne "France entière" (nation) 
# ou par des milliers de petites communes.
df_geo_stg = df_geo_stg[df_geo_stg['echelle_geo'] == 'departement']

# 4. Standardisation
# Forcer le code_geo en texte (ex: '1' -> '01')
df_geo_stg['code_geo'] = df_geo_stg['code_geo'].astype(str).str.zfill(2)

# Remplacer les NaN par 0 pour les volumes de ventes
cols_ventes = [c for c in df_geo_stg.columns if 'nb_ventes' in c]
df_geo_stg[cols_ventes] = df_geo_stg[cols_ventes].fillna(0)

# --- SECTION AFFICHAGE (Pour vérification avant sauvegarde) ---
print("-" * 30)
print("APERÇU STAGING GÉOGRAPHIQUE (DÉPARTEMENTS) :")
print(df_geo_stg.head())

print("\nNOMBRE DE DÉPARTEMENTS TROUVÉS :")
print(len(df_geo_stg)) # Devrait être autour de 100

print("\nVÉRIFICATION DES PRIX MÉDIANS :")
# On vérifie s'il reste des prix à 0 qui pourraient fausser le Top 10
print(f"Prix médian min (maisons) : {df_geo_stg['med_prix_m2_whole_maison'].min()}")
print("-" * 30)

# --- SECTION SAUVEGARDE ---
os.makedirs('./Data_lake/Staging', exist_ok=True)
df_geo_stg.to_csv('./Data_lake/Staging/stg_dvf_geo_staging.csv', index=False)
print("Fichier géographique nettoyé et sauvegardé dans Staging/stg_dvf_geo_staging.csv")