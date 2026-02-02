import pandas as pd
import os

# 1. Chargement
df = pd.read_csv('./Data_lake/raw/dvf_stats_mensuelles.csv')

# 2. Sélection des colonnes stratégiques (on ignore les 'local' qui ne sont pas demandés)
cols_utiles = [
    'annee_mois', 'code_geo', 'libelle_geo', 'echelle_geo',
    'nb_ventes_maison', 'med_prix_m2_maison',
    'nb_ventes_appartement', 'med_prix_m2_appartement'
]
df = df[cols_utiles].copy()

# 3. Nettoyage Temporel
df['annee_mois'] = pd.to_datetime(df['annee_mois'], format='%Y-%m')
# On crée des colonnes dédiées pour faciliter le SQL plus tard
df['annee'] = df['annee_mois'].dt.year
df['mois'] = df['annee_mois'].dt.month

# 4. Nettoyage Géographique
# On s'assure que le code_geo (ex: 01) reste du texte avec le 0
df['code_geo'] = df['code_geo'].astype(str).str.zfill(2)

# 5. Traitement des Valeurs (Ventes et Prix)
# Remplacement des NaN par 0 uniquement pour les colonnes de ventes
df['nb_ventes_maison'] = df['nb_ventes_maison'].fillna(0)
df['nb_ventes_appartement'] = df['nb_ventes_appartement'].fillna(0)

# Filtrage des prix aberrants (on ne garde que le "vrai" marché)
df = df[(df['med_prix_m2_maison'] > 0) | (df['med_prix_m2_appartement'] > 0)]

# 6. Sauvegarde
os.makedirs('./Data_lake/Staging', exist_ok=True)
df.to_csv('./Data_lake/Staging/stg_dvf_mensuel_staging.csv', index=False)

# --- SECTION AFFICHAGE (Pour vérification) ---
print("-" * 30)
print("APERÇU DES DONNÉES NETTOYÉES :")
print(df.head())

print("\nINFOS TECHNIQUES :")
print(df.info())

print("\nVÉRIFICATION DES DATES :")
print(f"Première date : {df['annee_mois'].min()}")
print(f"Dernière date  : {df['annee_mois'].max()}")

print("\nVÉRIFICATION DES VALEURS MANQUANTES :")
print(df.isna().sum())
print("-" * 30)
print(f"Staging terminé ! {len(df)} lignes conservées sur 3,6 millions.")
print(f"Période couverte : {df['annee_mois'].min()} à {df['annee_mois'].max()}")