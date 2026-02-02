import duckdb
import pandas as pd
import os

# Connexion à la base
db_path = './warehouse/dvf_market.db'
con = duckdb.connect(db_path)

print("--- DÉBUT DE LA VALIDATION DU WAREHOUSE ---")

# 1. Vérifier si les tables existent
tables = con.execute("SHOW TABLES").fetchall()
table_names = [t[0] for t in tables]
print(f"Tables présentes : {table_names}")

expected_tables = ['fact_monthly_indicators', 'dim_top_departments_volume', 'dim_top_departments_price']
for table in expected_tables:
    if table in table_names:
        print(f"✅ Table '{table}' : EXISTE")
    else:
        print(f"❌ Table '{table}' : MANQUANTE")

# 2. Vérifier si les tables contiennent des données (Row Count)
for table in table_names:
    count_sql = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    
    # Comparaison avec le fichier source CSV pour la cohérence
    # On prend le nom du fichier correspondant à la table
    if "monthly" in table:
        csv_path = './Data_lake/curated/france_mensuel.csv'
    elif "volume" in table:
        csv_path = './Data_lake/curated/top_dep_volume.csv'
    else:
        csv_path = './Data_lake/curated/top_dep_volume.csv'
        
    df_source = pd.read_csv(csv_path)
    count_csv = len(df_source)
    
    if count_sql == count_csv:
        print(f"✅ Table '{table}' : {count_sql} lignes (COHÉRENT avec le CSV)")
    else:
        print(f"❌ Table '{table}' : {count_sql} lignes vs {count_csv} dans le CSV (INCOHÉRENCE)")

# 3. Aperçu rapide du contenu
print("\nAperçu des 3 premières lignes de 'fact_monthly_indicators' :")
print(con.execute("SELECT * FROM fact_monthly_indicators LIMIT 3").df())

con.close()
print("\n--- FIN DE LA VALIDATION ---")