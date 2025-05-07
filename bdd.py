import os
import subprocess
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

class RcloneHelper:
    def __init__(self):
        self.rclone_path = "C:\\Rclone\\rclone.exe"
        self.config_path = "C:\\Requetes_Quotidiennes\\Rclone\\rclone.conf"
        self.remote = "Prod"
        self.remote_path = "Promo/Retroplanning Promo Yannis.xlsx"
        self.local_temp_dir = "D:\\WinFTP"
        self.local_file_name = "amelioration_continue.xlsx"
        
        # Configuration base de données
        self.db_url = "postgresql://postgres:123456@localhost:5432/Workflow_Promo"
        self.table_promos = "promos"
        self.table_etapes = "etapes"
        self.table_sous_etapes = "sous_etapes"

        # Mapping des colonnes Excel vers les colonnes DB
        self.column_mapping = {
            "Clôture GAME \nEnvoi Supply": "date_cloture_game",
            "N°BCP": "promo_code",
            "FERMETURE ENGAGEMENT niveau magasin": "end_date",
            "OUVERTURE ENGAGEMENT": "start_date",
            "CATALOGUES / COLLECTIONS": "title"
        }

    def download_excel_to_df(self):
        os.makedirs(self.local_temp_dir, exist_ok=True)
        local_file = os.path.join(self.local_temp_dir, self.local_file_name)

        print(f"[INFO] Téléchargement de : {self.remote}:{self.remote_path} → {local_file}")
        result = subprocess.run([
            self.rclone_path,
            "--config", self.config_path,
            "copyto",
            f"{self.remote}:{self.remote_path}",
            local_file
        ], capture_output=True, text=True)

        if result.returncode != 0:
            print("[ERREUR RCLONE]")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            raise RuntimeError("❌ Échec du téléchargement du fichier Excel")

        print(f"[OK] Fichier téléchargé : {local_file}")
        df = pd.read_excel(local_file)
        df.rename(columns=self.column_mapping, inplace=True)
        # Filtrage des dates valides
        df = df[
            pd.to_datetime(df['end_date'], errors='coerce').notna() &
            pd.to_datetime(df['start_date'], errors='coerce').notna() &
            pd.to_datetime(df['date_cloture_game'], errors='coerce').notna()
        ]

        today = datetime.now().date()
        def determine_status(row):
            start = pd.to_datetime(row['start_date']).date()
            end = pd.to_datetime(row['end_date']).date()
            if end < today:
                return "Terminé"
            elif start > today:
                return "à venir"
            else:
                return "en cours"

        df['status'] = df.apply(determine_status, axis=1)
        df['promo_code'] = df.apply(lambda row: f"{row['Année']}-{row['promo_code']}", axis=1)

        return df[['title', 'start_date', 'end_date', 'status', 'promo_code', 'date_cloture_game']]

    def export_all_to_db(self, df):
        engine = create_engine(self.db_url)
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                # 1. Promos
                conn.execute(text(f"TRUNCATE TABLE {self.table_promos} RESTART IDENTITY CASCADE;"))
                df.to_sql(self.table_promos, conn, if_exists='append', index=False)

                # 2. Etapes
                conn.execute(text(f"TRUNCATE TABLE {self.table_etapes} RESTART IDENTITY CASCADE;"))
                conn.execute(text(
                    """
                    INSERT INTO etapes (name, status, promo_id, intervenant_id)
                    SELECT etape_name, 'À faire', p.id, intervenant_id
                    FROM promos p,
                         (VALUES 
                            ('Reception MAD', 5),
                            ('Préparation du dossier', 5),
                            ('Analyse OP', 5),
                            ('Animation TDC', 5),
                            ('Correction Post TDC', 5),
                            ('CR Post TDC', 5),
                            ('Engagement', 2),
                            ('Transfert réseau', 2)
                         ) AS etapes(etape_name, intervenant_id);
                    """
                ))

                # 3. Sous-étapes
                conn.execute(text(f"TRUNCATE TABLE {self.table_sous_etapes} RESTART IDENTITY CASCADE;"))
                conn.execute(text(
                    """
                    INSERT INTO sous_etapes (name, status, etape_id, promo_id, ordre)
                    SELECT ss.sous_etape_name, 'À faire', e.id, e.promo_id, ss.ordre
                    FROM etapes e
                    JOIN (
                        VALUES
                            ('Reception MAD', 'Reception', 1),
                            ('Préparation du dossier', 'Extraction du Full Game', 1),
                            ('Préparation du dossier', 'Extraction des sous-ventes Game', 2),
                            ('Préparation du dossier', 'Extraction Apoline', 3),
                            ('Préparation du dossier', 'Extraction Focus', 4),
                            ('Préparation du dossier', 'Extraction KPI engagement', 5),
                            ('Préparation du dossier', 'Récupération listing E-collab', 6),
                            ('Préparation du dossier', 'Extraction/Correction poids et dimensions', 7),
                            ('Préparation du dossier', 'Génération du calcul', 8),
                            ('Préparation du dossier', 'Macro accélération', 9),
                            ('Préparation du dossier', 'Lancement du script Revue Engagement', 10),

                            ('Analyse OP', 'Retour E-collab', 1),
                            ('Analyse OP', 'Finalisation Analyse OP', 2),
                            ('Analyse OP', 'Forçage des prévisions', 3),
                            ('Analyse OP', 'Communication des Pré_TDC', 4),

                            ('Animation TDC', 'Animation', 1),

                            ('Correction Post TDC', 'Correction', 1),

                            ('CR Post TDC', 'Rédaction du CR', 1),
                            ('CR Post TDC', 'Copie et conversion du fichier', 2),
                            ('CR Post TDC', 'Partage et envoi du mail', 3),

                            ('Engagement', 'Controle engagement', 1),
                            ('Engagement', 'Chargement Game', 2),
                            ('Engagement', 'Controle post engagement', 3),
                            ('Engagement', 'Generation réferentiel promo', 4),

                            ('Transfert réseau', 'Envoi du mail', 1)
                    ) AS ss(nom_etape, sous_etape_name, ordre)
                    ON e.name = ss.nom_etape;
                    """
                ))

                # 4. Mise à jour des statuts terminés
                conn.execute(text(
                    "UPDATE promos SET status = 'Terminé' WHERE end_date < CURRENT_DATE;"
                ))
                conn.execute(text(
                    "UPDATE etapes e SET status = 'Terminé' FROM promos p WHERE e.promo_id = p.id AND p.status = 'Terminé';"
                ))
                conn.execute(text(
                    "UPDATE sous_etapes se SET status = 'Terminé' FROM etapes e WHERE se.etape_id = e.id AND e.status = 'Terminé';"
                ))

                trans.commit()
                print("[OK] Export complet et mise à jour des statuts réussis.")
            except Exception as e:
                trans.rollback()
                print("[ERREUR DB]", e)
                raise

# ---- Lancement principal ----
if __name__ == "__main__":
    helper = RcloneHelper()
    df = helper.download_excel_to_df()
    print("\n📄 Aperçu du fichier téléchargé :")
    print(df.head())

    helper.export_all_to_db(df)
