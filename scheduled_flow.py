#!/usr/bin/env python3
"""
🏢 TEMPLATE JOB CARREFOUR - PRÊT À L'EMPLOI (Hebdomadaire)
=========================================================

- ✅ Prefect 2.x (API moderne)
- ✅ Worker Docker / Work Pool
- ✅ Scheduling hebdomadaire (lundi 06:00 Europe/Paris)
- ✅ Gestion d'erreurs & logs
"""

import os
from datetime import datetime, timedelta

# !! CONFIG OBLIGATOIRE AVANT IMPORT PREFECT !!
os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"
os.environ["PREFECT_SERVER_ALLOW_EPHEMERAL_MODE"] = "false"

from prefect import flow, task
from prefect.client.schemas.schedules import CronSchedule  # <-- Schedule cron (Prefect 2.x)

# =============================================
# TÂCHES MÉTIER (exemples à adapter)
# =============================================

@task(name="extraction", retries=3, retry_delay_seconds=60)
def extraire_donnees_carrefour(source: str, date: str):
    print(f"📥 Extraction depuis {source} pour le {date}")
    import time
    time.sleep(2)  # Simulation
    donnees_fictives = {
        "nb_records": 1500,
        "source": source,
        "date_extraction": date,
        "status": "success"
    }
    print(f"✅ {donnees_fictives['nb_records']} records extraits de {source}")
    return donnees_fictives

@task(name="transformation", retries=2)
def transformer_donnees(donnees_brutes: dict):
    print(f"🔄 Transformation de {donnees_brutes['nb_records']} records")
    donnees_transformees = {
        **donnees_brutes,
        "records_valides": donnees_brutes["nb_records"] - 50,
        "records_rejetes": 50,
        "transformation_time": datetime.now().isoformat()
    }
    print(f"✅ Transformation terminée: {donnees_transformees['records_valides']} valides")
    return donnees_transformees

@task(name="chargement")
def charger_donnees(donnees_transformees: dict, destination: str):
    print(f"📤 Chargement vers {destination}")
    resultat = {
        "destination": destination,
        "records_charges": donnees_transformees["records_valides"],
        "chargement_time": datetime.now().isoformat(),
        "status": "success"
    }
    print(f"✅ {resultat['records_charges']} records chargés vers {destination}")
    return resultat

@task(name="notification")
def envoyer_notification(resultats: dict, success: bool = True):
    status_emoji = "✅" if success else "❌"
    status_text = "SUCCÈS" if success else "ÉCHEC"
    message = f"""
{status_emoji} JOB CARREFOUR - {status_text}
========================================
Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Records traités: {resultats.get('records_charges', 0)}
Destination: {resultats.get('destination', 'N/A')}
Status: {status_text}
    """
    print(message)
    return message

# =============================================
# FLOW PRINCIPAL
# =============================================

@flow(
    name="etl-carrefour-template",
    description="Template ETL pour environnement Carrefour",
    log_prints=True,
    retries=1,
    retry_delay_seconds=300
)
def etl_carrefour_template(
    source: str = "database_carrefour",
    destination: str = "datawarehouse",
    date_traitement: str | None = None
):
    """
    Flow ETL principal pour Carrefour
    Args:
        source: Source des données (BDD, API, fichiers...)
        destination: Destination (DWH, datalake...)
        date_traitement: Date à traiter (YYYY-MM-DD), défaut=hier
    """
    if not date_traitement:
        hier = datetime.now() - timedelta(days=1)
        date_traitement = hier.strftime("%Y-%m-%d")

    print(f"🚀 Démarrage ETL Carrefour pour le {date_traitement}")

    try:
        donnees_extraites = extraire_donnees_carrefour(source, date_traitement)
        donnees_transformees = transformer_donnees(donnees_extraites)
        resultats_chargement = charger_donnees(donnees_transformees, destination)
        notification = envoyer_notification(resultats_chargement, success=True)

        resultat_final = {
            "status": "SUCCESS",
            "date_traitement": date_traitement,
            "source": source,
            "destination": destination,
            "records_traites": resultats_chargement["records_charges"],
            "fin_traitement": datetime.now().isoformat(),
            "notification": notification
        }
        print(f"🎉 ETL terminé avec succès : {resultat_final['records_traites']} records")
        return resultat_final

    except Exception as e:
        error_info = {"error": str(e), "date": date_traitement}
        envoyer_notification(error_info, success=False)
        print(f"❌ Erreur dans l'ETL : {e}")
        raise

# =============================================
# DÉPLOIEMENT HEBDOMADAIRE (unique)
# =============================================

def deployer_job_hebdomadaire():
    """Déploie le job Carrefour une fois par semaine (lundi 06:00 Europe/Paris)."""
    schedule = CronSchedule(cron="0 6 * * 1", timezone="Europe/Paris")  # Lundi 06:00

    # IMPORTANT: on crée un "flow proxy" depuis la source distante
    flow_ref = etl_carrefour_template.from_source(
        source="https://github.com/thomas-rotszyld/prefect_test.git",     # repo public (ou privé + token via bloc)
        entrypoint="scheduled_flow.py:etl_carrefour_template"
    )


    deployment = flow_ref.deploy(
        name="carrefour-etl-hebdo",
        description="ETL Carrefour - exécution hebdomadaire (lundi 06:00)",
        work_pool_name="local-process",
        schedule=schedule,
        parameters={"source": "hebdo_database", "destination": "hebdo_datawarehouse"},
        tags=["carrefour", "etl", "hebdomadaire"],
        version="1.0.0-hebdo",
    )
    print(f"✅ Déploiement hebdomadaire créé : {deployment.name}")
    return deployment
    print(f"✅ Déploiement hebdomadaire créé : {deployment.name}")
    return deployment

# =============================================
# MAIN
# =============================================

if __name__ == "__main__":
    print("🏢 TEMPLATE JOB CARREFOUR (Hebdomadaire)")
    print("=" * 60)

    choix = input("""
Que voulez-vous faire ?
1. Test d'exécution locale (sans déploiement)
2. Déployer job HEBDOMADAIRE (lundi 06:00)
Votre choix (1/2) : """).strip()

    if choix == "1":
        print("\n🧪 Test d'exécution locale...")
        resultat = etl_carrefour_template(
            source="test_source",
            destination="test_destination"
        )
        print(f"\n📊 Résultat : {resultat['status']} - {resultat['records_traites']} records")

    elif choix == "2":
        deployer_job_hebdomadaire()
        print("\n💡 Pour vérifier ou activer/désactiver le planning :")
        print("   1. http://localhost:4200 → Deployments")
        print("   2. etl-carrefour-template / carrefour-etl-hebdo → Resume/Pause")

    else:
        print("❌ Choix invalide - Test local par défaut")
        etl_carrefour_template()

    print(f"\n🌐 Interface Prefect : http://localhost:4200")
    print(f"📋 Onglet 'Deployments' pour gérer vos jobs")
    print(f"📊 Onglet 'Flow Runs' pour voir les exécutions")