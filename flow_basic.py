#!/usr/bin/env python3
"""
⚡ TEST RAPIDE PREFECT - 30 SECONDES
==================================

Copiez ce code, sauvez comme quick_test.py et exécutez :
python quick_test.py
"""

import os
from datetime import datetime

# Configuration obligatoire
os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"
os.environ["PREFECT_SERVER_ALLOW_EPHEMERAL_MODE"] = "false"
from prefect import flow, task
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""

@task
def etape_1():
    message = f"✅ Étape 1 terminée à {datetime.now().strftime('%H:%M:%S')}"
    print(message)
    return message

@task  
def etape_2(message_precedent):
    resultat = len(message_precedent)
    message = f"✅ Étape 2 : '{message_precedent}' contient {resultat} caractères"
    print(message)
    return message

@task
def etape_finale(resultats):
    print("🎯 RÉCAPITULATIF :")
    for i, resultat in enumerate(resultats, 1):
        print(f"   {i}. {resultat}")
    return f"✅ Test terminé avec {len(resultats)} étapes !"

@flow(name="test-rapide", log_prints=True)
def test_rapide():
    """Test simple pour vérifier que Prefect fonctionne"""
    
    print("🚀 Démarrage du test rapide...")
    
    # Exécuter les tâches
    msg1 = etape_1()
    msg2 = etape_2(msg1)
    final = etape_finale([msg1, msg2])
    
    print(f"🎉 {final}")
    return final

if __name__ == "__main__":
    print("⚡ TEST RAPIDE PREFECT")
    print("=" * 30)
    
    # Vérification express
    try:
        import requests
        if requests.get("http://localhost:4200/api/health", timeout=3).status_code == 200:
            print("✅ Prefect accessible")
        else:
            print("❌ Prefect non accessible")
            exit(1)
    except:
        print("❌ Impossible de se connecter à Prefect")
        print("💡 Vérifiez : docker-compose ps")
        exit(1)
    
    # Exécuter le test
    resultat = test_rapide()
    
    print("\n" + "=" * 50)
    print("🎉 TEST RÉUSSI !")
    print("🌐 Vérifiez dans l'interface : http://localhost:4200")
    print("📋 Onglet 'Flow Runs' pour voir les détails")
    print("=" * 50)
    