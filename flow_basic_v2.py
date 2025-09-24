#!/usr/bin/env python3
"""
🎯 FLOW FORCÉ VERS SERVEUR DOCKER
==================================

Script qui FORCE Prefect à utiliser le serveur Docker
au lieu de démarrer un serveur temporaire
"""

import os
import sys
import time
import requests
import asyncio
from datetime import datetime

# !! CONFIGURATION AVANT TOUT IMPORT PREFECT !!
print("🔧 Configuration AVANT import Prefect...")

# Forcer l'API URL AVANT l'import de Prefect
os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"

# Désactiver explicitement le démarrage de serveur temporaire
os.environ["PREFECT_SERVER_ALLOW_EPHEMERAL_MODE"] = "false"

# Forcer le mode client distant
os.environ["PREFECT_API_ENABLE_HTTP2"] = "false"

print(f"✅ PREFECT_API_URL: {os.environ['PREFECT_API_URL']}")
print(f"✅ Server ephemeral disabled: {os.environ.get('PREFECT_SERVER_ALLOW_EPHEMERAL_MODE')}")

# Vérifier la connectivité AVANT d'importer Prefect
def verify_server_connection():
    """Vérifier que le serveur est accessible avant de commencer"""
    print("\n🔍 Vérification du serveur Docker AVANT import Prefect...")
    
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"   Tentative {attempt}/{max_attempts}...")
            response = requests.get(
                "http://localhost:4200/api/health",
                timeout=10,
                proxies={'http': '', 'https': ''}  # Force no proxy
            )
            
            if response.status_code == 200:
                print(f"   ✅ Serveur Docker accessible (status: {response.status_code})")
                return True
            else:
                print(f"   ⚠️  Serveur répond mais status: {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Tentative {attempt} échoue: {e}")
            
        if attempt < max_attempts:
            print(f"   ⏳ Attente 2 secondes avant retry...")
            time.sleep(2)
    
    print(f"   💥 Impossible de se connecter au serveur après {max_attempts} tentatives")
    return False

# Vérifier AVANT d'importer Prefect
if not verify_server_connection():
    print("❌ ARRÊT : Serveur Prefect non accessible")
    sys.exit(1)

print("\n📦 Import des modules Prefect...")
# Maintenant on peut importer Prefect en sécurité
from prefect import flow, task
from prefect.client.orchestration import get_client
from prefect.settings import PREFECT_API_URL

print(f"✅ Import terminé")
print(f"✅ Prefect API URL configuré: {PREFECT_API_URL.value()}")

async def verify_prefect_client():
    """Vérifier que le client Prefect peut se connecter"""
    print("\n🔍 Test du client Prefect...")
    try:
        async with get_client() as client:
            # Test de connectivité
            health_response = await client._client.get("/health")
            print(f"✅ Client Prefect connecté (status: {health_response.status_code})")
            
            # Test des work pools pour s'assurer que l'API complète fonctionne
            try:
                pools = await client.read_work_pools()
                print(f"✅ Work pools accessibles: {len(pools)} pools trouvés")
                for pool in pools:
                    print(f"   • {pool.name} (type: {pool.type})")
            except Exception as e:
                print(f"⚠️  Work pools non accessibles: {e}")
            
            return True
    except Exception as e:
        print(f"❌ Client Prefect échec: {e}")
        return False

# Test client asynchrone
print("🧪 Test du client Prefect...")
if not asyncio.run(verify_prefect_client()):
    print("❌ ARRÊT : Client Prefect non fonctionnel")
    sys.exit(1)

print("\n🎯 Tous les tests passent ! Démarrage du flow...")

# =========================================
# DÉFINITION DU FLOW
# =========================================

@task(name="connection-test")
def test_connection_dans_task():
    """Tâche qui teste la connectivité depuis l'intérieur du flow"""
    try:
        response = requests.get(
            "http://localhost:4200/api/health",
            timeout=5,
            proxies={'http': '', 'https': ''}
        )
        message = f"🔗 Connectivité dans task OK (status: {response.status_code})"
        print(message)
        return message
    except Exception as e:
        message = f"❌ Connectivité dans task échoue: {e}"
        print(message)
        return message

@task(name="info-environnement")
def info_environnement():
    """Informations sur l'environnement d'exécution"""
    import socket
    
    info = {
        "hostname": socket.gethostname(),
        "prefect_api_url": os.environ.get("PREFECT_API_URL"),
        "timestamp": datetime.now().isoformat(),
        "python_executable": sys.executable
    }
    
    print("🖥️  Environnement d'exécution :")
    for key, value in info.items():
        print(f"   {key}: {value}")
    
    return info

@task(name="etape-principale")
def etape_principale(numero: int):
    """Tâche principale du flow"""
    message = f"✅ Étape {numero} exécutée à {datetime.now().strftime('%H:%M:%S')}"
    print(message)
    return message

@flow(
    name="flow-force-serveur",
    description="Flow qui FORCE l'utilisation du serveur Docker",
    log_prints=True
)
def flow_force_serveur(nb_etapes: int = 3):
    """Flow principal qui s'exécute sur le serveur Docker"""
    
    print(f"🚀 Démarrage flow avec {nb_etapes} étapes")
    print(f"🎯 API URL utilisée: {os.environ.get('PREFECT_API_URL')}")
    
    # Test de connectivité depuis le flow
    conn_test = test_connection_dans_task()
    
    # Informations environnement
    env_info = info_environnement()
    
    # Étapes principales
    resultats = []
    for i in range(1, nb_etapes + 1):
        resultat = etape_principale(i)
        resultats.append(resultat)
    
    # Résumé
    resume = {
        "statut": "✅ Flow terminé avec succès !",
        "connectivite": conn_test,
        "environnement": env_info,
        "etapes": resultats,
        "total_etapes": len(resultats)
    }
    
    print("\n🎯 RÉSUMÉ FINAL :")
    print(f"   Statut: {resume['statut']}")
    print(f"   Hostname: {env_info['hostname']}")
    print(f"   Étapes: {len(resultats)}")
    print(f"   API: {env_info['prefect_api_url']}")
    
    return resume

# =========================================
# EXÉCUTION PRINCIPALE
# =========================================

if __name__ == "__main__":
    print("🎯 FLOW FORCÉ VERS SERVEUR DOCKER")
    print("=" * 60)
    
    print(f"📋 Configuration finale :")
    print(f"   PREFECT_API_URL: {os.environ.get('PREFECT_API_URL')}")
    print(f"   Ephemeral mode: {os.environ.get('PREFECT_SERVER_ALLOW_EPHEMERAL_MODE')}")
    
    print(f"\n🏃 Lancement du flow...")
    print("=" * 40)
    
    try:
        # Exécuter le flow
        resultat = flow_force_serveur(nb_etapes=2)
        
        print("\n" + "=" * 60)
        print("🎉 FLOW TERMINÉ AVEC SUCCÈS !")
        print("=" * 60)
        print("🔍 Points de vérification :")
        print("   1. Pas de message 'Starting temporary server'")
        print("   2. Flow visible dans http://localhost:4200")
        print("   3. Hostname différent de votre machine")
        print("")
        print("🌐 Vérifiez dans l'interface : http://localhost:4200")
        print("📋 Onglet 'Flow Runs' → 'flow-force-serveur'")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ ERREUR dans le flow : {e}")
        print("\n🔧 Diagnostic :")
        print("   • Le serveur Docker est-il toujours accessible ?")
        print("   • Y a-t-il des erreurs dans les logs du serveur ?")
        print(f"   • Testez: curl http://localhost:4200/api/health")
        
        import traceback
        print(f"\n📋 Stack trace complète :")
        traceback.print_exc()