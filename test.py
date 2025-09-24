#!/usr/bin/env python3
"""
🔍 TEST CONNECTIVITÉ PYTHON
===========================

Compare curl (qui fonctionne) vs Python/requests
"""

import os
import requests
import subprocess

def test_curl():
    """Test avec curl (comme vous avez fait)"""
    print("1️⃣ Test avec CURL :")
    try:
        result = subprocess.run(
            ['curl', '-s', '-w', '%{http_code}', 'http://localhost:4200/api/health'],
            capture_output=True,
            text=True,
            timeout=5
        )
        print(f"   Status: {result.returncode}")
        print(f"   Output: {result.stdout}")
        print(f"   ✅ CURL fonctionne" if result.returncode == 0 else f"   ❌ CURL échoue")
        return result.returncode == 0
    except Exception as e:
        print(f"   ❌ Erreur CURL: {e}")
        return False

def test_python_normal():
    """Test Python normal (avec proxies hérités)"""
    print("\n2️⃣ Test Python NORMAL (avec proxies hérités) :")
    print(f"   HTTP_PROXY: {os.environ.get('HTTP_PROXY', 'NON_DÉFINI')}")
    print(f"   NO_PROXY: {os.environ.get('NO_PROXY', 'NON_DÉFINI')}")
    
    try:
        response = requests.get("http://localhost:4200/api/health", timeout=5)
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.text[:50]}...")
        print(f"   ✅ Python normal fonctionne")
        return True
    except Exception as e:
        print(f"   ❌ Python normal échoue: {e}")
        return False

def test_python_no_proxy():
    """Test Python avec proxies explicitement désactivés"""
    print("\n3️⃣ Test Python SANS PROXY :")
    
    # Sauvegarder les proxies originaux
    original_proxies = {
        'HTTP_PROXY': os.environ.get('HTTP_PROXY'),
        'HTTPS_PROXY': os.environ.get('HTTPS_PROXY'),
        'http_proxy': os.environ.get('http_proxy'),
        'https_proxy': os.environ.get('https_proxy'),
        'NO_PROXY': os.environ.get('NO_PROXY'),
        'no_proxy': os.environ.get('no_proxy'),
    }
    
    # Forcer la désactivation des proxies
    os.environ["HTTP_PROXY"] = ""
    os.environ["HTTPS_PROXY"] = ""
    os.environ["http_proxy"] = ""
    os.environ["https_proxy"] = ""
    os.environ["NO_PROXY"] = "localhost,127.0.0.1,0.0.0.0,::1"
    os.environ["no_proxy"] = "localhost,127.0.0.1,0.0.0.0,::1"
    
    print(f"   HTTP_PROXY forcé: '{os.environ.get('HTTP_PROXY')}'")
    print(f"   NO_PROXY forcé: '{os.environ.get('NO_PROXY')}'")
    
    try:
        response = requests.get("http://localhost:4200/api/health", timeout=5)
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.text[:50]}...")
        print(f"   ✅ Python sans proxy fonctionne")
        success = True
    except Exception as e:
        print(f"   ❌ Python sans proxy échoue: {e}")
        success = False
    
    # Restaurer les proxies originaux (pour ne pas casser votre environnement)
    for key, value in original_proxies.items():
        if value is not None:
            os.environ[key] = value
        elif key in os.environ:
            del os.environ[key]
    
    return success

def test_python_session():
    """Test avec session requests configurée manuellement"""
    print("\n4️⃣ Test Python SESSION (proxy désactivé dans requests) :")
    
    # Créer une session sans proxy
    session = requests.Session()
    session.proxies = {
        'http': '',
        'https': '',
    }
    
    try:
        response = session.get("http://localhost:4200/api/health", timeout=5)
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.text[:50]}...")
        print(f"   ✅ Python session fonctionne")
        return True
    except Exception as e:
        print(f"   ❌ Python session échoue: {e}")
        return False

def test_prefect_client():
    """Test avec le client Prefect directement"""
    print("\n5️⃣ Test CLIENT PREFECT :")
    
    # Forcer la config Prefect
    os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"
    os.environ["HTTP_PROXY"] = ""
    os.environ["HTTPS_PROXY"] = ""
    
    try:
        from prefect.client.orchestration import get_client
        import asyncio
        
        async def test_client():
            async with get_client() as client:
                # Test simple de connectivité
                health = await client._client.get("/health")
                return health.status_code == 200
        
        result = asyncio.run(test_client())
        print(f"   ✅ Client Prefect fonctionne: {result}")
        return result
        
    except Exception as e:
        print(f"   ❌ Client Prefect échoue: {e}")
        return False

if __name__ == "__main__":
    print("🔍 DIAGNOSTIC CONNECTIVITÉ PYTHON vs CURL")
    print("=" * 60)
    
    print(f"🏢 Variables proxy Carrefour actuelles :")
    for var in ['HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY']:
        value = os.environ.get(var, 'NON_DÉFINI')
        print(f"   {var}: {value}")
    
    print("\n" + "=" * 60)
    
    # Exécuter tous les tests
    tests = [
        ("CURL", test_curl),
        ("Python normal", test_python_normal),
        ("Python sans proxy", test_python_no_proxy),
        ("Python session", test_python_session),
        ("Client Prefect", test_prefect_client)
    ]
    
    results = {}
    for name, test_func in tests:
        results[name] = test_func()
    
    print("\n" + "=" * 60)
    print("📊 RÉSUMÉ DES TESTS :")
    print("=" * 60)
    
    for name, success in results.items():
        status = "✅ RÉUSSI" if success else "❌ ÉCHEC"
        print(f"   {name:<20} : {status}")
    
    print("\n💡 RECOMMANDATION :")
    if results.get("Python sans proxy", False):
        print("   🎯 Solution : Forcer la désactivation des proxies dans vos scripts Python")
        print("   📝 Utilisez le script flow_fixed.py avec configuration proxy désactivée")
    elif results.get("Python session", False):
        print("   🎯 Solution : Utiliser une session requests sans proxy")
        print("   📝 Modifier vos scripts pour utiliser session.proxies = {'http': '', 'https': ''}")
    else:
        print("   🔧 Problème plus profond - vérifiez la configuration réseau Docker")
        print("   💡 Essayez d'exécuter le script depuis le conteneur Docker directement")
    
    print(f"\n🌐 Interface Prefect : http://localhost:4200")