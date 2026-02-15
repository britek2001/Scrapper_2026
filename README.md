# Projet Web Scraping — Documentation

![Architecture](./Captura%20de%20pantalla%202026-02-12%20a%20las%2018.32.48.png)

## .1 Architecture logicielle et pipeline de collecte

Nous avons conçu une architecture logicielle modulaire permettant de réalimenter
automatiquement les scrapers avec de nouvelles données. Cette approche facilite la mise
à jour continue du corpus et garantit la reproductibilité d’autres analyses.

Les différents scrapers ont été développés selon une logique de résilience, reposant
sur des fichiers de configuration au format JSON. Cette configuration centralisée permet
d’adapter dynamiquement les paramètres de collecte (mots-clés, périodes temporelles,
sources ou limites de requêtes) sans modifier directement le code source.

Par ailleurs, chaque module de collecte est exécuté de manière indépendante. Cette
séparation fonctionnelle permet de limiter l’impact des erreurs : en cas d’échec d’un
composant, les autres processus continuent de fonctionner normalement. Un mécanisme de
reprise automatique assure également la continuité de la collecte après interruption.

Les données collectées sont ensuite normalisées et structurées afin d’alimenter un
pipeline d’analyse dédié. Cette organisation facilite leur exploitation pour la
génération automatique de visualisations et de graphiques analytiques, permettant
d’illustrer de manière claire les dynamiques observées.

Cette architecture constitue ainsi une base robuste et évolutive pour l’analyse de flux
informationnels en temps réel, tout en garantissant la qualité, la cohérence et
l’exploitabilité des données.

---

## Environnement virtuel (venv)

Pour isoler les dépendances, créez et activez un environnement virtuel puis installez les requirements.

Linux / macOS (bash/zsh):
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
# Lancer le scraper
python FACEBOOK_SELENIUM/facebook_infinite_scroll.py
```

Windows (PowerShell):
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
# Lancer le scraper
python .\FACEBOOK_SELENIUM\facebook_infinite_scroll.py
```

Pour quitter l'environnement virtuel:
```bash
deactivate
```

---

Executing a update : 

python3 tools/match_config_generator.py --match-config tools/central_config_example.json --non-interactive --name "AFCON_Egypt_vs_Algeria_2026" --queries "Egypt Algeria match report;AFCON Egypt Algeria highlights;Egypt vs Algeria full match"
