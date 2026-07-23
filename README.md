# Collecteur de données LP Polymarket

Ce projet enregistre en continu les données publiques de niveau 2 des marchés
Polymarket qui distribuent des récompenses de liquidité. Il est conçu pour une
petite VM Google Cloud `e2-micro` : mémoire limitée, fichiers rotatifs et aucune
base de données résidente.

Il ne signe aucun ordre, ne contient aucune clé de portefeuille et ne peut pas
trader. Son unique rôle est de créer un historique propre pour les futurs
backtests et modèles d'apprentissage automatique.

## Données collectées

- carnets complets (`book`) ;
- changements de niveaux (`price_change`) ;
- meilleurs bid/ask ;
- dernières transactions ;
- changements de tick et résolutions ;
- métadonnées, volumes et paramètres de récompenses LP ;
- heure de réception locale en nanosecondes pour mesurer les délais.

Les fichiers terminés sont écrits sous la forme :

```text
data/
  market_ws/YYYY/MM/DD/market_ws-....jsonl.gz
  discovery/YYYY/MM/DD/discovery-....jsonl.gz
```

Chaque ligne est un objet JSON autonome. Les fichiers `.part` sont récupérés et
compressés automatiquement au prochain démarrage après une coupure brutale.

## Exécution locale

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
cp .env.example .env
DATA_DIR="$PWD/data" polymarket-collector
```

Tester uniquement l'accès aux API publiques avant de démarrer :

```bash
polymarket-collector --check
```

État du service :

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/metrics
```

Arrêter avec `Ctrl+C`. Le service finalise alors proprement les fichiers en
cours.

## Déploiement Google Cloud

Depuis un ordinateur ou Google Cloud Shell ayant `gcloud` authentifié :

```bash
./scripts/deploy_gcp.sh ID_DU_PROJET_GCP
```

Le script crée par défaut une VM `e2-micro` Ubuntu 24.04 dans `us-east1-b`, un
disque standard de 30 Go, copie le projet, puis installe et démarre le service.

Commandes utiles après installation :

```bash
gcloud compute ssh polymarket-collector --zone us-east1-b
sudo systemctl status polymarket-collector
sudo journalctl -u polymarket-collector -f
curl http://127.0.0.1:8080/healthz
```

## Sauvegarde vers Google Drive

Sur la VM :

```bash
sudo rclone config
sudoedit /etc/polymarket-collector.env
```

Définir ensuite, par exemple :

```text
RCLONE_REMOTE=gdrive:polymarket-data
LOCAL_RETENTION_DAYS=7
```

Puis activer la sauvegarde :

```bash
sudo systemctl enable --now polymarket-backup.timer
sudo systemctl start polymarket-backup.service
sudo journalctl -u polymarket-backup.service -n 100
```

Les fichiers locaux âgés de plus de `LOCAL_RETENTION_DAYS` ne sont supprimés
qu'après une copie `rclone` réussie. Mettre la valeur à `0` pour désactiver le
nettoyage local.

## Hébergement gratuit sans dépassement : GitHub Actions

L'architecture recommandée pour une collecte quasi continue sans VM
personnelle est :

1. un dépôt GitHub public utilisant uniquement le runner standard
   `ubuntu-latest` ;
2. une fenêtre de collecte de 5 h 38 qui met immédiatement son successeur en
   attente avant de se terminer ;
3. un dataset Hugging Face public pour les archives. Les fichiers locaux sont
   supprimés uniquement après la réussite du commit distant.

Le workflow se trouve dans `.github/workflows/collect.yml`. Il faut ajouter au
dépôt le secret `HF_TOKEN`, limité en écriture au dataset indiqué par
`HF_DATASET_REPO`, puis lancer une première fois le workflow manuellement.

GitHub limite chaque tâche hébergée à six heures. Le script arrête donc le
collecteur proprement avant cette limite, compresse la dernière tranche et
l'envoie avant la destruction du runner. Après un cycle réussi, le workflow
utilise son jeton GitHub temporaire pour déclencher un `workflow_dispatch`.
Le groupe de concurrence garde ce successeur en attente au lieu de lancer deux
collecteurs simultanés. Une planification horaire à la minute 47 sert uniquement
de watchdog si un cycle échoue avant de pouvoir créer son successeur. Un
heartbeat hebdomadaire maintient une activité légitime dans le dépôt, car GitHub
désactive sinon les planifications des dépôts publics inactifs après 60 jours.

Les runners standards GitHub sont gratuits pour les dépôts publics. Ne pas
choisir de « larger runner » et ne pas ajouter de moyen de paiement. Le service
reste du best effort : une exécution planifiée peut exceptionnellement être
retardée ou abandonnée par GitHub. La chaîne par `workflow_dispatch` évite de
dépendre de cette planification pour chaque transition réussie, mais le passage
d'un runner au suivant peut encore créer une courte interruption de quelques
secondes ou minutes.

Hugging Face affiche encore `CPU Basic` à 0 $ dans certaines documentations,
mais l'API refuse désormais la création d'un Space Docker pour un compte gratuit
sans abonnement PRO. `scripts/deploy_hf.py` n'est donc conservé que pour les
comptes qui possèdent déjà cet abonnement.

Koyeb n'est plus recommandé : depuis son rapprochement avec Mistral, les
nouveaux utilisateurs doivent fournir un moyen de paiement et souscrire un
forfait payant.

## Tests

```bash
python -m unittest discover -s tests -v
```

Les tests ne contactent pas Polymarket et ne nécessitent aucune clé.

## Limites importantes

- Avec 1 Go de RAM et 30 Go de disque, commencer avec 75 marchés maximum.
- Une VM gratuite aux États-Unis convient à la collecte, pas au futur bot de
  trading sensible à la latence.
- Le service conserve les messages bruts : les transformations en Parquet et
  l'entraînement se feront sur l'ordinateur équipé de la RTX 3060.
