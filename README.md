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
- univers LP utile : 500 marchés les mieux récompensés, totalité des marchés
  sponsorisés, sélection suivie et paramètres de récompense ;
- métadonnées CLOB/Gamma, frais, dates, catégories et sources de résolution ;
- checkpoints REST des carnets toutes les cinq minutes pour réparer les trous ;
- scores sportifs publics et prix de référence crypto Polymarket RTDS ;
- connexions, déconnexions et changements d'abonnement explicitement horodatés ;
- heure de réception locale en nanosecondes pour mesurer les délais.

Les fichiers terminés sont écrits sous la forme :

```text
data/
  market_ws/YYYY/MM/DD/market_ws-....jsonl.gz
  discovery/YYYY/MM/DD/discovery-....jsonl.gz
  sports_ws/YYYY/MM/DD/sports_ws-....jsonl.gz
  rtds/YYYY/MM/DD/rtds-....jsonl.gz
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
2. une fenêtre de collecte de 5 h 38 qui met son successeur en attente avant de
   commencer à collecter ;
3. un dataset Hugging Face public pour les archives. Les fichiers locaux sont
   supprimés uniquement après la réussite du commit distant.

Le workflow se trouve dans `.github/workflows/collect.yml`. Il faut ajouter au
dépôt le secret `HF_TOKEN`, limité en écriture au dataset indiqué par
`HF_DATASET_REPO`, puis lancer une première fois le workflow manuellement.

GitHub limite chaque tâche hébergée à six heures. Le script arrête donc le
collecteur proprement avant cette limite, compresse la dernière tranche et
l'envoie avant la destruction du runner. Dès le début de chaque cycle, le
workflow utilise son jeton GitHub temporaire pour déclencher un
`workflow_dispatch`. Le groupe de concurrence garde ce successeur en attente au
lieu de lancer deux collecteurs simultanés. Les modifications du code ne
déclenchent pas le collecteur et ne remplacent donc plus le successeur en
attente. Une planification horaire à la minute 47 sert uniquement de watchdog ;
elle peut remplacer le run en attente par un run plus récent sans toucher au
collecteur actif. Un heartbeat hebdomadaire maintient une activité légitime
dans le dépôt, car GitHub désactive sinon les planifications des dépôts publics
inactifs après 60 jours.

Les runners standards GitHub sont gratuits pour les dépôts publics. Ne pas
choisir de « larger runner » et ne pas ajouter de moyen de paiement. Le service
reste du best effort : une exécution planifiée peut exceptionnellement être
retardée ou abandonnée par GitHub. La chaîne par `workflow_dispatch` évite de
dépendre de cette planification pour chaque transition réussie, mais le passage
d'un runner au suivant peut encore créer une courte interruption de quelques
secondes ou minutes.

Chaque relais provoque nécessairement une courte coupure WebSocket. Les
événements de contrôle permettent de la mesurer et les snapshots REST publics
du carnet, enregistrés toutes les cinq minutes, réinitialisent ensuite l'état du
carnet. Ils ne peuvent toutefois pas recréer les transactions ou changements
intermédiaires qui n'ont jamais été reçus.

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

## Pipeline analytique : Parquet, carnets et backtests

Les dépendances analytiques sont optionnelles afin de ne pas ralentir le
collecteur GitHub :

```bash
python -m pip install -e '.[analytics]'
```

### 1. Conversion et contrôle qualité

```bash
polymarket-etl data/ --output-dir analytics/normalized
```

Cette commande produit :

- `events.parquet` : événements WebSocket/REST, contrôles de connexion, sports
  et références crypto normalisés, avec une ligne par changement de niveau ;
- `book_levels.parquet` : niveaux des snapshots WebSocket et checkpoints REST ;
- `markets.parquet` : sélection suivie, 500 marchés les mieux récompensés et
  totalité des marchés sponsorisés, avec tokens, récompenses, métadonnées et
  paramètres de frais ;
- `quality-report.json` : lignes invalides, doublons exacts, champs manquants,
  régressions temporelles et répartition des événements.

Le traitement est effectué par lots. Les empreintes utilisées pour détecter les
doublons exacts sont conservées dans une base SQLite temporaire sur disque :
l'historique complet n'est donc pas chargé en mémoire.

### 2. Reconstruction des carnets

```bash
polymarket-reconstruct \
  --events analytics/normalized/events.parquet \
  --book-levels analytics/normalized/book_levels.parquet \
  --output analytics/books.parquet
```

Un événement `book` remplace entièrement l'état d'un token. Chaque
`price_change` remplace ensuite la taille agrégée du niveau correspondant ; une
taille nulle supprime le niveau. Le fichier de qualité associé signale notamment
les mises à jour reçues avant un snapshot, les carnets croisés, les écarts de
temps et les incohérences de meilleur bid/ask. Les notifications
`best_bid_ask`, qui ne contiennent aucune taille, sont utilisées pour supprimer
les niveaux devenus impossibles mais ne déclenchent un snapshot qu'après le
`price_change` porteur de profondeur qui les accompagne.

### 3. Backtest LP conservateur

```bash
polymarket-backtest \
  --events analytics/normalized/events.parquet \
  --book-levels analytics/normalized/book_levels.parquet \
  --markets analytics/normalized/markets.parquet \
  --output-dir analytics/backtest
```

Sans `--asset-id`, le token ayant le plus de transactions et au moins un
snapshot est choisi. Le simulateur applique :

- ordres post-only et délai d'activation ;
- renouvellement périodique des quotes et skew d'inventaire ;
- capital, inventaire minimum/maximum et exécutions partielles ;
- file d'attente conservatrice égale par défaut à toute la taille déjà présente
  au niveau ;
- markout à 60 secondes, drawdown et valorisation au midpoint ; un markout
  reste non résolu si l'historique se termine avant son échéance ;
- frais maker nuls et calcul du fee-equivalent des transactions.

Le rapport distingue le PnL mark-to-market total du PnL excédentaire par rapport
au scénario où l'inventaire initial aurait simplement été conservé. C'est ce
dernier (`lp_excess_pnl_vs_hold`) qu'il faut privilégier pour juger la stratégie
de fourniture de liquidité.

Les maker rebates et récompenses LP restent à zéro par défaut : les données
publiques ne révèlent ni la position exacte de notre ordre dans la file, ni les
scores individuels de tous les makers. Des hypothèses explicites peuvent être
testées avec `--rebate-capture-rate` et `--assumed-reward-share`. Le rapport
`summary.json` conserve ces hypothèses et les limites du résultat.

## Limites importantes

- Le workflow suit les 40 meilleurs marchés récompensés en profondeur et
  archive les 500 meilleurs ainsi que tous les marchés sponsorisés à chaque
  découverte. Le snapshot contient un objet `coverage` qui rend cette frontière
  explicite. Paginer tous les marchés natifs à très faible récompense prendrait
  plus longtemps qu'un cycle et dupliquerait beaucoup de données sans valeur
  pratique pour la sélection LP. Augmenter `MAX_MARKETS` accroît directement
  le trafic et le stockage.
- Les données publiques ne contiennent pas les ordres, exécutions, scores de
  récompense ni position de file propres à un portefeuille. Il faudra ajouter
  plus tard le canal utilisateur authentifié pour le paper/live trading.
- Les données on-chain historiques (trades, positions et résolutions) peuvent
  être jointes depuis les subgraphs Goldsky publics ; il est inutile de les
  recopier à haute fréquence dans ce collecteur.
- Une VM gratuite aux États-Unis convient à la collecte, pas au futur bot de
  trading sensible à la latence.
- Le service conserve les messages bruts : les transformations en Parquet et
  l'entraînement se feront sur l'ordinateur équipé de la RTX 3060.
