# Mémoire mesurée (phase 1)

Mesure du socle (profil `streaming` : Kafka + PostgreSQL) avec `docker stats`, pour vérifier le
budget du PRD §8.4 avant d'ajouter Spark. Machine : portable Windows 11, i7-1355U, 15,6 Go de RAM,
Docker sous WSL2 plafonné à 8 Go.

## Protocole

1. Base vide : `make reset`, puis `make up` (topics recréés, tables vides).
2. **Repos** : 2 minutes après `make up`, `make stats`.
3. **Charge** : 100 000 messages de 300 octets à 1 000 messages/s sur le topic `transactions`
   (`kafka-producer-perf-test.sh`, `acks=all`), soit environ 100 s d'envoi. `docker stats` est
   relevé toutes les 5 s pendant l'envoi ; le pic du broker est lu dans `memory.peak` (cgroup v2).
4. Le producteur de test tourne dans un **conteneur à part** (`docker run --rm`, sur le réseau du
   projet) : sa JVM n'est pas comptée dans la mémoire du conteneur `kafka`.
5. Après la mesure : `make reset` (fait le 25/11/2026). Les messages de test ne sont pas du JSON : ils seraient comptés
   comme invalides par le premier consommateur (phase 3).

## Résultats (25/11/2026)

Conditions : Chrome fermé ; le PostgreSQL du projet 1 (`imf-postgres`) tournait, il n'est pas
compté.

| Conteneur | Repos (2 min après `up`) | Sous charge, régime stable | Sous charge, pic `docker stats` | `memory.peak` |
|---|---|---|---|---|
| `kafka` (tas 512 Mo) | 300 MiB | ≈ 557 MiB | 597 MiB | 737 MiB (772 841 472 o, cache de pages compris) |
| `postgres` | 36 MiB | 35 à 38 MiB | 38 MiB | — |
| **Total** | **≈ 336 MiB** | **≈ 595 MiB** | **≈ 635 MiB** | — |

Mémoire de la machine virtuelle WSL2 (`free`) :

| | `used` | `available` |
|---|---|---|
| Avant | 1,6 Gi | 5,9 Gi |
| Après la charge | 2,2 Gi | 5,3 Gi |

Producteur de test (100 000 messages × 300 octets, `--throughput 1000`, `acks=all`) :

| Débit | Latence p50 | p95 | p99 | max |
|---|---|---|---|---|
| 1 007,7 msg/s | 5 ms | 12 ms | 633 ms | 1 360 ms |

- **1 000 msg/s est un débit imposé** (`--throughput 1000`), pas une capacité mesurée : le
  producteur s'est limité à ce rythme, le broker n'a pas été poussé à saturation.
- Le p99 et le max viennent du démarrage du producteur (premières requêtes, métadonnées).
- CPU : pics de 140 à 219 % du conteneur `kafka` environ toutes les 30 s pendant la charge ; cause
  non établie (ramasse-miettes ?), à examiner seulement si Spark en souffre.

## Lecture

- Kafka tient dans son budget : tas plafonné à 512 Mo, conteneur sous 600 MiB en charge,
  737 MiB au pic cgroup en comptant le cache de pages. Le plan B Redpanda n'est pas nécessaire.
- Le socle (Kafka + PostgreSQL) occupe environ 0,6 Go en charge, sur un budget de 4 à 4,5 Go pour
  tout le profil `streaming` (PRD §8.4) : il reste de la place pour Spark (driver de 2 Go) et
  Grafana, à mesurer en phase 3.
- L'écart de `free` (+0,6 Gi de `used`) concerne toute la machine virtuelle WSL2 : il peut inclure
  la JVM du conteneur de test et d'autres processus ; il n'est pas imputable au seul socle.
