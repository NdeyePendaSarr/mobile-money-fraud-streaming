# SenPay : commandes du projet. `make help` liste les cibles.
# Les variables de .env (jamais versionné, voir .env.example) sont passées à
# Docker Compose et aux scripts Python ; ENV_FILE permet d'en choisir un autre.

ENV_FILE ?= .env
PROFILE ?= streaming
CHECKPOINT_DIR ?= checkpoints

COMPOSE := docker compose --env-file $(ENV_FILE)
# down et reset visent les deux profils, quel que soit celui qui tourne
COMPOSE_TOUS := $(COMPOSE) --profile streaming --profile batch
# Les outils Kafka lancés dans le conteneur gardent un petit tas : sans ça, ils
# hériteraient de -Xms512m et fausseraient la mémoire du broker
KAFKA_OUTIL := $(COMPOSE) --profile streaming exec -T -e KAFKA_HEAP_OPTS="-Xms32m -Xmx128m" kafka
UV_RUN := uv run --env-file $(ENV_FILE)

# Services démarrés par make up. Le job Spark n'en fait pas partie : il démarre
# à part (make stream), pour pouvoir le lancer après coup sur un arriéré (run A)
UP_SERVICES_streaming := kafka kafka-init postgres
UP_SERVICES_batch := postgres
# Modules du job importés avec le python3 de l'image (Python 3.10)
JOB_MODULES := streaming.job, streaming.validation, streaming.sinks

.DEFAULT_GOAL := help
.PHONY: help env install up down reset topics stats generate stream stream-kill stream-logs \
	stream-report check lint typecheck test test-integration check-image infra

help: ## Liste les commandes
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

# Docker Compose et uv donnent la priorité aux variables du shell sur $(ENV_FILE) :
# un POSTGRES_PORT=5433 exporté pour le projet 1 a déjà écrasé le .env. On refuse
# donc de lancer quoi que ce soit si une variable POSTGRES_* ou SENPAY_* est
# exportée. Seuls les noms sont affichés, jamais les valeurs (mot de passe).
env:
	@test -f $(ENV_FILE) || { echo "$(ENV_FILE) absent : cp .env.example .env, puis choisir un mot de passe"; exit 1; }
	@vars=$$(env | grep -oE '^(POSTGRES|SENPAY)_[A-Z0-9_]*=' | tr -d '=' | sort | tr '\n' ' '); \
	if [ -n "$$vars" ]; then \
	  echo "ÉCHEC : variables exportées dans le shell : $$vars"; \
	  echo "Elles écraseraient $(ENV_FILE). À retirer avec : unset $$vars"; \
	  exit 1; \
	fi

install: ## Installe l'environnement Python (uv, Python 3.12)
	uv sync

# --wait rend la main quand kafka-init a seulement démarré : `docker wait` attend
# sa fin et renvoie son vrai code de sortie (un conteneur en cours renvoie 0).
up: env ## Démarre Kafka et PostgreSQL du profil PROFILE (streaming par défaut), sans le job
	$(COMPOSE) --profile $(PROFILE) up -d --wait $(UP_SERVICES_$(PROFILE))
	@if [ "$(PROFILE)" = "streaming" ]; then \
	  code=$$(docker wait $$($(COMPOSE) --profile streaming ps -a -q kafka-init)); \
	  $(COMPOSE) --profile streaming logs --no-log-prefix kafka-init | grep -v '^$$'; \
	  echo "kafka-init terminé avec le code $$code"; test "$$code" = "0"; \
	fi

down: env ## Arrête tous les services (données conservées)
	$(COMPOSE_TOUS) down --remove-orphans

# down -v supprime aussi le volume du checkpoint Spark (spark_checkpoints, I6) et
# le cache Ivy ; rm -rf vise un éventuel dossier local de checkpoints
reset: env ## Arrête tout, supprime les volumes (topics, tables, checkpoint Spark)
	$(COMPOSE_TOUS) down -v --remove-orphans
	rm -rf $(CHECKPOINT_DIR)

topics: env ## Liste et décrit les topics Kafka
	$(KAFKA_OUTIL) /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list
	$(KAFKA_OUTIL) /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --describe --exclude-internal

stats: ## Mémoire et CPU des conteneurs (instantané)
	docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}'

# Générateur lancé sur l'hôte (H17), vers 127.0.0.1:9092 et 127.0.0.1:5434
generate: env ## Lance le générateur : make generate ARGS="--seed 42 --start … --duration 1d"
	$(UV_RUN) python -m producer.producer $(ARGS)

# Job Spark (phase 3), dans le conteneur spark-streaming (profil streaming)
stream: env ## Démarre le job Spark (construit l'image si besoin)
	$(COMPOSE) --profile streaming up -d --build spark-streaming
	@echo "Job démarré : make stream-logs pour suivre les lots, make stream-report pour le bilan"

stream-kill: env ## Tue le job Spark (SIGKILL, sans arrêt propre) ; make stream le relance
	$(COMPOSE) --profile streaming kill spark-streaming

stream-logs: env ## Suit le journal du job (une ligne par micro-batch)
	$(COMPOSE) --profile streaming logs -f --no-log-prefix spark-streaming

stream-report: env ## Chiffres du critère de la phase 3 (envoyés, insérés, invalides, doublons)
	$(UV_RUN) python -m scripts.stream_report

check: lint typecheck test check-image infra ## Lint, types, tests, imports dans l'image, infrastructure

lint:
	uv run ruff format --check scripts tests producer streaming
	uv run ruff check scripts tests producer streaming

# Deux passes : tout le dépôt en Python 3.12, puis le code du job en 3.10 (Python
# de l'image Spark) ; ses tests (streaming/tests) tournent sur l'hôte, en 3.12
typecheck:
	uv run mypy
	uv run mypy --python-version 3.10 streaming/*.py

# Import réel des modules du job avec le python3 de l'image : une syntaxe ou une
# bibliothèque absente de Python 3.10 échoue ici, pas au démarrage du job
check-image: env
	$(COMPOSE) --profile streaming run --rm --no-deps --build --entrypoint python3 \
	  spark-streaming -c "import sys, $(JOB_MODULES); print('imports OK, Python', sys.version.split()[0])"

# Les tests d'intégration écrivent un run en base : hors de make check
test:
	uv run pytest -q -m "not integration"

test-integration: env ## Tests d'intégration (services démarrés ; écrit un run en base)
	$(UV_RUN) pytest -q -m integration

infra: env ## Ports publiés, topics, 10 tables, detected_at immuable (services démarrés)
	$(UV_RUN) python -m scripts.check_infra
