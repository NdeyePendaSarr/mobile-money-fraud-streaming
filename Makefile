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

.DEFAULT_GOAL := help
.PHONY: help env install up down reset topics stats generate check lint typecheck test test-integration infra

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
up: env ## Démarre le profil PROFILE (streaming par défaut) et attend kafka-init
	$(COMPOSE) --profile $(PROFILE) up -d --wait
	@if [ "$(PROFILE)" = "streaming" ]; then \
	  code=$$(docker wait $$($(COMPOSE) --profile streaming ps -a -q kafka-init)); \
	  $(COMPOSE) --profile streaming logs --no-log-prefix kafka-init | grep -v '^$$'; \
	  echo "kafka-init terminé avec le code $$code"; test "$$code" = "0"; \
	fi

down: env ## Arrête tous les services (données conservées)
	$(COMPOSE_TOUS) down --remove-orphans

reset: env ## Arrête tout, supprime les volumes (topics, tables) et les checkpoints
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

check: lint typecheck test infra ## Lint, types, tests, puis contrôle de l'infrastructure (infra)

lint:
	uv run ruff format --check scripts tests producer
	uv run ruff check scripts tests producer

typecheck:
	uv run mypy

# Les tests d'intégration écrivent un run en base : hors de make check
test:
	uv run pytest -q -m "not integration"

test-integration: env ## Tests d'intégration (services démarrés ; écrit un run en base)
	$(UV_RUN) pytest -q -m integration

infra: env ## Ports publiés, topics, 8 tables, detected_at immuable (services démarrés)
	$(UV_RUN) python -m scripts.check_infra
