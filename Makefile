PYTHON ?= python3
.DEFAULT_GOAL := help
.PHONY: help env up down restart ps logs url config native-env native-migrate native-up test test-deps test-mysql build-local-image
help:
	@echo "Demo: make up / down / ps / logs / url"
	@echo "Native: make native-env, edit .env.native, make native-migrate, make native-up"
	@echo "No make required: python3 scripts/demo.py --help"
env:
	$(PYTHON) scripts/demo.py init
up:
	$(PYTHON) scripts/demo.py up
down:
	$(PYTHON) scripts/demo.py down
restart:
	$(PYTHON) scripts/demo.py restart
ps:
	$(PYTHON) scripts/demo.py status
logs:
	$(PYTHON) scripts/demo.py logs
url:
	$(PYTHON) scripts/demo.py urls
config:
	$(PYTHON) scripts/demo.py check
native-env:
	$(PYTHON) scripts/demo.py native-init
native-migrate:
	$(PYTHON) scripts/demo.py native-migrate
native-up:
	$(PYTHON) scripts/demo.py native-up
test:
	go test ./... -count=1
	$(PYTHON) -m unittest discover -s scripts -p demo_test.py
test-mysql:
	docker compose -f deploy/test/compose.yml up -d --wait mysql
test-deps:
	docker compose -f deploy/test/compose.yml up -d --wait mysql redis kafka
	docker compose -f deploy/test/compose.yml run --rm identities
	docker compose -f deploy/test/compose.yml run --rm topics
build-local-image: env
	mkdir -p .local/bin
	@arch="$${HUB_DOCKER_ARCH:-$$(docker version --format '{{.Server.Arch}}')}"; \
	case "$$arch" in amd64|arm64) ;; *) echo "Unsupported Docker architecture: $$arch"; exit 1;; esac; \
	for app in hub mock migrate healthprobe; do CGO_ENABLED=0 GOOS=linux GOARCH="$$arch" go build -trimpath -o .local/bin/$$app ./cmd/$$app || exit; done
	$(PYTHON) -c 'from scripts.demo import ROOT, read_env, run; run(["docker", "build", "-f", "deploy/local.Dockerfile", "-t", read_env(ROOT / ".env").get("HUB_IMAGE", "partner-hub/mvp:local"), "."])'
