include .env
MIGRATIONS_PATH = ./migrate/migrations

.PHONY: test
test:
	@go test -v ./...

.PHONY: install-migrate
install-migrate:
	@go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

.PHONY: migrate-create
migration:
	@migrate create -seq -ext sql -dir $(MIGRATIONS_PATH) $(filter-out $@,$(MAKECMDGOALS))

.PHONY: migrate-up
migrate-up:
	@migrate -path=$(MIGRATIONS_PATH) -database=$(DB_URL) up

.PHONY: migrate-down
migrate-down:
	@migrate -path=$(MIGRATIONS_PATH) -database=$(DB_URL) down $(filter-out $@,$(MAKECMDGOALS))

.PHONY: start-imports
start-imports:
	@go run cmd/import_data/import_data.go

.PHONY: start-imports-v2
start-imports-v2:
	@go run cmd/import_data/import_data_v2.go	

.PHONY: start-server
start-server:
	@echo server listening on http://localhost:${SERVER_PORT}/api/v1/health
	@go run cmd/start_server/start_server.go