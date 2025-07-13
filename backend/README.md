# Enube Backend

Projeto desenvolvido durante o processo seletivo da Enube para a vaga de backend pleno. O objetivo principal é importar um arquivo Excel com mais de 70 mil linhas e inserir os dados no banco de forma eficiente, além de realizar a normalização desses dados.

## Arquitetura do Banco de Dados

Neste projeto, utilizei o modelo de esquema estrela, criando uma tabela fato e várias tabelas dimensão associadas. Para visualizar a estrutura do banco, confira a imagem na pasta `/docs`.

## Estrutura do Projeto

O projeto está dividido em três subprojetos:

- **importer**: responsável pela importação dos dados do arquivo Excel.
- **migrate**: gerencia as migrações do banco de dados.
- **server**: API REST para acessar os dados importados.

## Como Executar

1. Copie o conteúdo do arquivo `.env.example` para um novo arquivo `.env` na raiz do projeto e preencha com os dados necessários.
2. Suba o banco de dados via Docker executando na raiz do projeto:

   ```
   docker compose up -d
   ```

3. Execute as migrações para criar as tabelas no banco:

   ```
   go run cmd/migrate_up/migrate_up.go
   ```

## Pontos de Entrada

Na pasta `/cmd` estão os comandos principais para rodar cada parte do projeto:

- Rodar migrações:

  ```
  go run cmd/migrate_up/migrate_up.go
  ```

- Reverter migrações:

  ```
  go run cmd/migrate_down/migrate_down.go
  ```

- Executar o importador:

  ```
  go run cmd/import_data/import_data.go
  ```

- Subir o servidor:

  ```
  go run cmd/start_server/start_server.go
  ```

  O servidor ficará disponível em [http://localhost:8080](http://localhost:8080)

## Documentação da API

Na pasta `/docs` você encontra a coleção do Postman com os endpoints da aplicação:

- `/auth/login`
- `/customers`
- `/partners`
- `/products`
- `/months_charge_dates`
- `/usage_dates`
- `/billing_currencies`
- `/pricing_currencies`
- `/resource_locations`
- `/services`
