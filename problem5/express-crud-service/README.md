# Express CRUD Service

A TypeScript and Express REST API for managing resources, persisted in a local SQLite database.

## Project structure

The service separates HTTP concerns from business logic and persistence:

- `src/app.ts` configures Express middleware and mounts routes.
- `src/routes/` maps resource endpoints to controller handlers.
- `src/controllers/` handles HTTP input and response behavior.
- `src/services/` contains resource operations and business rules.
- `src/repositories/` encapsulates SQLite queries.
- `src/domain/` defines resource types and input validation.
- `src/middleware/` contains shared request error handling.
- `src/index.ts` starts the server and handles graceful shutdown.

## Requirements

- Node.js 22.13 or newer (for the built-in `node:sqlite` module)

## Run

From this directory:

```sh
bun install
bun run build
bun run start
```

For development with automatic restarts:

```sh
bun run dev
```

The server listens on port `3000` by default. Set `PORT` to change the port and `DATABASE_PATH` to choose a different SQLite file (default: `data/resources.sqlite`).

## Sample data and queries

With dependencies installed:

```sh
bun run db:seed
```

The seed command inserts the example resources in `data-init/resources.json`. It is safe to rerun: existing sample IDs are left unchanged. Start the server in one terminal, then in another run:

```sh
bun run sample:queries
```

The query script demonstrates listing and filtering resources, then creating, reading, patching, replacing, and deleting a temporary resource. Set `API_BASE_URL` to use a different API URL (default: `http://localhost:3000/api/resources`).

## Resource shape

```json
{
  "id": "generated UUID",
  "name": "Example",
  "description": "An optional description",
  "category": "An optional category",
  "createdAt": "ISO 8601 timestamp",
  "updatedAt": "ISO 8601 timestamp"
}
```

`name` is required and must be 1–120 characters. `description` and `category` are optional and limited to 2000 and 100 characters respectively.

## API

All resource routes are under `/api/resources`.

| Method     | Endpoint               | Description                                                                                                                                                |
| ---------- | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `POST`   | `/api/resources`     | Create a resource. Send`name`, optionally `description` and `category`.                                                                              |
| `GET`    | `/api/resources`     | List resources. Supports`name` (case-insensitive partial match), `category` (exact match), `limit` (1–100, default 20), and `offset` (default 0). |
| `GET`    | `/api/resources/:id` | Get a resource by UUID.                                                                                                                                    |
| `PUT`    | `/api/resources/:id` | Replace a resource. Requires`name`; omitted optional fields become empty strings.                                                                        |
| `PATCH`  | `/api/resources/:id` | Update one or more provided fields.                                                                                                                        |
| `DELETE` | `/api/resources/:id` | Delete a resource. Returns`204 No Content`.                                                                                                              |

Successful reads and writes return their resource(s) in a `data` property. List responses also include `pagination` with `total`, `limit`, and `offset`. Invalid input returns `400`; requests for missing resources return `404`.
