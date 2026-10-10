import 'dotenv/config';
import { randomUUID } from 'node:crypto';
import express, { type ErrorRequestHandler, type Request, type Response } from 'express';
import { database } from './database.js';

interface Resource {
  id: string;
  name: string;
  description: string;
  category: string;
  createdAt: string;
  updatedAt: string;
}

interface ResourceInput {
  name: string;
  description: string;
  category: string;
}

const app = express();
const port = Number(process.env.PORT ?? 3000);
const maxPageSize = 100;

app.use(express.json({ limit: '100kb' }));

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseResourceInput(value: unknown): ResourceInput | null;
function parseResourceInput(value: unknown, partial: true): Partial<ResourceInput> | null;
function parseResourceInput(
  value: unknown,
  partial = false,
): ResourceInput | Partial<ResourceInput> | null {
  if (!isRecord(value)) return null;

  const allowedFields: (keyof ResourceInput)[] = ['name', 'description', 'category'];
  if (Object.keys(value).some((key) => !allowedFields.some((field) => field === key))) return null;
  if (!partial && (typeof value.name !== 'string' || !value.name.trim())) return null;
  if (partial && Object.keys(value).length === 0) return null;

  const input: Partial<ResourceInput> = {};

  for (const field of allowedFields) {
    const fieldValue = value[field];
    if (fieldValue === undefined) continue;
    if (typeof fieldValue !== 'string') return null;

    const trimmedValue = fieldValue.trim();
    if (field === 'name' && (!trimmedValue || trimmedValue.length > 120)) return null;
    if (field === 'description' && trimmedValue.length > 2000) return null;
    if (field === 'category' && trimmedValue.length > 100) return null;
    input[field] = trimmedValue;
  }

  if (partial) return input;
  if (typeof input.name !== 'string') return null;
  return {
    name: input.name,
    description: input.description ?? '',
    category: input.category ?? '',
  };
}

function toResource(row: Record<string, unknown>): Resource {
  return {
    id: String(row.id),
    name: String(row.name),
    description: String(row.description),
    category: String(row.category),
    createdAt: String(row.created_at),
    updatedAt: String(row.updated_at),
  };
}

function getResourceId(request: Request): string | null {
  const id = request.params.id;
  return typeof id === 'string' ? id : null;
}

function sendBadRequest(response: Response): void {
  response.status(400).json({
    error: 'Invalid resource data. Provide a non-empty name (max 120 characters), an optional description (max 2000 characters), and an optional category (max 100 characters).',
  });
}

app.get('/', (_request: Request, response: Response) => {
  response.json({ message: 'Hello from Express + TypeScript!' });
});

app.post('/api/resources', (request: Request, response: Response) => {
  const input = parseResourceInput(request.body);
  if (!input) {
    sendBadRequest(response);
    return;
  }

  const now = new Date().toISOString();
  const resource: Resource = {
    id: randomUUID(),
    ...input,
    createdAt: now,
    updatedAt: now,
  };

  database.prepare(`
    INSERT INTO resources (id, name, description, category, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(
    resource.id,
    resource.name,
    resource.description,
    resource.category,
    resource.createdAt,
    resource.updatedAt,
  );

  response.status(201).json({ data: resource });
});

app.get('/api/resources', (request: Request, response: Response) => {
  const { name, category, limit, offset } = request.query;
  if (
    (name !== undefined && typeof name !== 'string') ||
    (category !== undefined && typeof category !== 'string') ||
    (limit !== undefined && typeof limit !== 'string') ||
    (offset !== undefined && typeof offset !== 'string')
  ) {
    response.status(400).json({ error: 'Filters, limit, and offset must be strings.' });
    return;
  }

  const pageSize = limit === undefined ? 20 : Number(limit);
  const pageOffset = offset === undefined ? 0 : Number(offset);
  if (
    !Number.isInteger(pageSize) ||
    pageSize < 1 ||
    pageSize > maxPageSize ||
    !Number.isInteger(pageOffset) ||
    pageOffset < 0
  ) {
    response.status(400).json({
      error: `limit must be an integer between 1 and ${maxPageSize}, and offset must be a non-negative integer.`,
    });
    return;
  }

  const filters: string[] = [];
  const parameters: (string | number)[] = [];
  if (name !== undefined && name.trim()) {
    filters.push('name LIKE ?');
    parameters.push(`%${name.trim()}%`);
  }
  if (category !== undefined && category.trim()) {
    filters.push('category = ?');
    parameters.push(category.trim());
  }

  const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
  const countRow = database.prepare(
    `SELECT COUNT(*) AS total FROM resources ${whereClause}`,
  ).get(...parameters) as { total: number };
  const rows = database.prepare(`
    SELECT id, name, description, category, created_at, updated_at
    FROM resources
    ${whereClause}
    ORDER BY created_at DESC, id ASC
    LIMIT ? OFFSET ?
  `).all(...parameters, pageSize, pageOffset);

  response.json({
    data: rows.map((row) => toResource(row)),
    pagination: {
      total: countRow.total,
      limit: pageSize,
      offset: pageOffset,
    },
  });
});

app.get('/api/resources/:id', (request: Request, response: Response) => {
  const id = getResourceId(request);
  if (id === null) {
    response.status(404).json({ error: 'Resource not found.' });
    return;
  }
  const row = database.prepare(`
    SELECT id, name, description, category, created_at, updated_at
    FROM resources WHERE id = ?
  `).get(id);

  if (!row) {
    response.status(404).json({ error: 'Resource not found.' });
    return;
  }
  response.json({ data: toResource(row) });
});

app.put('/api/resources/:id', (request: Request, response: Response) => {
  const id = getResourceId(request);
  if (id === null) {
    response.status(404).json({ error: 'Resource not found.' });
    return;
  }
  const input = parseResourceInput(request.body);
  if (!input) {
    sendBadRequest(response);
    return;
  }

  const updatedAt = new Date().toISOString();
  const row = database.prepare(`
    UPDATE resources
    SET name = ?, description = ?, category = ?, updated_at = ?
    WHERE id = ?
    RETURNING id, name, description, category, created_at, updated_at
  `).get(input.name, input.description, input.category, updatedAt, id);
  if (!row) {
    response.status(404).json({ error: 'Resource not found.' });
    return;
  }
  response.json({ data: toResource(row) });
});

app.patch('/api/resources/:id', (request: Request, response: Response) => {
  const id = getResourceId(request);
  if (id === null) {
    response.status(404).json({ error: 'Resource not found.' });
    return;
  }
  const input = parseResourceInput(request.body, true);
  if (!input) {
    sendBadRequest(response);
    return;
  }

  const existing = database.prepare(`
    SELECT id, name, description, category, created_at, updated_at
    FROM resources WHERE id = ?
  `).get(id);
  if (!existing) {
    response.status(404).json({ error: 'Resource not found.' });
    return;
  }

  const current = toResource(existing);
  const updated = {
    ...current,
    ...input,
    updatedAt: new Date().toISOString(),
  };
  database.prepare(`
    UPDATE resources
    SET name = ?, description = ?, category = ?, updated_at = ?
    WHERE id = ?
  `).run(updated.name, updated.description, updated.category, updated.updatedAt, updated.id);

  response.json({ data: updated });
});

app.delete('/api/resources/:id', (request: Request, response: Response) => {
  const id = getResourceId(request);
  if (id === null) {
    response.status(404).json({ error: 'Resource not found.' });
    return;
  }
  const result = database.prepare('DELETE FROM resources WHERE id = ?').run(id);
  if (result.changes === 0) {
    response.status(404).json({ error: 'Resource not found.' });
    return;
  }
  response.status(204).end();
});

const errorHandler: ErrorRequestHandler = (error, _request, response, _next) => {
  if (error instanceof SyntaxError && 'body' in error) {
    response.status(400).json({ error: 'Request body must contain valid JSON.' });
    return;
  }
  if (isRecord(error) && error.type === 'entity.too.large') {
    response.status(413).json({ error: 'Request body must not exceed 100kb.' });
    return;
  }
  console.error(error);
  response.status(500).json({ error: 'Internal server error.' });
};
app.use(errorHandler);

if (process.env.NODE_ENV !== 'test') {
  const server = app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
  });

  const shutdown = (): void => {
    server.close(() => {
      database.close();
      process.exit(0);
    });
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

export default app;
