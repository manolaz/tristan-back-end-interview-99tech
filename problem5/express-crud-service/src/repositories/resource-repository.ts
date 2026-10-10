import type { SQLOutputValue } from 'node:sqlite';
import { database } from '../database.js';
import type {
  Resource,
  ResourceFilters,
  ResourceInput,
  ResourcePage,
} from '../domain/resource.js';

function toResource(row: Record<string, SQLOutputValue>): Resource {
  return {
    id: String(row.id),
    name: String(row.name),
    description: String(row.description),
    category: String(row.category),
    createdAt: String(row.created_at),
    updatedAt: String(row.updated_at),
  };
}

export class ResourceRepository {
  create(resource: Resource): void {
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
  }

  list(filters: ResourceFilters): ResourcePage {
    const clauses: string[] = [];
    const parameters: (string | number)[] = [];

    if (filters.name) {
      clauses.push('name LIKE ?');
      parameters.push(`%${filters.name}%`);
    }
    if (filters.category) {
      clauses.push('category = ?');
      parameters.push(filters.category);
    }

    const whereClause = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
    const countRow = database.prepare(
      `SELECT COUNT(*) AS total FROM resources ${whereClause}`,
    ).get(...parameters) as { total: number };
    const rows = database.prepare(`
      SELECT id, name, description, category, created_at, updated_at
      FROM resources
      ${whereClause}
      ORDER BY created_at DESC, id ASC
      LIMIT ? OFFSET ?
    `).all(...parameters, filters.limit, filters.offset);

    return {
      data: rows.map(toResource),
      pagination: {
        total: countRow.total,
        limit: filters.limit,
        offset: filters.offset,
      },
    };
  }

  findById(id: string): Resource | null {
    const row = database.prepare(`
      SELECT id, name, description, category, created_at, updated_at
      FROM resources WHERE id = ?
    `).get(id);
    return row ? toResource(row) : null;
  }

  replace(id: string, input: ResourceInput, updatedAt: string): Resource | null {
    const row = database.prepare(`
      UPDATE resources
      SET name = ?, description = ?, category = ?, updated_at = ?
      WHERE id = ?
      RETURNING id, name, description, category, created_at, updated_at
    `).get(input.name, input.description, input.category, updatedAt, id);
    return row ? toResource(row) : null;
  }

  update(id: string, resource: Resource): void {
    database.prepare(`
      UPDATE resources
      SET name = ?, description = ?, category = ?, updated_at = ?
      WHERE id = ?
    `).run(resource.name, resource.description, resource.category, resource.updatedAt, id);
  }

  delete(id: string): boolean {
    return database.prepare('DELETE FROM resources WHERE id = ?').run(id).changes > 0;
  }
}
