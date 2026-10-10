import 'dotenv/config';
import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const databasePath = resolve(process.env.DATABASE_PATH ?? 'data/resources.sqlite');
mkdirSync(dirname(databasePath), { recursive: true });

export const database = new DatabaseSync(databasePath);

database.exec(`
  CREATE TABLE IF NOT EXISTS resources (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS resources_name_idx ON resources(name);
  CREATE INDEX IF NOT EXISTS resources_category_idx ON resources(category);
`);
