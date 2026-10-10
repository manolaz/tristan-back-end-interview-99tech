import 'dotenv/config';
import { readFile } from 'node:fs/promises';
import { database } from '../src/database.js';

interface SampleResource {
  id: string;
  name: string;
  description: string;
  category: string;
  createdAt: string;
  updatedAt: string;
}

function isSampleResource(value: unknown): value is SampleResource {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  return (
    typeof record.id === 'string' &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(record.id) &&
    typeof record.name === 'string' &&
    record.name.trim().length > 0 &&
    record.name.trim().length <= 120 &&
    typeof record.description === 'string' &&
    record.description.length <= 2000 &&
    typeof record.category === 'string' &&
    record.category.length <= 100 &&
    typeof record.createdAt === 'string' &&
    !Number.isNaN(Date.parse(record.createdAt)) &&
    typeof record.updatedAt === 'string' &&
    !Number.isNaN(Date.parse(record.updatedAt))
  );
}

async function seedSampleData(): Promise<void> {
  const contents = await readFile(new URL('../data-init/resources.json', import.meta.url), 'utf8');
  const samples: unknown = JSON.parse(contents);
  if (!Array.isArray(samples) || !samples.every(isSampleResource)) {
    throw new Error('data-init/resources.json must contain an array of valid resources.');
  }

  const insert = database.prepare(`
    INSERT OR IGNORE INTO resources (id, name, description, category, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `);

  let inserted = 0;
  database.exec('BEGIN IMMEDIATE');
  try {
    for (const sample of samples) {
      inserted += Number(insert.run(
        sample.id,
        sample.name,
        sample.description,
        sample.category,
        sample.createdAt,
        sample.updatedAt,
      ).changes);
    }
    database.exec('COMMIT');
  } catch (error) {
    database.exec('ROLLBACK');
    throw error;
  }

  console.log(`Seeded ${inserted} new sample resource(s); ${samples.length - inserted} already existed.`);
}

seedSampleData()
  .catch((error: unknown) => {
    console.error('Failed to seed sample data:', error);
    process.exitCode = 1;
  })
  .finally(() => database.close());
