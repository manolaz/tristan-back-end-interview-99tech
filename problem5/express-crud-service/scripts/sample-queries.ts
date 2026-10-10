import 'dotenv/config';

interface Resource {
  id: string;
  name: string;
  description: string;
  category: string;
  createdAt: string;
  updatedAt: string;
}

interface ApiResponse<T> {
  data: T;
  pagination?: {
    total: number;
    limit: number;
    offset: number;
  };
}

const baseUrl = (process.env.API_BASE_URL ?? 'http://localhost:3000/api/resources')
  .replace(/\/+$/, '');

async function request<T>(path = '', init?: RequestInit): Promise<ApiResponse<T> | null> {
  const response = await fetch(`${baseUrl}${path}`, init);
  if (response.status === 204) return null;

  const payload: unknown = await response.json();
  if (!response.ok) {
    throw new Error(`${init?.method ?? 'GET'} ${path || '/'} failed (${response.status}): ${JSON.stringify(payload)}`);
  }
  return payload as ApiResponse<T>;
}

function print<T>(label: string, result: ApiResponse<T> | null): void {
  console.log(`\n${label}`);
  console.log(JSON.stringify(result, null, 2));
}

async function runSampleQueries(): Promise<void> {
  const listing = await request<Resource[]>('?limit=5&offset=0');
  print('List resources', listing);

  const filtered = await request<Resource[]>('?category=electronics&name=keyboard');
  print('Filter electronics by a partial name', filtered);

  const created = await request<Resource>('', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      name: 'Sample API resource',
      description: 'Created by scripts/sample-queries.ts',
      category: 'sample',
    }),
  });
  if (!created) throw new Error('Create request did not return a resource.');

  const resourceId = created.data.id;
  let shouldDelete = true;
  try {
    print('Create resource', created);

    const detail = await request<Resource>(`/${encodeURIComponent(resourceId)}`);
    print('Get resource details', detail);

    const patched = await request<Resource>(`/${encodeURIComponent(resourceId)}`, {
      method: 'PATCH',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ description: 'Updated with PATCH by the sample script.' }),
    });
    print('Patch resource details', patched);

    const replaced = await request<Resource>(`/${encodeURIComponent(resourceId)}`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        name: 'Sample API resource (replaced)',
        description: 'Replaced with PUT by the sample script.',
        category: 'sample',
      }),
    });
    print('Replace resource details', replaced);

    print('Delete resource', await request<never>(`/${encodeURIComponent(resourceId)}`, {
      method: 'DELETE',
    }));
    shouldDelete = false;
  } finally {
    if (shouldDelete) {
      await request<never>(`/${encodeURIComponent(resourceId)}`, { method: 'DELETE' });
      console.log('\nCleaned up the temporary sample resource.');
    }
  }
}

runSampleQueries().catch((error: unknown) => {
  console.error('Sample API queries failed:', error);
  process.exitCode = 1;
});
