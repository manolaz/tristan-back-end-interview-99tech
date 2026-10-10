import 'dotenv/config';
const baseUrl = (process.env.API_BASE_URL ?? 'http://localhost:3000/api/resources')
    .replace(/\/+$/, '');
async function request(path = '', init) {
    const response = await fetch(`${baseUrl}${path}`, init);
    if (response.status === 204)
        return null;
    const payload = await response.json();
    if (!response.ok) {
        throw new Error(`${init?.method ?? 'GET'} ${path || '/'} failed (${response.status}): ${JSON.stringify(payload)}`);
    }
    return payload;
}
function print(label, result) {
    console.log(`\n${label}`);
    console.log(JSON.stringify(result, null, 2));
}
async function runSampleQueries() {
    const listing = await request('?limit=5&offset=0');
    print('List resources', listing);
    const filtered = await request('?category=electronics&name=keyboard');
    print('Filter electronics by a partial name', filtered);
    const created = await request('', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
            name: 'Sample API resource',
            description: 'Created by scripts/sample-queries.ts',
            category: 'sample',
        }),
    });
    if (!created)
        throw new Error('Create request did not return a resource.');
    const resourceId = created.data.id;
    let shouldDelete = true;
    try {
        print('Create resource', created);
        const detail = await request(`/${encodeURIComponent(resourceId)}`);
        print('Get resource details', detail);
        const patched = await request(`/${encodeURIComponent(resourceId)}`, {
            method: 'PATCH',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ description: 'Updated with PATCH by the sample script.' }),
        });
        print('Patch resource details', patched);
        const replaced = await request(`/${encodeURIComponent(resourceId)}`, {
            method: 'PUT',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
                name: 'Sample API resource (replaced)',
                description: 'Replaced with PUT by the sample script.',
                category: 'sample',
            }),
        });
        print('Replace resource details', replaced);
        print('Delete resource', await request(`/${encodeURIComponent(resourceId)}`, {
            method: 'DELETE',
        }));
        shouldDelete = false;
    }
    finally {
        if (shouldDelete) {
            await request(`/${encodeURIComponent(resourceId)}`, { method: 'DELETE' });
            console.log('\nCleaned up the temporary sample resource.');
        }
    }
}
runSampleQueries().catch((error) => {
    console.error('Sample API queries failed:', error);
    process.exitCode = 1;
});
//# sourceMappingURL=sample-queries.js.map