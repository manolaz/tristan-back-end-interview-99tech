import { randomUUID } from 'node:crypto';
import type { Resource, ResourceFilters, ResourceInput } from '../domain/resource.js';
import { ResourceRepository } from '../repositories/resource-repository.js';

export class ResourceService {
  constructor(private readonly repository = new ResourceRepository()) {}

  create(input: ResourceInput): Resource {
    const now = new Date().toISOString();
    const resource: Resource = {
      id: randomUUID(),
      ...input,
      createdAt: now,
      updatedAt: now,
    };
    this.repository.create(resource);
    return resource;
  }

  list(filters: ResourceFilters) {
    return this.repository.list(filters);
  }

  findById(id: string): Resource | null {
    return this.repository.findById(id);
  }

  replace(id: string, input: ResourceInput): Resource | null {
    return this.repository.replace(id, input, new Date().toISOString());
  }

  update(id: string, input: Partial<ResourceInput>): Resource | null {
    const current = this.repository.findById(id);
    if (!current) return null;

    const updated = {
      ...current,
      ...input,
      updatedAt: new Date().toISOString(),
    };
    this.repository.update(id, updated);
    return updated;
  }

  delete(id: string): boolean {
    return this.repository.delete(id);
  }
}
