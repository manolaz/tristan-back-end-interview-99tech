import type { Request, Response } from 'express';
import { parseResourceInput } from '../domain/resource-validation.js';
import { ResourceService } from '../services/resource-service.js';

const maxPageSize = 100;
const invalidResourceMessage =
  'Invalid resource data. Provide a non-empty name (max 120 characters), an optional description (max 2000 characters), and an optional category (max 100 characters).';
const service = new ResourceService();

function sendBadRequest(response: Response): void {
  response.status(400).json({ error: invalidResourceMessage });
}

function getResourceId(request: Request): string | null {
  const id = request.params.id;
  return typeof id === 'string' ? id : null;
}

export const resourceController = {
  create(request: Request, response: Response): void {
    const input = parseResourceInput(request.body);
    if (!input) {
      sendBadRequest(response);
      return;
    }
    response.status(201).json({ data: service.create(input) });
  },

  list(request: Request, response: Response): void {
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

    response.json({
      ...service.list({
        ...(name !== undefined && name.trim() ? { name: name.trim() } : {}),
        ...(category !== undefined && category.trim() ? { category: category.trim() } : {}),
        limit: pageSize,
        offset: pageOffset,
      }),
    });
  },

  get(request: Request, response: Response): void {
    const id = getResourceId(request);
    const resource = id === null ? null : service.findById(id);
    if (!resource) {
      response.status(404).json({ error: 'Resource not found.' });
      return;
    }
    response.json({ data: resource });
  },

  replace(request: Request, response: Response): void {
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
    const resource = service.replace(id, input);
    if (!resource) {
      response.status(404).json({ error: 'Resource not found.' });
      return;
    }
    response.json({ data: resource });
  },

  update(request: Request, response: Response): void {
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
    const resource = service.update(id, input);
    if (!resource) {
      response.status(404).json({ error: 'Resource not found.' });
      return;
    }
    response.json({ data: resource });
  },

  delete(request: Request, response: Response): void {
    const id = getResourceId(request);
    if (id === null || !service.delete(id)) {
      response.status(404).json({ error: 'Resource not found.' });
      return;
    }
    response.status(204).end();
  },
};
