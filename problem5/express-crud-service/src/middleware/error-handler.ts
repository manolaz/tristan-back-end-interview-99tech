import type { ErrorRequestHandler } from 'express';

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export const errorHandler: ErrorRequestHandler = (error, _request, response, _next) => {
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
