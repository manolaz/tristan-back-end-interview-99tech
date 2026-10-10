import type { ResourceInput } from './resource.js';

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function parseResourceInput(value: unknown): ResourceInput | null;
export function parseResourceInput(value: unknown, partial: true): Partial<ResourceInput> | null;
export function parseResourceInput(
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
