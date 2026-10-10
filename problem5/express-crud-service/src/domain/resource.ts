export interface Resource {
  id: string;
  name: string;
  description: string;
  category: string;
  createdAt: string;
  updatedAt: string;
}

export interface ResourceInput {
  name: string;
  description: string;
  category: string;
}

export interface ResourceFilters {
  name?: string;
  category?: string;
  limit: number;
  offset: number;
}

export interface ResourcePage {
  data: Resource[];
  pagination: {
    total: number;
    limit: number;
    offset: number;
  };
}
