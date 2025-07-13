export interface PaginatedResult<T> {
  page: number;
  limit: number;
  results: number;
  total: number;
  data: T[];
}

export interface PaginationParams {
  page?: number;
  limit?: number;
}
