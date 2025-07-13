import { API_ROUTES } from "../constants/api-routes";
import { api } from "../libs/axios";
import type { PaginationParams, PaginatedResult } from "../types/pagination";
import type { Product } from "./dtos/products";

export async function findAllProducts(params?: PaginationParams) {
  const { data } = await api.get<PaginatedResult<Product>>(
    API_ROUTES.PRODUCTS,
    {
      params,
    }
  );
  return data;
}
