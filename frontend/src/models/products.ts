import { useQuery } from "@tanstack/react-query";
import { API_ROUTES } from "../constants/api-routes";
import type { PaginationParams } from "../types/pagination";
import { findAllProducts } from "../services/products";

export function useFindAllProducts(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.PRODUCTS, params],
    queryFn: () => findAllProducts(params),
  });
}
