import { useQuery } from "@tanstack/react-query";
import { API_ROUTES } from "../constants/api-routes";
import type { PaginationParams } from "../types/pagination";
import { findAllProducts } from "../services/products";
import selectOptionAdapter from "../utils/select-option-adapter";

export function useFindAllProducts(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.PRODUCTS, params],
    queryFn: () => findAllProducts(params),
  });
}

export function useProductsSelect(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.PRODUCTS, params],
    queryFn: () => findAllProducts(params),
    select: ({ data }) =>
      selectOptionAdapter(data, {
        label: (d) => d.name,
        value: (d) => d.sk,
      }),
  });
}
