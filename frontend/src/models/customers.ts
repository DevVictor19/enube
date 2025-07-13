import { useQuery } from "@tanstack/react-query";
import type { PaginationParams } from "../types/pagination";
import { API_ROUTES } from "../constants/api-routes";
import { findAllCustomers } from "../services/customers";

export function useFindAllCustomers(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.CUSTOMERS, params],
    queryFn: () => findAllCustomers(params),
  });
}
