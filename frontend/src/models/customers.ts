import { useQuery } from "@tanstack/react-query";
import type { PaginationParams } from "../types/pagination";
import { API_ROUTES } from "../constants/api-routes";
import { findAllCustomers } from "../services/customers";
import selectOptionAdapter from "../utils/select-option-adapter";

export function useFindAllCustomers(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.CUSTOMERS, params],
    queryFn: () => findAllCustomers(params),
  });
}

export function useCustomersSelect(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.CUSTOMERS, params],
    queryFn: () => findAllCustomers(params),
    select: ({ data }) =>
      selectOptionAdapter(data, {
        label: (d) => d.customer_name,
        value: (d) => d.customer_sk,
      }),
  });
}
