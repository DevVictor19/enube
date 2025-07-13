import { useQuery } from "@tanstack/react-query";
import { API_ROUTES } from "../constants/api-routes";
import { findAllPartners } from "../services/partners";
import type { PaginationParams } from "../types/pagination";
import selectOptionAdapter from "../utils/select-option-adapter";

export function useFindAllPartners(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.PARTNERS, params],
    queryFn: () => findAllPartners(params),
  });
}

export function usePartnersSelect(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.PARTNERS, params],
    queryFn: () => findAllPartners(params),
    select: ({ data }) =>
      selectOptionAdapter(data, {
        label: (d) => `${d.name} invoice:${d.invoice_number}`,
        value: (d) => d.sk,
      }),
  });
}
