import { useQuery } from "@tanstack/react-query";
import type { PaginationParams } from "../types/pagination";
import { API_ROUTES } from "../constants/api-routes";
import { findAllBillingCurrencies } from "../services/billing-currencies";
import selectOptionAdapter from "../utils/select-option-adapter";

export function useFindAllBillingCurrencies(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.BILLING_CURRENCIES, params],
    queryFn: () => findAllBillingCurrencies(params),
  });
}

export function useBillingCurrenciesSelect(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.BILLING_CURRENCIES, params],
    queryFn: () => findAllBillingCurrencies(params),
    select: ({ data }) =>
      selectOptionAdapter(data, {
        label: (d) => d.currency,
        value: (d) => d.sk,
      }),
  });
}
