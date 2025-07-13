import { useQuery } from "@tanstack/react-query";
import { API_ROUTES } from "../constants/api-routes";
import { findAllPricingCurrencies } from "../services/pricing-currencies";
import type { PaginationParams } from "../types/pagination";
import selectOptionAdapter from "../utils/select-option-adapter";

export function useFindAllPricingCurrencies(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.PRICING_CURRENCIES, params],
    queryFn: () => findAllPricingCurrencies(params),
  });
}

export function usePricingCurrenciesSelect(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.PRICING_CURRENCIES, params],
    queryFn: () => findAllPricingCurrencies(params),
    select: ({ data }) =>
      selectOptionAdapter(data, {
        label: (d) => d.currency,
        value: (d) => d.sk,
      }),
  });
}
