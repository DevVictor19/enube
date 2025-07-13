import { useQuery } from "@tanstack/react-query";
import { API_ROUTES } from "../constants/api-routes";
import { findAllPricingCurrencies } from "../services/pricing-currencies";
import type { PaginationParams } from "../types/pagination";

export function useFindAllPricingCurrencies(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.PRICING_CURRENCIES, params],
    queryFn: () => findAllPricingCurrencies(params),
  });
}
