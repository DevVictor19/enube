import { useQuery } from "@tanstack/react-query";
import type { PaginationParams } from "../types/pagination";
import { API_ROUTES } from "../constants/api-routes";
import { findAllBillingCurrencies } from "../services/billing-currencies";

export function useFindAllBillingCurrencies(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.BILLING_CURRENCIES, params],
    queryFn: () => findAllBillingCurrencies(params),
  });
}
