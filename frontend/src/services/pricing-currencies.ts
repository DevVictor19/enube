import { API_ROUTES } from "../constants/api-routes";
import { api } from "../libs/axios";
import type { PaginationParams, PaginatedResult } from "../types/pagination";
import type { PricingCurrency } from "./dtos/pricing-currencies";

export async function findAllPricingCurrencies(params?: PaginationParams) {
  const { data } = await api.get<PaginatedResult<PricingCurrency>>(
    API_ROUTES.PRICING_CURRENCIES,
    {
      params,
    }
  );
  return data;
}
