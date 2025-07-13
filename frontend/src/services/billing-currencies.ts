import { API_ROUTES } from "../constants/api-routes";
import { api } from "../libs/axios";
import type { PaginationParams, PaginatedResult } from "../types/pagination";
import type { BillingCurrency } from "./dtos/billing-currencies";

export async function findAllBillingCurrencies(params?: PaginationParams) {
  const { data } = await api.get<PaginatedResult<BillingCurrency>>(
    API_ROUTES.BILLING_CURRENCIES,
    {
      params,
    }
  );
  return data;
}
