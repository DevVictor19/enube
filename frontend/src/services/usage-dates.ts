import { API_ROUTES } from "../constants/api-routes";
import { api } from "../libs/axios";
import type { PaginationParams, PaginatedResult } from "../types/pagination";
import type { UsageDate } from "./dtos/usage-dates";

export async function findAllUsageDates(params?: PaginationParams) {
  const { data } = await api.get<PaginatedResult<UsageDate>>(
    API_ROUTES.USAGE_DATES,
    {
      params,
    }
  );
  return data;
}
