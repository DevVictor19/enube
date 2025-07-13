import { API_ROUTES } from "../constants/api-routes";
import { api } from "../libs/axios";
import type { PaginationParams, PaginatedResult } from "../types/pagination";
import type { MonthChargeDate } from "./dtos/months-charge-dates";

export async function findAllMonthsChargeDates(params?: PaginationParams) {
  const { data } = await api.get<PaginatedResult<MonthChargeDate>>(
    API_ROUTES.MONTHS_CHARGE_DATES,
    {
      params,
    }
  );
  return data;
}
