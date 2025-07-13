import { useQuery } from "@tanstack/react-query";
import type { PaginationParams } from "../types/pagination";
import { API_ROUTES } from "../constants/api-routes";
import { findAllMonthsChargeDates } from "../services/months-charge-dates";

export function useFindAllMonthsChargeDates(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.MONTHS_CHARGE_DATES, params],
    queryFn: () => findAllMonthsChargeDates(params),
  });
}
