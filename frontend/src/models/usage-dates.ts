import { useQuery } from "@tanstack/react-query";
import { API_ROUTES } from "../constants/api-routes";
import { findAllUsageDates } from "../services/usage-dates";
import type { PaginationParams } from "../types/pagination";

export function useFindAllUsageDates(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.USAGE_DATES, params],
    queryFn: () => findAllUsageDates(params),
  });
}
