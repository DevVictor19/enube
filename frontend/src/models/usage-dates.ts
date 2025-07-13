import { useQuery } from "@tanstack/react-query";
import { API_ROUTES } from "../constants/api-routes";
import { findAllUsageDates } from "../services/usage-dates";
import type { PaginationParams } from "../types/pagination";
import selectOptionAdapter from "../utils/select-option-adapter";
import { formatISODate } from "../utils/format";

export function useFindAllUsageDates(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.USAGE_DATES, params],
    queryFn: () => findAllUsageDates(params),
  });
}

export function useUsageDatesSelect(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.USAGE_DATES, params],
    queryFn: () => findAllUsageDates(params),
    select: ({ data }) =>
      selectOptionAdapter(data, {
        label: (d) => formatISODate(d.usage_date),
        value: (d) => d.sk,
      }),
  });
}
