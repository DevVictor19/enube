import { useQuery } from "@tanstack/react-query";
import type { PaginationParams } from "../types/pagination";
import { API_ROUTES } from "../constants/api-routes";
import { findAllMonthsChargeDates } from "../services/months-charge-dates";
import selectOptionAdapter from "../utils/select-option-adapter";
import { formatISODate } from "../utils/format";

export function useFindAllMonthsChargeDates(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.MONTHS_CHARGE_DATES, params],
    queryFn: () => findAllMonthsChargeDates(params),
  });
}

export function useMonthsChargeDatesSelect(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.MONTHS_CHARGE_DATES, params],
    queryFn: () => findAllMonthsChargeDates(params),
    select: ({ data }) =>
      selectOptionAdapter(data, {
        label: (d) =>
          `${formatISODate(d.charge_start_date)} to ${formatISODate(
            d.charge_end_date
          )}`,
        value: (d) => d.sk,
      }),
  });
}
