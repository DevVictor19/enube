import { useQuery } from "@tanstack/react-query";
import { API_ROUTES } from "../constants/api-routes";
import { findAllPartners } from "../services/partners";
import type { PaginationParams } from "../types/pagination";

export function useFindAllPartners(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.PARTNERS, params],
    queryFn: () => findAllPartners(params),
  });
}
