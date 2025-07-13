import { useQuery } from "@tanstack/react-query";
import { API_ROUTES } from "../constants/api-routes";
import { findAllServices } from "../services/services";
import type { PaginationParams } from "../types/pagination";

export function useFindAllServices(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.SERVICES, params],
    queryFn: () => findAllServices(params),
  });
}
