import { useQuery } from "@tanstack/react-query";
import { findAllResourceLocations } from "../services/resource-locations";
import { API_ROUTES } from "../constants/api-routes";
import type { PaginationParams } from "../types/pagination";

export function useFindAllResourceLocations(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.RESOURCE_LOCATIONS, params],
    queryFn: () => findAllResourceLocations(params),
  });
}
