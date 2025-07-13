import { useQuery } from "@tanstack/react-query";
import { findAllResourceLocations } from "../services/resource-locations";
import { API_ROUTES } from "../constants/api-routes";
import type { PaginationParams } from "../types/pagination";
import selectOptionAdapter from "../utils/select-option-adapter";

export function useFindAllResourceLocations(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.RESOURCE_LOCATIONS, params],
    queryFn: () => findAllResourceLocations(params),
  });
}

export function useResourceLocationsSelect(params?: PaginationParams) {
  return useQuery({
    queryKey: [API_ROUTES.RESOURCE_LOCATIONS, params],
    queryFn: () => findAllResourceLocations(params),
    select: ({ data }) =>
      selectOptionAdapter(data, {
        label: (d) => d.location,
        value: (d) => d.sk,
      }),
  });
}
