import { API_ROUTES } from "../constants/api-routes";
import { api } from "../libs/axios";
import type { PaginationParams, PaginatedResult } from "../types/pagination";
import type { ResourceLocation } from "./dtos/resource-locations";

export async function findAllResourceLocations(params?: PaginationParams) {
  const { data } = await api.get<PaginatedResult<ResourceLocation>>(
    API_ROUTES.RESOURCE_LOCATIONS,
    {
      params,
    }
  );
  return data;
}
