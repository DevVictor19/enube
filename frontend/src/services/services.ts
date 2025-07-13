import { API_ROUTES } from "../constants/api-routes";
import { api } from "../libs/axios";
import type { PaginationParams, PaginatedResult } from "../types/pagination";
import type { Service } from "./dtos/services";

export async function findAllServices(params?: PaginationParams) {
  const { data } = await api.get<PaginatedResult<Service>>(
    API_ROUTES.SERVICES,
    {
      params,
    }
  );

  console.log(data);
  return data;
}
