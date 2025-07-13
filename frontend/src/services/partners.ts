import { API_ROUTES } from "../constants/api-routes";
import { api } from "../libs/axios";
import type { PaginationParams, PaginatedResult } from "../types/pagination";
import type { Partner } from "./dtos/partners";

export async function findAllPartners(params?: PaginationParams) {
  const { data } = await api.get<PaginatedResult<Partner>>(
    API_ROUTES.PARTNERS,
    {
      params,
    }
  );
  return data;
}
