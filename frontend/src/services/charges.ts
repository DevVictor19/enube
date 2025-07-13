import { API_ROUTES } from "../constants/api-routes";
import { api } from "../libs/axios";
import type { PaginatedResult } from "../types/pagination";
import type { ChargeData, ChargeParams, ChargesResume } from "./dtos/charges";

export async function findAllCharges(params?: ChargeParams) {
  const { data } = await api.get<PaginatedResult<ChargeData>>(
    API_ROUTES.CHARGES,
    {
      params,
    }
  );
  return data;
}

export async function getChargesResume(params?: ChargeParams) {
  const { data } = await api.get<ChargesResume>(API_ROUTES.CHARGES_RESUME, {
    params,
  });
  return data;
}
