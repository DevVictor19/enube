import { useQuery } from "@tanstack/react-query";
import type { ChargeParams } from "../services/dtos/charges";
import { API_ROUTES } from "../constants/api-routes";
import { findAllCharges, getChargesResume } from "../services/charges";

export function useFindAllCharges(params?: ChargeParams) {
  return useQuery({
    queryKey: [API_ROUTES.CHARGES, params],
    queryFn: () => findAllCharges(params),
  });
}

export function useGetChargesResume(params?: ChargeParams) {
  return useQuery({
    queryKey: [API_ROUTES.CHARGES_RESUME, params],
    queryFn: () => getChargesResume(params),
  });
}
