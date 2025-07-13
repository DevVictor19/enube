import { API_ROUTES } from "../constants/api-routes";
import { api } from "../libs/axios";
import type { PaginatedResult, PaginationParams } from "../types/pagination";
import type { Customer } from "./dtos/customers";

export async function findAllCustomers(params?: PaginationParams) {
  const { data } = await api.get<PaginatedResult<Customer>>(
    API_ROUTES.CUSTOMERS,
    {
      params,
    }
  );
  return data;
}
