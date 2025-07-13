import { API_ROUTES } from "../constants/api-routes";
import { api } from "../libs/axios";
import type { LoginDTO } from "./dtos/auth";

export async function login(dto: LoginDTO): Promise<void> {
  await api.post(API_ROUTES.LOGIN, dto);
}
