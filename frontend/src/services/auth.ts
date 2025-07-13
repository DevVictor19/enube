import { API_ROUTES } from "../constants/api-routes";
import { api } from "../libs/axios";
import { type LoginResponseDTO, type LoginDTO } from "./dtos/auth";

export async function login(dto: LoginDTO) {
  const { data } = await api.post<LoginResponseDTO>(API_ROUTES.LOGIN, dto);
  return data;
}
