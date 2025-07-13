import { createContext, useContext } from "react";

export interface IAuthContext {
  token: string | null;
  handleSetToken: (token: string) => void;
  handleClearToken: () => void;
}

export const AuthContext = createContext({} as IAuthContext);

export function useAuthContext() {
  return useContext(AuthContext);
}
