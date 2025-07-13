import { useEffect, useState } from "react";
import {
  getLocalStorageItem,
  removeLocalStorageItem,
  setLocalStorageItem,
} from "../libs/local-storage";
import { api } from "../libs/axios";
import { AuthContext } from "../hooks/useAuthContext";

const TOKEN_KEY = "token";

export function AuthContextProvider({
  children,
}: {
  children: React.ReactNode;
}) {
  const [token, setToken] = useState<string | null>(null);

  useEffect(() => {
    const token = getLocalStorageItem<string>(TOKEN_KEY);

    if (token) {
      api.defaults.headers.common["Authorization"] = `Bearer ${token}`;

      api.interceptors.response.use(
        (response) => response,
        (error) => {
          if (error.response?.status === 401) {
            setToken(null);
            removeLocalStorageItem(TOKEN_KEY);
          }
          return Promise.reject(error);
        }
      );

      setToken(token);
    }
  }, []);

  const handleSetToken = (token: string) => {
    setLocalStorageItem(TOKEN_KEY, token);

    api.defaults.headers.common["Authorization"] = `Bearer ${token}`;

    api.interceptors.response.use(
      (response) => response,
      (error) => {
        if (error.response?.status === 401) {
          setToken(null);
          removeLocalStorageItem(TOKEN_KEY);
        }
        return Promise.reject(error);
      }
    );

    setToken(token);
  };

  const handleClearToken = () => {
    removeLocalStorageItem(TOKEN_KEY);
    setToken(null);
  };

  return (
    <AuthContext.Provider value={{ token, handleSetToken, handleClearToken }}>
      {children}
    </AuthContext.Provider>
  );
}
