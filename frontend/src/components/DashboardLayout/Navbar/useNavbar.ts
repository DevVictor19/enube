import { useAuthContext } from "../../../hooks/useAuthContext";

export function useNavbar() {
  const { handleClearToken } = useAuthContext();

  const handleLogout = () => {
    handleClearToken();
  };

  return {
    handleLogout,
  };
}
