import { BrowserRouter } from "react-router";
import { useAuthContext } from "../hooks/useAuthContext";
import ProtectedRoutes from "./protected";
import PublicRoutes from "./public";

export default function Router() {
  const { token } = useAuthContext();
  const isAuthenticated = Boolean(token);

  return (
    <BrowserRouter>
      {isAuthenticated ? <ProtectedRoutes /> : <PublicRoutes />}
    </BrowserRouter>
  );
}
