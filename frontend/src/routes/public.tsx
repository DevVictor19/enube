import { Navigate, Route, Routes } from "react-router";
import LoginPage from "../pages/Login";
import { APP_ROUTES } from "../constants/app-routes";

export default function PublicRoutes() {
  return (
    <Routes>
      <Route path={APP_ROUTES.HOME} element={<LoginPage />} />
      <Route path="*" element={<Navigate to={APP_ROUTES.HOME} replace />} />
    </Routes>
  );
}
