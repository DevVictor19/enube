import { Navigate, Route, Routes } from "react-router";
import { APP_ROUTES } from "../constants/app-routes";

export default function ProtectedRoutes() {
  return (
    <Routes>
      <Route index path={APP_ROUTES.DASHBOARD} element={<div>dashboard</div>} />
      <Route
        path="*"
        element={<Navigate to={APP_ROUTES.DASHBOARD} replace />}
      />
    </Routes>
  );
}
