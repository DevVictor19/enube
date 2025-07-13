import { Navigate, Route, Routes } from "react-router";
import { APP_ROUTES } from "../constants/app-routes";
import DashboardLayout from "../components/DashboardLayout";
import ChargesPage from "../pages/Charges";

export default function ProtectedRoutes() {
  return (
    <Routes>
      <Route path={APP_ROUTES.DASHBOARD} element={<DashboardLayout />}>
        <Route index element={<ChargesPage />} />
      </Route>
      <Route
        path="*"
        element={<Navigate to={APP_ROUTES.DASHBOARD} replace />}
      />
    </Routes>
  );
}
