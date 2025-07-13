import { Navigate, Route, Routes } from "react-router";
import { APP_ROUTES } from "../constants/app-routes";
import DashboardLayout from "../components/DashboardLayout";
import ChargesPage from "../pages/Charges";
import CustomersPage from "../pages/Customers";
import PartnerPage from "../pages/Partners";
import MonthChargeDatesPage from "../pages/ChargeMonthDates";
import UsageDatesPage from "../pages/UsageDates";
import BillingCurrenciesPage from "../pages/BillingCurrencies";
import PricingCurrenciesPage from "../pages/PricingCurrencies";
import ResourceLocationsPage from "../pages/ResourceLocations";
import ServicesPage from "../pages/Services";

export default function ProtectedRoutes() {
  return (
    <Routes>
      <Route path={APP_ROUTES.DASHBOARD} element={<DashboardLayout />}>
        <Route index element={<ChargesPage />} />
        <Route path={APP_ROUTES.CUSTOMERS} element={<CustomersPage />} />
        <Route path={APP_ROUTES.PARTNERS} element={<PartnerPage />} />
        <Route
          path={APP_ROUTES.CHARGE_MONTHS}
          element={<MonthChargeDatesPage />}
        />
        <Route path={APP_ROUTES.USAGE_DATES} element={<UsageDatesPage />} />
        <Route
          path={APP_ROUTES.BILLING_CURRENCIES}
          element={<BillingCurrenciesPage />}
        />
        <Route
          path={APP_ROUTES.PRICING_CURRENCIES}
          element={<PricingCurrenciesPage />}
        />
        <Route
          path={APP_ROUTES.RESOURCE_LOCATIONS}
          element={<ResourceLocationsPage />}
        />
        <Route path={APP_ROUTES.SERVICES} element={<ServicesPage />} />
      </Route>
      <Route
        path="*"
        element={<Navigate to={APP_ROUTES.DASHBOARD} replace />}
      />
    </Routes>
  );
}
