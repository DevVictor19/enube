import { useBillingCurrenciesSelect } from "../../../models/billing-currencies";
import { useCustomersSelect } from "../../../models/customers";
import { useMonthsChargeDatesSelect } from "../../../models/months-charge-dates";
import { usePartnersSelect } from "../../../models/partners";
import { usePricingCurrenciesSelect } from "../../../models/pricing-currencies";
import { useProductsSelect } from "../../../models/products";
import { useResourceLocationsSelect } from "../../../models/resource-locations";
import { useServicesSelect } from "../../../models/services";
import { useUsageDatesSelect } from "../../../models/usage-dates";

const pagination = {
  limit: 100,
  page: 1,
};

export function useFilters() {
  const { data: customers, isLoading: isLoadingCustomers } =
    useCustomersSelect(pagination);
  const { data: products, isLoading: isLoadingProducts } =
    useProductsSelect(pagination);
  const { data: partners, isLoading: isLoadingPartners } =
    usePartnersSelect(pagination);
  const { data: monthsChargeDates, isLoading: isLoadingMonthsChargeDates } =
    useMonthsChargeDatesSelect(pagination);
  const { data: usageDates, isLoading: isLoadingUsageDates } =
    useUsageDatesSelect(pagination);
  const { data: billingCurrencies, isLoading: isLoadingBillingCurrencies } =
    useBillingCurrenciesSelect(pagination);
  const { data: pricingCurrencies, isLoading: isLoadingPricingCurrencies } =
    usePricingCurrenciesSelect(pagination);
  const { data: resourceLocations, isLoading: isLoadingResourceLocations } =
    useResourceLocationsSelect(pagination);
  const { data: services, isLoading: isLoadingServices } =
    useServicesSelect(pagination);

  return {
    customers: {
      data: customers ?? [],
      isLoading: isLoadingCustomers,
    },
    products: {
      data: products ?? [],
      isLoading: isLoadingProducts,
    },
    partners: {
      data: partners ?? [],
      isLoading: isLoadingPartners,
    },
    monthsChargeDates: {
      data: monthsChargeDates ?? [],
      isLoading: isLoadingMonthsChargeDates,
    },
    usageDates: {
      data: usageDates ?? [],
      isLoading: isLoadingUsageDates,
    },
    billingCurrencies: {
      data: billingCurrencies ?? [],
      isLoading: isLoadingBillingCurrencies,
    },
    pricingCurrencies: {
      data: pricingCurrencies ?? [],
      isLoading: isLoadingPricingCurrencies,
    },
    resourceLocations: {
      data: resourceLocations ?? [],
      isLoading: isLoadingResourceLocations,
    },
    services: {
      data: services ?? [],
      isLoading: isLoadingServices,
    },
  };
}
