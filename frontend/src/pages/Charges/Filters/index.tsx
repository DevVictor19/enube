import Grid from "@mui/material/Grid";
import type { ChargeFilter } from "../../../services/dtos/charges";
import FilterSelect from "../FilterSelect";
import { useFilters } from "./useFilters";

interface FiltersProps {
  onChangeFilter: (filter: ChargeFilter, value: number | undefined) => void;
}

export default function Filters({ onChangeFilter }: FiltersProps) {
  const {
    billingCurrencies,
    customers,
    monthsChargeDates,
    partners,
    pricingCurrencies,
    products,
    resourceLocations,
    services,
    usageDates,
  } = useFilters();

  return (
    <Grid container spacing={4} mb={5}>
      <Grid size={4}>
        <FilterSelect
          options={partners.data}
          label="Partner"
          disabled={partners.isLoading}
          filter="dp.partner_sk"
          onChangeFilter={onChangeFilter}
        />
      </Grid>
      <Grid size={4}>
        <FilterSelect
          options={customers.data}
          label="Customer"
          disabled={customers.isLoading}
          filter="dc.customer_sk"
          onChangeFilter={onChangeFilter}
        />
      </Grid>
      <Grid size={4}>
        <FilterSelect
          options={products.data}
          label="Product"
          disabled={products.isLoading}
          filter="dp2.product_sk"
          onChangeFilter={onChangeFilter}
        />
      </Grid>
      <Grid size={4}>
        <FilterSelect
          options={resourceLocations.data}
          label="Resource Location"
          disabled={resourceLocations.isLoading}
          filter="drl.resource_location_sk"
          onChangeFilter={onChangeFilter}
        />
      </Grid>
      <Grid size={4}>
        <FilterSelect
          options={services.data}
          label="Services"
          disabled={services.isLoading}
          filter="ds.service_sk"
          onChangeFilter={onChangeFilter}
        />
      </Grid>
      <Grid size={4}>
        <FilterSelect
          options={billingCurrencies.data}
          label="Billing Currencies"
          disabled={billingCurrencies.isLoading}
          filter="dbc.billing_currency_sk"
          onChangeFilter={onChangeFilter}
        />
      </Grid>
      <Grid size={4}>
        <FilterSelect
          options={pricingCurrencies.data}
          label="Pricing Currencies"
          disabled={pricingCurrencies.isLoading}
          filter="dpc.pricing_currency_sk"
          onChangeFilter={onChangeFilter}
        />
      </Grid>
      <Grid size={4}>
        <FilterSelect
          options={usageDates.data}
          label="Usage Dates"
          disabled={usageDates.isLoading}
          filter="dud.usage_date_sk"
          onChangeFilter={onChangeFilter}
        />
      </Grid>
      <Grid size={4}>
        <FilterSelect
          options={monthsChargeDates.data}
          label="Month Charge Dates"
          disabled={monthsChargeDates.isLoading}
          filter="dmcd.months_charge_date_sk"
          onChangeFilter={onChangeFilter}
        />
      </Grid>
    </Grid>
  );
}
