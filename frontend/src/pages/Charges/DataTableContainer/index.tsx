import DataGrid, { type DataGridColumn } from "../../../components/DataGrid";
import { useFindAllCharges } from "../../../models/charges";
import type { ChargeData } from "../../../services/dtos/charges";
import { formatISODate } from "../../../utils/format";

export default function DataTableContainer() {
  const { data, isLoading, isError } = useFindAllCharges();

  if (isError) {
    return <div>Error loading data</div>;
  }

  const columns: DataGridColumn<ChargeData>[] = [
    { realName: "charge_sk", label: "SK" },
    { realName: "partner_name", label: "Partner" },
    { realName: "customer_name", label: "Customer" },
    { realName: "product_name", label: "Product" },
    { realName: "resource_location", label: "Resource Location" },
    { realName: "service", label: "Service" },
    {
      realName: "effective_unit_price",
      label: "Effective Unit Price",
    },
    { realName: "unit_price", label: "Unit Price" },
    { realName: "quantity", label: "Quantity" },
    {
      realName: "billing_pre_tax_total",
      label: "Billing Pre-Tax Total",
    },
    {
      realName: "billing_currency",
      label: "Billing Currency",
    },
    {
      realName: "pricing_pre_tax_total",
      label: "Pricing Pre-Tax Total",
    },
    { realName: "pricing_currency", label: "Pricing Currency" },
    { realName: "pc_to_bc_exchange_rate", label: "PC to BC Exchange Rate" },
    {
      realName: "pc_to_bc_exchange_rate_date",
      label: "Exchange Rate Date",
      format: formatISODate,
    },
    { realName: "usage_date", label: "Usage Date", format: formatISODate },
    {
      realName: "charge_start_date",
      label: "Charge Start Date",
      format: formatISODate,
    },
    {
      realName: "charge_end_date",
      label: "Charge End Date",
      format: formatISODate,
    },
  ];

  return (
    <DataGrid<ChargeData>
      columns={columns}
      paginatedResult={data}
      isLoading={isLoading}
      primaryKey="charge_sk"
      maxHeight="60vh"
      onChangeLimit={(data) => console.log("Change limit:", data)}
      onChangePage={(data) => console.log("Change page:", data)}
    />
  );
}
