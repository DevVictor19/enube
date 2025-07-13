import DataGrid, { type DataGridColumn } from "../../../components/DataGrid";
import { usePagination } from "../../../hooks/usePagination";
import { useFindAllBillingCurrencies } from "../../../models/billing-currencies";
import type { BillingCurrency } from "../../../services/dtos/billing-currencies";

export default function DataTableContainer() {
  const { handleChangeLimit, handleChangePage, pagination } = usePagination();
  const { data, isError, isLoading } = useFindAllBillingCurrencies(pagination);

  if (isError) {
    return <div>Error loading data</div>;
  }

  const columns: DataGridColumn<BillingCurrency>[] = [
    { realName: "sk", label: "SK" },
    { realName: "currency", label: "Currency" },
  ];

  return (
    <DataGrid<BillingCurrency>
      columns={columns}
      paginatedResult={data}
      isLoading={isLoading}
      primaryKey="sk"
      maxHeight="60vh"
      onChangeLimit={handleChangeLimit}
      onChangePage={handleChangePage}
    />
  );
}
