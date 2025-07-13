import DataGrid, { type DataGridColumn } from "../../../components/DataGrid";
import { usePagination } from "../../../hooks/usePagination";
import { useFindAllPricingCurrencies } from "../../../models/pricing-currencies";
import type { PricingCurrency } from "../../../services/dtos/pricing-currencies";

export default function DataTableContainer() {
  const { handleChangeLimit, handleChangePage, pagination } = usePagination();
  const { data, isError, isLoading } = useFindAllPricingCurrencies(pagination);

  if (isError) {
    return <div>Error loading data</div>;
  }

  const columns: DataGridColumn<PricingCurrency>[] = [
    { realName: "sk", label: "SK" },
    { realName: "currency", label: "Currency" },
  ];

  return (
    <DataGrid<PricingCurrency>
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
