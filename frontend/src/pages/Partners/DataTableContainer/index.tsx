// DataTableContainer.tsx
import DataGrid, { type DataGridColumn } from "../../../components/DataGrid";
import { usePagination } from "../../../hooks/usePagination";
import { useFindAllPartners } from "../../../models/partners";
import type { Partner } from "../../../services/dtos/partners";

export default function DataTableContainer() {
  const { handleChangeLimit, handleChangePage, pagination } = usePagination();
  const { data, isError, isLoading } = useFindAllPartners(pagination);

  if (isError) {
    return <div>Error loading data</div>;
  }

  const columns: DataGridColumn<Partner>[] = [
    { realName: "sk", label: "SK" },
    { realName: "id", label: "ID" },
    { realName: "name", label: "Name" },
    { realName: "mpn_id", label: "MPN ID" },
    { realName: "invoice_number", label: "Invoice Number" },
  ];

  return (
    <DataGrid<Partner>
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
