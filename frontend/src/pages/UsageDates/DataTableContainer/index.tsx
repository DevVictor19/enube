// DataTableContainer.tsx
import DataGrid, { type DataGridColumn } from "../../../components/DataGrid";
import { usePagination } from "../../../hooks/usePagination";
import { useFindAllUsageDates } from "../../../models/usage-dates";
import type { UsageDate } from "../../../services/dtos/usage-dates";
import { formatISODate } from "../../../utils/format";

export default function DataTableContainer() {
  const { handleChangeLimit, handleChangePage, pagination } = usePagination();
  const { data, isError, isLoading } = useFindAllUsageDates(pagination);

  if (isError) {
    return <div>Error loading data</div>;
  }

  const columns: DataGridColumn<UsageDate>[] = [
    { realName: "sk", label: "SK" },
    { realName: "usage_date", label: "Usage Date", format: formatISODate },
  ];

  return (
    <DataGrid<UsageDate>
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
