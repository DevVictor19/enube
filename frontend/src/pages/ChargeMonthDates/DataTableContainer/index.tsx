import DataGrid, { type DataGridColumn } from "../../../components/DataGrid";
import { usePagination } from "../../../hooks/usePagination";
import { useFindAllMonthsChargeDates } from "../../../models/months-charge-dates";
import type { MonthChargeDate } from "../../../services/dtos/months-charge-dates";
import { formatISODate } from "../../../utils/format";

export default function DataTableContainer() {
  const { handleChangeLimit, handleChangePage, pagination } = usePagination();
  const { data, isError, isLoading } = useFindAllMonthsChargeDates(pagination);

  if (isError) {
    return <div>Error loading data</div>;
  }

  const columns: DataGridColumn<MonthChargeDate>[] = [
    { realName: "sk", label: "SK" },
    {
      realName: "charge_start_date",
      label: "Start Date",
      format: formatISODate,
    },
    { realName: "charge_end_date", label: "End Date", format: formatISODate },
  ];

  return (
    <DataGrid<MonthChargeDate>
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
