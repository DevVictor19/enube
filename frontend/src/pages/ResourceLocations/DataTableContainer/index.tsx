import DataGrid, { type DataGridColumn } from "../../../components/DataGrid";
import { usePagination } from "../../../hooks/usePagination";
import { useFindAllResourceLocations } from "../../../models/resource-locations";
import type { ResourceLocation } from "../../../services/dtos/resource-locations";

export default function DataTableContainer() {
  const { handleChangeLimit, handleChangePage, pagination } = usePagination();
  const { data, isError, isLoading } = useFindAllResourceLocations(pagination);

  if (isError) {
    return <div>Error loading data</div>;
  }

  const columns: DataGridColumn<ResourceLocation>[] = [
    { realName: "sk", label: "SK" },
    { realName: "location", label: "Location" },
  ];

  return (
    <DataGrid<ResourceLocation>
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
