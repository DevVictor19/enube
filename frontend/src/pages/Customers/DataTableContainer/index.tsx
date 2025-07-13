import DataGrid, { type DataGridColumn } from "../../../components/DataGrid";
import { usePagination } from "../../../hooks/usePagination";
import { useFindAllCustomers } from "../../../models/customers";
import type { Customer } from "../../../services/dtos/customers";

export default function DataTableContainer() {
  const { handleChangeLimit, handleChangePage, pagination } = usePagination();
  const { data, isError, isLoading } = useFindAllCustomers(pagination);

  if (isError) {
    return <div>Error loading data</div>;
  }

  const columns: DataGridColumn<Customer>[] = [
    { realName: "customer_sk", label: "SK" },
    { realName: "customer_name", label: "Name" },
    { realName: "customer_domain_name", label: "Domain" },
    { realName: "customer_country", label: "Country" },
    { realName: "tier_2_mpn_id", label: "T2MpnID" },
  ];

  return (
    <DataGrid<Customer>
      columns={columns}
      paginatedResult={data}
      isLoading={isLoading}
      primaryKey="customer_sk"
      maxHeight="60vh"
      onChangeLimit={handleChangeLimit}
      onChangePage={handleChangePage}
    />
  );
}
