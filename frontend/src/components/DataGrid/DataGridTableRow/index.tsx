import TableRow from "@mui/material/TableRow";

import { type DataGridColumn } from "..";
import DataGridTableDataRowCell from "../DataGridTableDataRowCell";

export type DataGridTableDataRowProps<T extends object> = {
  primaryKey: keyof T;
  columns: DataGridColumn<T>[];
  data: T;
};

export default function DataGridTableDataRow<T extends object>({
  primaryKey,
  columns,
  data,
}: DataGridTableDataRowProps<T>) {
  return (
    <TableRow key={`row-${data[primaryKey]}`} hover tabIndex={-1}>
      {columns.map((column, index) => (
        <DataGridTableDataRowCell<T>
          key={`row-cell-${index}`}
          column={column}
          data={data}
        />
      ))}
    </TableRow>
  );
}
