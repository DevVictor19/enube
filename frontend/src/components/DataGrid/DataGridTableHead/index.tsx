import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import TableCell from "@mui/material/TableCell";

import type { DataGridColumn } from "..";

interface DataGridTableHeadProps<T extends object> {
  columns: DataGridColumn<T>[];
}

export default function DataGridTableHead<T extends object>({
  columns,
}: DataGridTableHeadProps<T>) {
  return (
    <TableHead>
      <TableRow>
        {columns.map((column) => (
          <TableCell
            sx={{ whiteSpace: "nowrap" }}
            key={String(column.realName)}
            align={column.align}
            style={{ minWidth: column.minWidth }}
          >
            {column.label}
          </TableCell>
        ))}
      </TableRow>
    </TableHead>
  );
}
