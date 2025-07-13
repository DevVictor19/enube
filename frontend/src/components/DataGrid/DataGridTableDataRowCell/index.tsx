/* eslint-disable @typescript-eslint/no-explicit-any */
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import Chip from "@mui/material/Chip";
import TableCell from "@mui/material/TableCell";
import { type MouseEvent } from "react";
import type { DataGridColumn } from "..";

type DataGridTableDataRowCellProps<T extends object> = {
  data: T;
  column: DataGridColumn<T>;
};

export default function DataGridTableDataRowCell<T extends object>({
  data,
  column,
}: DataGridTableDataRowCellProps<T>) {
  const { realName, format, chip, link, align, minWidth } = column;
  let value: any = data[realName];

  if (link) {
    const handleClick = (e: MouseEvent<HTMLDivElement>) => {
      e.stopPropagation();
      window.open(value, "__blank")?.focus();
    };

    return (
      <TableCell
        sx={{ whiteSpace: "nowrap" }}
        align={align}
        style={{ minWidth }}
      >
        <Chip
          label="Link"
          color="primary"
          variant="outlined"
          onClick={handleClick}
          size="medium"
          icon={<OpenInNewIcon fontSize="small" />}
        />
      </TableCell>
    );
  }

  if (format) {
    value = format(value);
  }

  if (chip) {
    const config = chip(value);

    return (
      <TableCell
        sx={{ whiteSpace: "nowrap" }}
        align={align}
        style={{ minWidth }}
      >
        <Chip
          label={config.label}
          color={config.color}
          variant={config.variant}
        />
      </TableCell>
    );
  }

  return (
    <TableCell sx={{ whiteSpace: "nowrap" }} align={align} style={{ minWidth }}>
      {value}
    </TableCell>
  );
}
