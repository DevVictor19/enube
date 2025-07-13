/* eslint-disable @typescript-eslint/no-explicit-any */
"use client";

import Paper from "@mui/material/Paper";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableContainer from "@mui/material/TableContainer";
import DataGridTableHead from "./DataGridTableHead";
import DataGridTableDataRow from "./DataGridTableRow";
import DataGridPagination from "./DataGridPagination";
import DataGridSkeleton from "./DataGridSkeleton";
import type { PaginatedResult } from "../../types/pagination";

export interface DataGridColumn<T extends object> {
  realName: keyof T;
  label: string;
  minWidth?: number | string;
  link?: boolean;
  align?: "right" | "center" | "left" | "inherit" | "justify";
  format?: (value: any) => string | number;
  chip?: (value: any) => {
    label: string;
    variant: "filled" | "outlined";
    color:
      | "default"
      | "primary"
      | "secondary"
      | "error"
      | "info"
      | "success"
      | "warning";
  };
}

interface DataGridProps<T extends object> {
  columns: DataGridColumn<T>[];
  primaryKey: keyof T;
  isLoading: boolean;
  paginatedResult: PaginatedResult<T> | undefined;
  maxHeight: string | number;
  onChangePage: (newPage: number) => void;
  onChangeLimit: (newLimit: number) => void;
}

export default function DataGrid<T extends object>({
  columns,
  primaryKey,
  paginatedResult,
  isLoading,
  maxHeight,
  onChangePage,
  onChangeLimit,
}: DataGridProps<T>) {
  return (
    <Paper component="div">
      <TableContainer
        sx={{
          maxHeight,
          width: "100%",
        }}
      >
        <Table stickyHeader aria-label="sticky table">
          <DataGridTableHead columns={columns} />
          {!isLoading && paginatedResult !== undefined && (
            <TableBody>
              {paginatedResult.data.map((d) => (
                <DataGridTableDataRow<T>
                  key={`row-${d[primaryKey]}`}
                  columns={columns}
                  data={d}
                  primaryKey={primaryKey}
                />
              ))}
            </TableBody>
          )}
        </Table>
      </TableContainer>
      {isLoading && <DataGridSkeleton />}
      <DataGridPagination
        limit={paginatedResult?.limit}
        onChangeLimit={onChangeLimit}
        onChangePage={onChangePage}
        page={paginatedResult?.page}
        results={paginatedResult?.results}
        total={paginatedResult?.total}
      />
    </Paper>
  );
}
