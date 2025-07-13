import { useState } from "react";
import type { PaginationParams } from "../types/pagination";

export function usePagination() {
  const [pagination, setPagination] = useState<PaginationParams>({
    limit: 10,
    page: 1,
  });

  const handleChangeLimit = (newLimit: number) => {
    setPagination({
      limit: newLimit,
      page: 1, // Reset to first page when limit changes
    });
  };

  const handleChangePage = (newPage: number) => {
    setPagination((prev) => ({
      ...prev,
      page: newPage,
    }));
  };

  return {
    pagination,
    handleChangeLimit,
    handleChangePage,
  };
}
