import { useState } from "react";
import type {
  ChargeFilter,
  ChargeParams,
} from "../../../services/dtos/charges";

const initialState = {
  limit: 10,
  page: 1,
};

export function useChargeParams() {
  const [chargeParams, setChargeParams] = useState<ChargeParams>(initialState);

  const handleChangeLimit = (newLimit: number) => {
    setChargeParams({
      ...chargeParams,
      limit: newLimit,
      page: 1, // Reset to first page when limit changes
    });
  };

  const handleChangePage = (newPage: number) => {
    setChargeParams((prev) => ({
      ...prev,
      page: newPage,
    }));
  };

  const handleChangeFilter = (
    filter: ChargeFilter,
    value: number | undefined
  ) => {
    setChargeParams((prev) => ({
      ...prev,
      [filter]: value,
    }));
  };

  return {
    chargeParams,
    handleChangeLimit,
    handleChangePage,
    handleChangeFilter,
  };
}
