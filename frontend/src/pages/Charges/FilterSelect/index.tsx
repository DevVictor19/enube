import TextField from "@mui/material/TextField";
import Autocomplete from "@mui/material/Autocomplete";
import type { AutocompleteOption } from "../../../components/AutocompleteControlled";
import type { ChargeFilter } from "../../../services/dtos/charges";
import { useState } from "react";

interface FilterSelectProps {
  label: string;
  disabled?: boolean;
  filter: ChargeFilter;
  options: AutocompleteOption[];
  onChangeFilter: (filter: ChargeFilter, value: number | undefined) => void;
}

export default function FilterSelect({
  label,
  disabled,
  options,
  filter,
  onChangeFilter,
}: FilterSelectProps) {
  const [value, setValue] = useState<AutocompleteOption | null | undefined>(
    undefined
  );

  return (
    <Autocomplete
      disablePortal
      options={options}
      fullWidth
      disabled={disabled}
      value={value}
      onChange={(event, value) => {
        const val = value as AutocompleteOption;

        if (val === null) {
          setValue(null);
          onChangeFilter(filter, undefined);
          return;
        }

        onChangeFilter(filter, Number(val.value));
      }}
      renderInput={(params) => (
        <TextField {...params} label={label} variant="standard" size="small" />
      )}
    />
  );
}
