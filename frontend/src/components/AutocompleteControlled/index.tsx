/* eslint-disable @typescript-eslint/no-explicit-any */
import Autocomplete from "@mui/material/Autocomplete";
import TextField, { type TextFieldProps } from "@mui/material/TextField";
import {
  type Control,
  Controller,
  type FieldValues,
  type Path,
} from "react-hook-form";

export interface AutocompleteOption {
  label: string;
  value: number | string | boolean;
}

type AutocompleteControlledProps<T extends FieldValues> = TextFieldProps & {
  name: Path<T>;
  control: Control<T, any>;
  options: AutocompleteOption[];
};

export default function AutocompleteControlled<T extends FieldValues>({
  name,
  control,
  options,
  ...rest
}: AutocompleteControlledProps<T>) {
  return (
    <Controller
      name={name}
      control={control}
      render={({ field }) => {
        return (
          <Autocomplete
            options={options}
            value={field.value}
            onChange={(event, values) => field.onChange(values)}
            getOptionLabel={(option) => option.label || ""}
            isOptionEqualToValue={(option, value) =>
              option.value === value.value
            }
            onBlur={field.onBlur}
            disabled={rest.disabled}
            ref={field.ref}
            renderInput={(params) => {
              return <TextField {...rest} {...params} />;
            }}
          />
        );
      }}
    />
  );
}
