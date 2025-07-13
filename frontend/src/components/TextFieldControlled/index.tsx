/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  type Control,
  Controller,
  type FieldValues,
  type Path,
} from "react-hook-form";
import TextField, { type TextFieldProps } from "@mui/material/TextField";

type TextFieldControlledProps<T extends FieldValues> = TextFieldProps & {
  name: Path<T>;
  control: Control<T, any>;
  mask?: (value: string) => string;
};

export default function TextFieldControlled<T extends FieldValues>({
  name,
  control,
  mask,
  ...rest
}: TextFieldControlledProps<T>) {
  return (
    <Controller
      name={name}
      control={control}
      render={({ field }) => (
        <TextField
          value={field.value}
          onChange={(event) => {
            if (mask) {
              event.target.value = mask(event.target.value);
            }
            field.onChange(event);
          }}
          onBlur={field.onBlur}
          disabled={field.disabled}
          ref={field.ref}
          {...rest}
        />
      )}
    />
  );
}
