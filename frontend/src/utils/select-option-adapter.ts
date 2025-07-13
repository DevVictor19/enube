import type { AutocompleteOption } from "../components/AutocompleteControlled";

interface AdapterFunctions<T extends object> {
  label: (d: T) => string;
  value: (d: T) => string | number;
}

export default function selectOptionAdapter<T extends object>(
  data: T[],
  { label, value }: AdapterFunctions<T>
): AutocompleteOption[] {
  return data.map((d) => ({ label: label(d), value: value(d) }));
}
