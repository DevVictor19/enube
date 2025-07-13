import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import Divider from "@mui/material/Divider";
import Resume from "./Resume";
import DataTableContainer from "./DataTableContainer";
import { useChargeParams } from "./hooks/useChargeParams";
import Filters from "./Filters";

export default function ChargesPage() {
  const {
    chargeParams,
    handleChangeFilter,
    handleChangeLimit,
    handleChangePage,
  } = useChargeParams();

  return (
    <Box>
      <Typography variant="h5" gutterBottom>
        Charges
      </Typography>
      <Divider sx={{ marginBottom: 2 }} />
      <Resume params={chargeParams} />
      <Divider sx={{ marginBottom: 2 }} />
      <Filters onChangeFilter={handleChangeFilter} />
      <DataTableContainer
        onChangeLimit={handleChangeLimit}
        onChangePage={handleChangePage}
        params={chargeParams}
      />
    </Box>
  );
}
