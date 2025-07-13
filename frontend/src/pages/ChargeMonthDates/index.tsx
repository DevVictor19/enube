import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import Divider from "@mui/material/Divider";
import DataTableContainer from "./DataTableContainer";

export default function MonthChargeDatesPage() {
  return (
    <Box>
      <Typography variant="h5" gutterBottom>
        Month Charge Dates
      </Typography>
      <Divider sx={{ marginBottom: 2 }} />
      <DataTableContainer />
    </Box>
  );
}
