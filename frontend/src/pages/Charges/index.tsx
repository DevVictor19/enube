import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import Divider from "@mui/material/Divider";
import Resume from "./Resume";
import DataTableContainer from "./DataTableContainer";

export default function ChargesPage() {
  return (
    <Box>
      <Typography variant="h5" gutterBottom>
        Charges
      </Typography>
      <Divider sx={{ marginBottom: 2 }} />
      <Resume />
      <DataTableContainer />
    </Box>
  );
}
