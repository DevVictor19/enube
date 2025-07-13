// PartnerPage.tsx
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import Divider from "@mui/material/Divider";
import DataTableContainer from "./DataTableContainer";

export default function PartnerPage() {
  return (
    <Box>
      <Typography variant="h5" gutterBottom>
        Partners
      </Typography>
      <Divider sx={{ marginBottom: 2 }} />
      <DataTableContainer />
    </Box>
  );
}
