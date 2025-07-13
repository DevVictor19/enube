import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import { formatCurrency } from "../../../utils/format";
import { useGetChargesResume } from "../../../models/charges";

export default function Resume() {
  const { data } = useGetChargesResume();

  return (
    <Box mb={2}>
      <Typography variant="body1" gutterBottom>
        Total of charges:{" "}
        <Box component="span" sx={{ fontWeight: "bold" }}>
          {data?.charges_total ?? 0}
        </Box>
      </Typography>
      <Typography variant="body1" gutterBottom>
        Billing pre tax total:{" "}
        <Box component="span" sx={{ fontWeight: "bold" }}>
          {formatCurrency(data?.billing_pre_tax_total ?? 0)}{" "}
        </Box>
      </Typography>
      <Typography variant="body1" gutterBottom>
        Pricing pre tax total:{" "}
        <Box component="span" sx={{ fontWeight: "bold" }}>
          {formatCurrency(data?.pricing_pre_tax_total ?? 0)}
        </Box>
      </Typography>
    </Box>
  );
}
