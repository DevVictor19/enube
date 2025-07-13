import Skeleton from "@mui/material/Skeleton";
import Stack from "@mui/material/Stack";

export default function DataGridSkeleton() {
  return (
    <Stack sx={{ width: "100%", padding: 1 }} spacing={2}>
      <Skeleton variant="rectangular" sx={{ width: "100%" }} animation="wave" />
      <Skeleton variant="rectangular" sx={{ width: "100%" }} animation="wave" />
      <Skeleton variant="rectangular" sx={{ width: "100%" }} animation="wave" />
      <Skeleton variant="rectangular" sx={{ width: "100%" }} animation="wave" />
      <Skeleton variant="rectangular" sx={{ width: "100%" }} animation="wave" />
      <Skeleton variant="rectangular" sx={{ width: "100%" }} animation="wave" />
      <Skeleton variant="rectangular" sx={{ width: "100%" }} animation="wave" />
      <Skeleton variant="rectangular" sx={{ width: "100%" }} animation="wave" />
    </Stack>
  );
}
