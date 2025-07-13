import Box from "@mui/material/Box";

import { Outlet } from "react-router";
import Navbar from "./Navbar";
import Sidebar from "./Sidebar";

export default function DashboardLayout() {
  return (
    <Box sx={{ display: "flex", flexDirection: "column", height: "100vh" }}>
      <Navbar />
      <Box sx={{ display: "flex", flexDirection: "row", width: "100%" }}>
        <Sidebar />
        <Box
          sx={{
            flexGrow: 1,
            padding: 2,
            marginTop: "64px", // Adjust for the height of the Navbar
          }}
        >
          <Outlet />
        </Box>
      </Box>
    </Box>
  );
}
