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
            padding: 2,
            width: "calc(100% - 240px)",
            marginTop: "64px",
            marginLeft: "240px",
          }}
        >
          <Outlet />
        </Box>
      </Box>
    </Box>
  );
}
