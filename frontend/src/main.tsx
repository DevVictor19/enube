import "@fontsource/roboto/300.css";
import "@fontsource/roboto/400.css";
import "@fontsource/roboto/500.css";
import "@fontsource/roboto/700.css";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import QueryProvider from "./contexts/QueryContext.tsx";
import CssBaseline from "@mui/material/CssBaseline";
import { AuthContextProvider } from "./contexts/AuthContext.tsx";
import Router from "./routes/router.tsx";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryProvider>
      <AuthContextProvider>
        <CssBaseline />
        <Router />
      </AuthContextProvider>
    </QueryProvider>
  </StrictMode>
);
