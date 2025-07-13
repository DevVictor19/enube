import Container from "@mui/material/Container";
import Form from "./components/Form";

export default function LoginPage() {
  return (
    <Container
      sx={{
        minHeight: "100vh",
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
      }}
    >
      <Form />
    </Container>
  );
}
