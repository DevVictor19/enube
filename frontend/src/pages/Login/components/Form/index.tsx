import Grid from "@mui/material/Grid";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import TextFieldControlled from "../../../../components/TextFieldControlled";
import Button from "@mui/material/Button";
import { useLoginForm } from "./useLoginForm";

export default function Form() {
  const { control, errors, isError, isPending, onSubmit } = useLoginForm();

  return (
    <Paper sx={{ maxWidth: 400, width: "100%", p: 3 }} variant="outlined">
      <Typography align="center" variant="h6" component="h1" mb={2}>
        Login
      </Typography>
      <Grid component="form" container spacing={2}>
        <Grid size={12}>
          <TextFieldControlled
            control={control}
            name="email"
            label="Email"
            variant="standard"
            placeholder="Enter your email"
            error={Boolean(errors.email || isError)}
            helperText={errors.email?.message || (isError && "Login failed")}
            disabled={isPending}
            autoFocus
            fullWidth
          />
        </Grid>
        <Grid size={12}>
          <TextFieldControlled
            control={control}
            name="password"
            label="Password"
            variant="standard"
            type="password"
            placeholder="Enter your password"
            error={Boolean(errors.password) || isError}
            helperText={errors.password?.message}
            disabled={isPending}
            fullWidth
          />
        </Grid>
        <Grid size={12}>
          <Button
            variant="contained"
            fullWidth
            onClick={onSubmit}
            disabled={isPending}
            sx={{ mt: 2 }}
          >
            {isPending ? "Logging in..." : "Login"}
          </Button>
        </Grid>
      </Grid>
    </Paper>
  );
}
