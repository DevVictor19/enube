import z from "zod";

export const loginSchema = z.object({
  email: z.email("Invalid email address").trim(),
  password: z.string("Password is required").trim(),
});

export type LoginFormValues = z.infer<typeof loginSchema>;
