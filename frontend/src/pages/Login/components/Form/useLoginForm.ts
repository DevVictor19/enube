import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { loginSchema } from "./schema";
import { useLogin } from "../../../../models/auth";

export function useLoginForm() {
  const { mutateAsync, isError, isPending } = useLogin();

  const {
    handleSubmit,
    control,
    formState: { errors },
  } = useForm({
    resolver: zodResolver(loginSchema),
  });

  const onSubmit = handleSubmit(async (data) => {
    try {
      await mutateAsync(data);
    } catch (error) {
      console.error("Login failed:", error);
    }
  });

  return {
    control,
    errors,
    onSubmit,
    isError,
    isPending,
  };
}
