import { useLocation, useNavigate } from "react-router";

export function useSidebar() {
  const location = useLocation();
  const navigate = useNavigate();

  const navigateTo = (path: string) => {
    navigate(path, { replace: true });
  };

  return {
    pathname: location.pathname,
    navigateTo,
  };
}
