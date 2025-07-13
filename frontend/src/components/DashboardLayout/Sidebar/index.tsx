import Paper from "@mui/material/Paper";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import ListSubheader from "@mui/material/ListSubheader";
import RequestQuoteIcon from "@mui/icons-material/RequestQuote";
import PersonIcon from "@mui/icons-material/Person";
import HandshakeIcon from "@mui/icons-material/Handshake";
import EventAvailableIcon from "@mui/icons-material/EventAvailable";
import QueryBuilderIcon from "@mui/icons-material/QueryBuilder";
import CurrencyExchangeIcon from "@mui/icons-material/CurrencyExchange";
import PublicIcon from "@mui/icons-material/Public";
import MiscellaneousServicesIcon from "@mui/icons-material/MiscellaneousServices";
import Divider from "@mui/material/Divider";
import { useSidebar } from "./useSidebar";
import { APP_ROUTES } from "../../../constants/app-routes";

interface SidebarItem {
  label: string;
  path: string;
  icon: React.ReactNode;
  onClick: () => void;
}

export default function Sidebar() {
  const { navigateTo, pathname } = useSidebar();

  const sidebarItems: SidebarItem[] = [
    {
      label: "Charges",
      icon: <RequestQuoteIcon />,
      onClick: () => navigateTo(APP_ROUTES.DASHBOARD),
      path: APP_ROUTES.DASHBOARD,
    },
    {
      label: "Customers",
      icon: <PersonIcon />,
      onClick: () => navigateTo(APP_ROUTES.CUSTOMERS),
      path: APP_ROUTES.CUSTOMERS,
    },
    {
      label: "Partners",
      icon: <HandshakeIcon />,
      onClick: () => navigateTo(APP_ROUTES.PARTNERS),
      path: APP_ROUTES.PARTNERS,
    },
    {
      label: "Charge Months",
      icon: <EventAvailableIcon />,
      onClick: () => navigateTo(APP_ROUTES.CHARGE_MONTHS),
      path: APP_ROUTES.CHARGE_MONTHS,
    },
    {
      label: "Usage Dates",
      icon: <QueryBuilderIcon />,
      onClick: () => navigateTo(APP_ROUTES.USAGE_DATES),
      path: APP_ROUTES.USAGE_DATES,
    },
    {
      label: "Billing Currencies",
      icon: <CurrencyExchangeIcon />,
      onClick: () => navigateTo(APP_ROUTES.BILLING_CURRENCIES),
      path: APP_ROUTES.BILLING_CURRENCIES,
    },
    {
      label: "Pricing Currencies",
      icon: <CurrencyExchangeIcon />,
      onClick: () => navigateTo(APP_ROUTES.PRICING_CURRENCIES),
      path: APP_ROUTES.PRICING_CURRENCIES,
    },
    {
      label: "Resource Locations",
      icon: <PublicIcon />,
      onClick: () => navigateTo(APP_ROUTES.RESOURCE_LOCATIONS),
      path: APP_ROUTES.RESOURCE_LOCATIONS,
    },
    {
      label: "Services",
      icon: <MiscellaneousServicesIcon />,
      onClick: () => navigateTo(APP_ROUTES.SERVICES),
      path: APP_ROUTES.SERVICES,
    },
  ];

  return (
    <Paper
      sx={{
        flexGrow: 1,
        maxWidth: 260,
        height: "calc(100vh - 64px)",
        marginTop: "64px",
      }}
    >
      <List
        subheader={
          <ListSubheader component="div" id="nested-list-subheader">
            Administration
          </ListSubheader>
        }
      >
        <Divider />
        {sidebarItems.map((item) => (
          <SideBarItem key={item.path} item={item} pathname={pathname} />
        ))}
      </List>
    </Paper>
  );
}

interface SidebarItemProps {
  pathname: string;
  item: SidebarItem;
}

function SideBarItem({ item, pathname }: SidebarItemProps) {
  return (
    <>
      <ListItem disablePadding>
        <ListItemButton
          selected={pathname === item.path}
          onClick={item.onClick}
        >
          <ListItemIcon>{item.icon}</ListItemIcon>
          <ListItemText primary={item.label} />
        </ListItemButton>
      </ListItem>
      <Divider />
    </>
  );
}
