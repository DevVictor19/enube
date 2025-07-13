import type { PaginationParams } from "../../types/pagination";

export interface ChargeData {
  charge_sk: number;
  partner_name: string;
  customer_name: string;
  product_name: string;
  resource_location: string;
  service: string;
  effective_unit_price: number;
  unit_price: number;
  quantity: number;
  billing_pre_tax_total: number;
  billing_currency: string;
  pricing_pre_tax_total: number;
  pricing_currency: string;
  pc_to_bc_exchange_rate: number;
  pc_to_bc_exchange_rate_date: string; // ISO date string
  usage_date: string; // ISO date string
  charge_start_date: string; // ISO date string
  charge_end_date: string; // ISO date string
}

export interface ChargesResume {
  charges_total: number | null;
  billing_pre_tax_total: number | null;
  pricing_pre_tax_total: number | null;
}

export interface ChargeParams extends PaginationParams {
  "dc.customer_sk"?: number;
  "dp.partner_sk"?: number;
  "dp2.product_sk"?: number;
  "dmcd.months_charge_date_sk"?: number;
  "dud.usage_date_sk"?: number;
  "dbc.billing_currency_sk"?: number;
  "dpc.pricing_currency_sk"?: number;
  "drl.resource_location_sk"?: number;
  "ds.service_sk"?: number;
}

export type ChargeFilter = keyof Omit<ChargeParams, "limit" | "page">;
