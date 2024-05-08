CREATE OR REPLACE VIEW "carshare_v2_rpt_corporate_details" AS 
SELECT
  c.name company_name
, c.company_reg_id
, c.erp_trade_receivable_number
, CAST(c.credit_value AS int) credit_value
, c.credit_terms
, c.industry
, c.number_of_employees
, replace(replace(c.address, chr(10), ' '), chr(13), ' ') address
, c.city
, c.phone phone_number
, c.key_account_person
, c.contact_person
, c.updated_at
, count(DISTINCT ca.user_id) number_of_users
, r.total_reservations
, r.total_revenue
, CAST(max(r1.created_at) AS date) last_rented_date
, round(CAST(os.amount AS bigint)) outstanding_amount
, recharge.number_of_recharge_invoices
, recharge.total_recharge_amount
, c.created_at
FROM
  (((((carshare_v2_staging_companies c
LEFT JOIN carshare_v2_staging_corporate_accounts ca ON (ca.company_id = c.company_id))
LEFT JOIN (
   SELECT
     com.company_id
   , count(DISTINCT re.invoice_number) total_reservations
   , round(CAST(sum(re.total_price) AS bigint)) total_revenue
   FROM
     ((carshare_v2_staging_reservations re
   LEFT JOIN carshare_v2_staging_corporate_accounts ca ON (ca.user_id = re.user_id))
   LEFT JOIN carshare_v2_staging_companies com ON (com.company_id = ca.company_id))
   WHERE ((re.deleted_at IS NULL) AND (re.status = 'CLOSED'))
   GROUP BY 1
)  r ON (r.company_id = c.company_id))
LEFT JOIN carshare_v2_staging_reservations r1 ON (ca.user_id = r1.user_id))
LEFT JOIN (
   SELECT
     a.company_id
   , sum(a.amount) amount
   FROM
     (
      SELECT
        ca2.company_id
      , (CASE WHEN (or1.reservation_sub_invoice_id IN (SELECT sub_invoice_id
FROM
  carshare_v2_staging_reservation_sub_invoices rsi
WHERE ((rsi.transaction_type <> 'REFUND') AND (rsi.payment_status <> 'PAID'))
)) THEN or1.amount ELSE or1.amount END) amount
      FROM
        ((carshare_v2_staging_outstanding_reservations or1
      LEFT JOIN carshare_v2_staging_reservations r ON (r.reservation_id = or1.reservation_id))
      LEFT JOIN carshare_v2_staging_corporate_accounts ca2 ON (ca2.user_id = r.user_id))
   )  a
   GROUP BY 1
)  os ON (os.company_id = ca.company_id))
LEFT JOIN (
   SELECT
     a.company_id
   , count(DISTINCT a.sub_invoice_id) number_of_recharge_invoices
   , sum(a.amount) total_recharge_amount
   FROM
     (
      SELECT
        c.company_id
      , rsi.sub_invoice_id
      , (CASE WHEN ((rsi.sub_invoice_id = rac.reservation_sub_invoice_id) AND (rsi.apply_tax = 0)) THEN rac.amount WHEN ((rsi.sub_invoice_id = rac.reservation_sub_invoice_id) AND (rsi.apply_tax = 1)) THEN (rac.amount + (rac.amount * 5E-2)) END) amount
      FROM
        ((((carshare_v2_staging_reservations r
      LEFT JOIN carshare_v2_staging_reservation_sub_invoices rsi ON (rsi.reservation_id = r.reservation_id))
      LEFT JOIN carshare_v2_staging_reservation_additional_charges rac ON (rac.reservation_sub_invoice_id = rsi.sub_invoice_id))
      LEFT JOIN carshare_v2_staging_corporate_accounts ca ON (ca.user_id = r.user_id))
      LEFT JOIN carshare_v2_staging_companies c ON (c.company_id = ca.company_id))
      WHERE ((rsi.transaction_type = 'RECHARGE') AND (r.type = 'CORPORATE'))
   )  a
   GROUP BY 1
)  recharge ON (recharge.company_id = c.company_id))
WHERE (c.deleted_at IS NULL)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 18, 19, 20, 21