CREATE OR REPLACE VIEW "carshare_v2_rpt_invoices_details" AS 
SELECT
  r.reservation_id
, rsi.invoice_number invoice_number
, rsi.transaction_type
, CAST(round(sum((CASE WHEN (rsi.sub_invoice_id = or2.reservation_sub_invoice_id) THEN or2.amount WHEN ((rsi.sub_invoice_id = rac.reservation_sub_invoice_id) AND (rsi.apply_tax = 0)) THEN rac.amount WHEN ((rsi.sub_invoice_id = rac.reservation_sub_invoice_id) AND (rsi.apply_tax = 1)) THEN (rac.amount + (rac.amount * 5E-2)) END))) AS int) amount
, r.invoice_number reservation_invoice_number
, rsi.payment_status
, rsi.created_at created_at
, u.username customer_name
, p.erp_account_number
, rsi.memo
, rsi.apply_tax
, rsi.updated_at
FROM
  (((((carshare_v2_staging_reservation_sub_invoices rsi
LEFT JOIN carshare_v2_staging_outstanding_reservations or2 ON (or2.reservation_sub_invoice_id = rsi.sub_invoice_id))
LEFT JOIN carshare_v2_staging_reservation_additional_charges rac ON (rac.reservation_sub_invoice_id = rsi.sub_invoice_id))
LEFT JOIN carshare_v2_staging_reservations r ON (r.reservation_id = rsi.reservation_id))
LEFT JOIN carshare_v2_staging_users u ON (u.user_id = r.user_id))
LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = u.user_id))
GROUP BY 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12