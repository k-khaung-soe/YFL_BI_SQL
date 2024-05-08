CREATE OR REPLACE VIEW "carshare_v2_rpt_branch_utilization" AS 
SELECT
  created_date
, location_name
, region
, status location_status
, COALESCE(assign_fleet, 0) assign_fleet
, COALESCE(available_utilization_hour, 0) available_utilization_hour
, COALESCE(idle_in_yard, 0) idle_in_yard
, COALESCE(on_rent, 0) on_rent
, COALESCE(replacement, 0) replacement
, COALESCE(drive_car, 0) drive_car
, COALESCE(cleaning_process, 0) cleaning_process
, COALESCE(relocation, 0) relocation
, COALESCE(transit, 0) transit
, COALESCE(panel_shop, 0) panel_shop
, COALESCE(maintenance, 0) maintenance
, COALESCE(pending, 0) pending
, COALESCE(overdue, 0) overdue
, concat(COALESCE(CAST(round(((utilization / available_utilization_hour) * 100)) AS varchar), '0'), '%') utilization
, concat(COALESCE(CAST(round(((drive_car / available_utilization_hour) * 100)) AS varchar), '0'), '%') drive_car_utilization
, concat(COALESCE(CAST(round(((ops_utilization / available_utilization_hour) * 100)) AS varchar), '0'), '%') ops_utilization
, concat(COALESCE(CAST(round(((idle_in_yard / available_utilization_hour) * 100)) AS varchar), '0'), '%') lost_utilization
FROM
  (
   SELECT
     (CASE WHEN (du.created_date IS NOT NULL) THEN CAST(du.created_date AS date) ELSE csl.date END) created_date
   , csl.name location_name
   , csl.region
   , csl.status
   , count(DISTINCT du.vehicle_id) assign_fleet
   , sum(du.available_utilization_hour) available_utilization_hour
   , sum(du.idle_in_yard) idle_in_yard
   , sum(du.on_rent) on_rent
   , sum(du.replacement) replacement
   , sum(du.drive_car) drive_car
   , sum(du.cleaning_process) cleaning_process
   , sum(du.relocation) relocation
   , sum(du.transit) transit
   , sum(du.panel_shop) panel_shop
   , sum(du.maintenance) maintenance
   , sum(du.pending) pending
   , sum(du.overdue) overdue
   , sum(((du.on_rent + du.replacement) + du.overdue)) utilization
   , sum((((((du.cleaning_process + du.relocation) + du.transit) + du.panel_shop) + du.maintenance) + du.pending)) ops_utilization
   FROM
     (carshare_v2_rpt_daily_utilization du
   RIGHT JOIN (
      SELECT
        dt.date
      , l.name
      , l.region
      , l.status
      FROM
        ((
         SELECT DISTINCT CAST(start_time AS date) date
         FROM
           carshare_v2_staging_vehicle_histories
      )  dt
      CROSS JOIN (
         SELECT
           name
         , region
         , status
         FROM
           carshare_v2_staging_locations
      )  l)
      WHERE (dt.date IS NOT NULL)
   )  csl ON ((csl.name = du.location_name) AND (CAST(du.created_date AS date) = csl.date)))
   GROUP BY 1, 2, 3, 4
) 
ORDER BY 1 ASC