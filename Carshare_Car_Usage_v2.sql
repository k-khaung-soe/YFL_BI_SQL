SELECT v.license_plate_number,
	v.name,
	COALESCE(
		SUM(
			DATE_DIFF('second', r.pickup_at, r.actual_end_at) / 60
		),
		0
	) AS "Total Duration",
	CAST(
		json_extract(vehicle_attributes, '$.value[0]') AS VARCHAR
	) AS "Production Year",
	CAST(
		json_extract(vehicle_attributes, '$.value[1]') AS VARCHAR
	) AS "Fuel Type",
	CAST(
		json_extract(vehicle_attributes, '$.value[2]') AS VARCHAR
	) AS "Vehicle Category",
	CAST(
		json_extract(vehicle_attributes, '$.value[3]') AS VARCHAR
	) AS "Transmission",
	CAST(
		json_extract(vehicle_attributes, '$.value[4]') AS VARCHAR
	) AS "Color",
	CAST(
		json_extract(vehicle_attributes, '$.value[5]') AS VARCHAR
	) AS "Engine",
	CAST(
		json_extract(vehicle_attributes, '$.value[6]') AS VARCHAR
	) AS "Build Type",
	CAST(
		json_extract(vehicle_attributes, '$.value[7]') AS VARCHAR
	) AS "Steering",
	CAST(
		json_extract(vehicle_attributes, '$.value[8]') AS VARCHAR
	) AS "Registered State",
	CAST(
		json_extract(vehicle_attributes, '$.value[9]') AS VARCHAR
	) AS "Interior Color",
	CAST(
		json_extract(vehicle_attributes, '$.value[10]') AS VARCHAR
	) AS "Grade",
	CAST(
		json_extract(vehicle_attributes, '$.value[11]') AS VARCHAR
	) AS "Registration Date",
	CAST(
		json_extract_scalar(vehicle_attributes, '$.value[12]') AS VARCHAR
	) AS "Registration Renewal Due Date",
	CAST(
		json_extract_scalar(vehicle_attributes, '$.value[13]') AS VARCHAR
	) AS "Date Due Off Fleet",
	CAST(
		json_extract(vehicle_attributes, '$.value[14]') AS VARCHAR
	) AS "Memo"
FROM carshare_v2_staging_vehicles v
	LEFT JOIN (
		SELECT vehicle_id,
			pickup_at,
			actual_end_at
		FROM carshare_v2_staging_reservations
	) r ON r.vehicle_id = v.vehicle_id
GROUP BY v.license_plate_number,
	vehicle_attributes,
	v.name
ORDER BY "Total Duration";