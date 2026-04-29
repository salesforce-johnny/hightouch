with
	route_templates as (
		select distinct
			analytics.mart_drivers.driver_external_id as driver_external_id
			, analytics.data_carriers.external_id as carrier_id
			, analytics.mart_route_templates.external_id as route_template_id__C
			, fulfilled_on
			, fulfilled_on >= current_date::date - interval '7 days' as active_route_driver__c
		from analytics.mart_routes
		left join analytics.mart_route_templates
			on analytics.mart_routes.route_template_id = analytics.mart_route_templates.id
		left join analytics.mart_drivers
			on analytics.mart_routes.driver_id = analytics.mart_drivers.DRIVER_ID
		left join analytics.data_carriers
			on analytics.mart_routes.carrier_id = analytics.data_carriers.id
	)
	, active_route_drivers as (
		SELECT distinct
			driver_external_id
			, route_template_id__c
			, active_route_driver__c
		from route_templates
		where active_route_driver__c = true
		qualify row_number() over (partition by driver_external_id order by fulfilled_on desc nulls last) = 1
	)
	, all_route_drivers as (
		select distinct
			driver_external_id
		from route_templates
	)
	, splits as (
		select
			t.driver_external_id
			, listagg(distinct f.value::string, ';') as Driver_Accessories__c
			, listagg(distinct g.value::string, ';') as Driver_Capabilities__c
			, listagg(distinct h.value::string, ';') as Active_Violation_Types__c
			, listagg(distinct i.value::string, ';') as Lifetime_Violation_types__c
			, listagg(distinct j.value::string, ';') as Vehicle_Accessories__c
			, listagg(distinct k.value::string, ';') as Preferred_Delivery_Methods__c
			, listagg(distinct l.value::string, ';') as Upscale_Delivery_Methods__c
		from analytics.data_drivers t
			, lateral flatten(input => t.driver_accessory_names) f
			, lateral flatten(input => t.driver_capability_names) g
			, lateral flatten(input => t.active_violation_types) h
			, lateral flatten(input => t.lifetime_violation_types) i
			, lateral flatten(input => t.driver_vehicle_accessory_names) j
			, lateral flatten(input => t.preferred_delivery_methods) k
			, lateral flatten(input => t.upscale_delivery_methods) l
		group by t.driver_external_id
		order by t.driver_external_id
	)
	, accessories as (
		select
			driver_external_id
			, f.value::string as accessory_name
		from analytics.data_drivers t
			, lateral flatten(input => t.driver_accessory_names) f
		where f.value::string ilike '%(PPE)%'
	)
	, all_drivers AS (
  		SELECT driver_external_id, MAX(driver_name) AS driver_name, MAX(carrier_external_id) AS carrier_external_id, MAX(carrier_name) AS carrier_name
  		FROM CURRI.ANALYTICS.BT_DELIVERIES
  		WHERE created_at >= DATEADD(day, -30, CURRENT_DATE)
    		AND driver_external_id IS NOT NULL
    		AND delivery_type NOT IN ('route', 'cms')
  		GROUP BY driver_external_id

  		UNION

  		SELECT driver_external_id, MAX(driver_name) AS driver_name, MAX(carrier_external_id) AS carrier_external_id, MAX(carrier_name) AS carrier_name
  		FROM CURRI.ANALYTICS.BT_VIOLATIONS
  		WHERE created_at >= DATEADD(day, -30, CURRENT_DATE)
    		AND driver_external_id IS NOT NULL
    		AND delivery_type NOT IN ('route', 'cms')
  		GROUP BY driver_external_id
	)

	, driver_completions AS (
		SELECT
			c.driver_external_id,
			COUNT(*) AS completed_deliveries,
			COUNT(CASE WHEN av.delivery_external_id IS NULL THEN 1 END) AS accurate_deliveries
		FROM CURRI.ANALYTICS.BT_DELIVERIES c
		LEFT JOIN (
			SELECT DISTINCT delivery_external_id
			FROM CURRI.ANALYTICS.BT_VIOLATIONS
			WHERE created_at >= DATEADD(day, -30, CURRENT_DATE)
				AND delivery_type NOT IN ('route', 'cms')
				AND violation_type_code IN (
					'did_not_follow_delivery_instructions',
					'wrong_vehicle',
					'confirmed_wrong_vehicle',
					'wrong_accessories',
					'did_not_deliver_all_items'
				)
		) av ON c.delivery_external_id = av.delivery_external_id
		WHERE c.created_at >= DATEADD(day, -30, CURRENT_DATE)
			AND c.is_completed_delivery = true
			AND c.driver_external_id IS NOT NULL
			AND c.delivery_type NOT IN ('route', 'cms')
		GROUP BY c.driver_external_id
	)

	, driver_ratings AS (
		SELECT
			driver_external_id,
			AVG(customer_rating) AS avg_customer_rating,
			COUNT(CASE WHEN customer_rating IS NOT NULL THEN 1 END) AS rated_deliveries
		FROM CURRI.ANALYTICS.BT_DELIVERIES
		WHERE is_completed_delivery = true
			AND driver_external_id IS NOT NULL
			AND delivery_type NOT IN ('route', 'cms')
		GROUP BY driver_external_id
	)

	, driver_completion_viols AS (
		SELECT driver_external_id, COUNT(*) AS completion_violation_count
		FROM CURRI.ANALYTICS.BT_VIOLATIONS
		WHERE created_at >= DATEADD(day, -30, CURRENT_DATE)
			AND violation_type_code IN (
				'self_unassignment',
				'scheduled_delivery_failure',
				'did_not_complete_multistop_delivery'
			)
			AND driver_external_id IS NOT NULL
			AND delivery_type NOT IN ('route', 'cms')
		GROUP BY driver_external_id
	)

	, driver_eta AS (
		SELECT
			d.driver_external_id,
			COUNT(*) AS eta_delivery_count,
			ROUND(
			COUNT(CASE WHEN e.seconds_to_origin_delta / 60.0 BETWEEN -30 AND 27 THEN 1 END)
			/ NULLIF(COUNT(*), 0) * 100, 2
			) AS pickup_ontime_pct,
			ROUND(
			COUNT(CASE WHEN e.seconds_to_destination_delta / 60.0 BETWEEN -30 AND 12 THEN 1 END)
			/ NULLIF(COUNT(*), 0) * 100, 2
			) AS dropoff_ontime_pct
		FROM CURRI.ANALYTICS.DELIVERY_DRIVER_ETAS_VS_ATAS e
		JOIN CURRI.ANALYTICS.BT_DELIVERIES d ON e.delivery_id = d.delivery_id
		WHERE d.created_at >= DATEADD(day, -30, CURRENT_DATE)
			AND d.is_completed_delivery = true
			AND d.driver_external_id IS NOT NULL
			AND d.delivery_type NOT IN ('route', 'cms')
		GROUP BY d.driver_external_id
	)
select distinct
-- driver basics & ids
    analytics.data_drivers.driver_external_id as pk
	, left(first_name, 40) as FirstName
	, left(last_name, 40) as LastName
	, analytics.data_drivers.driver_external_id as Driver_External_ID__c
	, analytics.data_drivers.carrier_external_id as Carrier_External_ID__c
	, Route_Template_ID__c
	-- , stripe_account_id as Stripe_Account_ID__c
	, case 
	    when phone_number ilike 'tel://%' 
	         and regexp_replace(phone_number, 'tel://.+(\\+1)?(\\d{10})', '\\2') is not null
	         then '+1' || regexp_replace(phone_number, 'tel://.+(\\+1)?(\\d{10})', '\\2')
		when regexp_like(phone_number, '(\\+1[\\s-]*)*(\\(\\d{3}\\)|\\d{3})([\\s.-]*)(\\d{3})([\\s.-]*)(\\d{4})')
			then case 
				when regexp_like(phone_number, '^\\+?1?[\\s-]*\\(\\d{3}\\)')
				  then regexp_replace(
						 phone_number, 
						 '^(\\+1[\\s-]*)?\\((\\d{3})\\)[\\s.-]*(\\d{3})[\\s.-]*(\\d{4})$', 
						 '+1\\2\\3\\4'
					   )
				else regexp_replace(
						 phone_number, 
						 '^(\\+1[\\s-]*)?(\\d{3})[\\s.-]*(\\d{3})[\\s.-]*(\\d{4})$', 
						 '+1\\2\\3\\4'
					   )
				end
	    when phone_number ilike 'tel%' then null
	    when phone_number ilike '%del%' then null
	    when phone_number ilike '--' then null
	    when phone_number = '' then null
	    when regexp_like(phone_number, '^\\d{10}$') then '+1' || phone_number
	    when regexp_like(phone_number, '^1\\d{10}$') then '+' || phone_number
	    when regexp_like(phone_number, '^\\+1\\d{10}$') then phone_number
	    when regexp_like(phone_number, '^1\\d{10}.*') then '+' || left(phone_number, 11)
	    when regexp_like(phone_number, '^\\d{10}.*') then '+1' || left(phone_number, 10)
	    else phone_number 
	  end as Phone
 	, lower(trim(analytics.data_drivers.email_address)) as Email
	--	status
	, activity_level_status as Activity_Level_Status__c
	, activity_level as Activity_Level__c
	, Active_Route_Driver__c             
	, analytics.data_drivers.status as Status__c
	, has_active_violation as Has_Active_Violation__c
	--	driver & carrier info
	, acquisition_source as Acquisition_Source__c
	, carrier_permissions as Carrier_Permissions__c
	, case when carrier_id is not null then true else false end as Is_Carrier__c
	, driver_notification_preference as Driver_Notification_Preference__c
	, driver_type as Driver_Type__c
	, is_dsp as Is_DSP__c
	, is_preferred_driver as Is_Preferred_Driver__c
	, case when all_route_drivers.driver_external_id is not null then true else false end as Is_Route_Driver__c
--	checkr
	, checkr_invitation_status as Checkr_Invitation_Status__c
	, to_varchar(convert_timezone('UTC', analytics.data_drivers.created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Created_At__c
	, to_varchar(convert_timezone('UTC', carrier_approved_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Carrier_Approved_At__c
	, to_varchar(convert_timezone('UTC', carrier_created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Carrier_Created_At__c
	, to_varchar(convert_timezone('UTC', checkr_invitation_created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Checkr_Invitation_Created_At__c
	, to_varchar(convert_timezone('UTC', checkr_invitation_completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Checkr_Invitation_Completed_At__c
	, to_varchar(convert_timezone('UTC', first_checkr_invitation_created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as First_Checkr_Invitation_Created_At__c
	, to_varchar(convert_timezone('UTC', first_checkr_invitation_completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as First_Checkr_Invitation_Completed_At__c
	, to_varchar(convert_timezone('UTC', first_banned_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as First_Banned_at__c
	, to_varchar(convert_timezone('UTC', last_banned_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Banned_At__c
	, to_varchar(convert_timezone('UTC', first_violation_created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as First_Violation_Created_At__c
	, to_varchar(convert_timezone('UTC', last_violation_created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Violation_Created_At__c
	, to_varchar(convert_timezone('UTC', address_created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Address_Created_At__c
	, to_varchar(convert_timezone('UTC', last_request_sent), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Request_Sent__c
	, to_varchar(convert_timezone('UTC', last_request_accepted), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Request_Accepted__c
	, to_varchar(convert_timezone('UTC', last_request_viewed), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Request_Viewed__c
	, to_varchar(convert_timezone('UTC', first_delivery_completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as First_Delivery_Completed_At__c
	, to_varchar(convert_timezone('UTC', third_delivery_completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Third_Delivery_Completed_At__c
	, to_varchar(convert_timezone('UTC', fifth_delivery_completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Fifth_Delivery_Completed_At__c
	, to_varchar(convert_timezone('UTC', last_delivery_completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Delivery_Completed_At__c
	, to_varchar(convert_timezone('UTC', last_hotshot_completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Hotshot_Completed_At__c
	, to_varchar(convert_timezone('UTC', last_route_completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Route_Completed_At__c
	, to_varchar(convert_timezone('UTC', last_delivery_assignment), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Delivery_Assignment__c
	, checkr_invitation_id as Checkr_Invitation_ID__c
	, first_checkr_invitation_id as First_Checkr_Invitation_ID__c
	, first_checkr_invitation_status as First_Checkr_Invitation_Status__c	
	, first_background_check_approval as First_Background_Check_Approval__c
	, first_background_check_rejection as First_Background_Check_Rejection__c
	, last_background_check_approval as Last_Background_Check_Approval__c
	, last_background_check_rejection as Last_Background_Check_Rejection__c
	, has_background_check_passed as Has_Background_Check_Passed__c
	, checkr_candidate_id as Checkr_Candidate_ID__c
--	bans
	, total_bans as Total_Bans__c
	, user_who_banned_driver_first as User_Who_Banned_Driver_First__c
	, user_who_banned_driver_last as User_Who_Banned_Driver_Last__c
	, first_ban_reason as First_Ban_Reason__c
	, last_ban_reason as Last_Ban_Reason__c
	, is_banned as Is_Banned__c
--	violations
	, active_violation_count as Active_Violation_Count__c
	, lifetime_violation_count as Lifetime_Violation_Count__c
	, most_common_violation as Most_Common_Violation__c
--	geolocation
	, address_line_1 as Address_Line_1__c
	, address_line_2 as Address_Line_2__c
	, left(city, 40) as City__c
	, case 
		when lower(trim(state)) like '%alabama%' then 'AL'
		when lower(trim(state)) like '%alaska%' then 'AK'
		when lower(trim(state)) like '%arizona%' then 'AZ'
		when lower(trim(state)) like '%arkansas%' then 'AR'
		when lower(trim(state)) like '%california%' then 'CA'
		when lower(trim(state)) like '%colorado%' then 'CO'
		when lower(trim(state)) like '%connecticut%' then 'CT'
		when lower(trim(state)) like '%delaware%' then 'DE'
		when lower(trim(state)) like '%district%of%columbia%' then 'DC'
		when lower(trim(state)) like '%florida%' then 'FL'
		when lower(trim(state)) like '%georgia%' then 'GA'
		when lower(trim(state)) like '%hawaii%' then 'HI'
		when lower(trim(state)) like '%idaho%' then 'ID'
		when lower(trim(state)) like '%illinois%' then 'IL'
		when lower(trim(state)) like '%indiana%' then 'IN'
		when lower(trim(state)) like '%iowa%' then 'IA'
		when lower(trim(state)) like '%kansas%' then 'KS'
		when lower(trim(state)) like '%kentucky%' then 'KY'
		when lower(trim(state)) like '%louisiana%' then 'LA'
		when lower(trim(state)) like '%maine%' then 'ME'
		when lower(trim(state)) like '%maryland%' then 'MD'
		when lower(trim(state)) like '%massachusetts%' then 'MA'
		when lower(trim(state)) like '%michigan%' then 'MI'
		when lower(trim(state)) like '%minnesota%' then 'MN'
		when lower(trim(state)) like '%mississippi%' then 'MS'
		when lower(trim(state)) like '%missouri%' then 'MO'
		when lower(trim(state)) like '%montana%' then 'MT'
		when lower(trim(state)) like '%nebraska%' then 'NE'
		when lower(trim(state)) like '%nevada%' then 'NV'
		when lower(trim(state)) like '%new%hampshire%' then 'NH'
		when lower(trim(state)) like '%new%jersey%' then 'NJ'
		when lower(trim(state)) like '%new%mexico%' then 'NM'
		when lower(trim(state)) like '%new%york%' then 'NY'
		when lower(trim(state)) like '%north%carolina%' then 'NC'
		when lower(trim(state)) like '%north%dakota%' then 'ND'
		when lower(trim(state)) like '%ohio%' then 'OH'
		when lower(trim(state)) like '%oklahoma%' then 'OK'
		when lower(trim(state)) like '%oregon%' then 'OR'
		when lower(trim(state)) like '%pennsylvania%' then 'PA'
		when lower(trim(state)) like '%rhode%island%' then 'RI'
		when lower(trim(state)) like '%south%carolina%' then 'SC'
		when lower(trim(state)) like '%south%dakota%' then 'SD'
		when lower(trim(state)) like '%tennessee%' then 'TN'
		when lower(trim(state)) like '%texas%' then 'TX'
		when lower(trim(state)) like '%utah%' then 'UT'
		when lower(trim(state)) like '%vermont%' then 'VT'
		when lower(trim(state)) like '%virginia%' then 'VA'
		when lower(trim(state)) like '%washington%' then 'WA'
		when lower(trim(state)) like '%west%virginia%' then 'WV'
		when lower(trim(state)) like '%wisconsin%' then 'WI'
		when lower(trim(state)) like '%wyoming%' then 'WY'
		when upper(trim(state)) in ('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC') then upper(trim(state))
		else null end as State__c
	, analytics.data_drivers.cbsa_name as CBSA__c
	, zip_code as Zip_Code__c
	, analytics.data_drivers.zcta as ZCTA__c
	, analytics.data_drivers.major_region as Major_Region__c
	, address_latitude as Latitude__c
	, address_longitude as Longitude__c
	, zip_code_latitude as Zip_Code_Latitude__c
	, zip_code_longitude as Zip_Code_Longitude__c
--	delivery activity
	, total_completed_deliveries as Total_Completed_Deliveries__c
	, total_hotshots_deliveries_completed as Total_Hotshot_Deliveries_Completed__c
	, total_route_deliveries_completed as Total_Route_Deliveries_Completed__c
--	 ratings & scoring
	, total_ratings as Total_Ratings__c
	, total_customer_ratings as Total_Customer_Ratings__c
	, total_internal_ratings as Total_Internal_Ratings__c
	, average_customer_rating as Average_Customer_Rating__c
	, average_internal_rating as Average_Internal_Rating__c
	, weighted_average_rating as Weighted_Average_Rating__c
	, all_time_score as All_Time_Score__c
	, on_time_destination_score as On_Time_Destination_Score__c
	, on_time_origin_score as On_Time_Origin_Score__c
	, on_time_score as On_Time_Score__c
	, photos_score as Photos_Score__c
	, scorecard_score as Scorecard_Score__c
	, scorecard_unassignments as Scorecard_Unassignments__c
--	vehicle	
	, vehicle_cargo_capacity as Vehicle_Cargo_Capacity__c
	, vehicle_max_payload as Vehicle_Max_Payload__c
	, vehicle_max_tow as Vehicle_Max_tow__c
	, vehicle_teoalida_id as Vehicle_Teoalida_ID__c
	, vehicle_year as Vehicle_Year__c
	, vehicle_class as Vehicle_Class__c
	, vehicle_make as Vehicle_Make__c
	, vehicle_model as Vehicle_Model__c
	, vehicle_trim as Vehicle_Trim__c
-- driver scorecard per driver 20260420
	, COALESCE(driver_completions.completed_deliveries, 0) AS scorecard_completed_deliveries__c
	, COALESCE(driver_completion_viols.completion_violation_count, 0) AS scorecard_completion_violations__c
	, ROUND(
		COALESCE(driver_completions.completed_deliveries, 0)
		/ NULLIF(COALESCE(driver_completions.completed_deliveries, 0) + COALESCE(driver_completion_viols.completion_violation_count, 0), 0)
		* 100, 2
	) AS scorecard_completion_rate__c
	, COALESCE(driver_completions.accurate_deliveries, 0) AS scorecard_accurate_deliveries__c
	, ROUND(
		COALESCE(driver_completions.accurate_deliveries, 0)
		/ NULLIF(COALESCE(driver_completions.completed_deliveries, 0), 0)
		* 100, 2
	) AS scorecard_delivery_accuracy__c
	, CASE WHEN COALESCE(driver_ratings.rated_deliveries, 0) > 0 THEN ROUND(driver_ratings.avg_customer_rating, 2) END AS scorecard_average_customer_rating__c
	, COALESCE(driver_ratings.rated_deliveries, 0) AS scorecard_rated_deliveries__c
	, driver_eta.eta_delivery_count AS scorecard_eta_delivery_count__c
	, driver_eta.pickup_ontime_pct AS scorecard_pickup_ontime_percent__c
	, driver_eta.dropoff_ontime_pct AS scorecard_dropoff_ontime_percent__c
--	boolean qualifiers
	, has_app_installed as Has_App_Installed__c
	-- , has_bank_account_on_file as Has_Bank_Account_on_File__c
	-- , has_card_on_file as Has_Card_on_File__c
	, has_profile_photo as Has_Profile_Photo__c
	, went_through_carrier_flow as Went_Through_Carrier_Flow__c
	, 'Driver' as contact_record_type
	, splits.Driver_Accessories__c
	, splits.Driver_Capabilities__c
	, splits.Active_Violation_Types__c
	, splits.Lifetime_Violation_types__c
	, splits.Vehicle_Accessories__c
	, splits.Preferred_Delivery_Methods__c
	, splits.Upscale_Delivery_Methods__c
	, case
		when array_contains('ppe-certified'::variant, analytics.data_drivers.driver_tags) then 'Certified'
		when (not array_contains('ppe-certified'::variant, analytics.data_drivers.driver_tags) or analytics.data_drivers.driver_tags is null) and analytics.data_drivers.driver_external_id in (select driver_external_id from accessories) then 'Unverified'
		else 'None' end as PPE_certification
from analytics.data_drivers
left join active_route_drivers
	on analytics.data_drivers.driver_external_id = active_route_drivers.driver_external_id
left join all_route_drivers
	on analytics.data_drivers.driver_external_id = all_route_drivers.driver_external_id
left join splits
	on analytics.data_drivers.driver_external_id = splits.driver_external_id
left join all_drivers
	on analytics.data_drivers.driver_external_id = all_drivers.driver_external_id
left join driver_completions
	on analytics.data_drivers.driver_external_id = driver_completions.driver_external_id
left join driver_ratings
	on analytics.data_drivers.driver_external_id = driver_ratings.driver_external_id
left join driver_completion_viols
	on analytics.data_drivers.driver_external_id = driver_completion_viols.driver_external_id
left join driver_eta
	on analytics.data_drivers.driver_external_id = driver_eta.driver_external_id
where 1=1
	and first_name is not null
	and last_name is not null
	and first_name <> ''
	and last_name <> ''
	-- invalid emails
	and email_address not in ('Jameseburke99@gmail', 'darion92moody@.gmail.com', '.davidjohnwetmore@gmail.com', '.mhp900@aol.com', '@jnyemelah@gmail.com', '@plinioclamper@gmail.com', '1345@tux@gmail.com', '5257 rl63@gmail.com', '6236941448', 'acj63002@gmail@com', 'ahmadalexander@31@gmail.com', 'aissiousouheib@gmail..com', 'ajaanisah1@gmail.comsah1@gmail.com', 'akashdeepscolia@786@gmail.com', 'alban@blackgauntlet60@gmail.com', 'alcosey55@gmail..com', 'alfredo.vazquez@13@yahoo.com', 'alyssa kennedy59@gmail.com', 'amccloud74@yahoo@com', 'américa.61@hotmail.com', 'andrés.beltran@aol.com', 'ángel-pérez-123@hotmail.con', 'ave@462@aol.com', 'azulito68@gmail@com', 'b jordan2809@yahoo.com', 'barry polk123@gmail.com', 'barryturner2019 barryturner2019@gmail.com', 'bedat@85@yahoo.com', 'bhbasnet33@gmail..com', 'blacknwhitefamous@yahoo', 'bryce@yrd15@gmail.com', 'buds@960@yahoo.com', 'cal roland1@aol.com', 'calhounservices 18@yahoo.com', 'cedricksamuelsr@gmail.com@gmail.com', 'chester jackson87@gmail.com', 'chris tiamzon8273@gmail.com', 'chris12422@.gmail.com', 'chynalynaecanady@gmail@com', 'cjackson9476cjackson9476@gmail.com@gmail.com', 'climafan33@.gmail.com', 'codiet2018@gmail@com', 'comeaux chasity@gmail.com', 'crawford_ brothers@yahoo.com', 'çreed101514@gmail.com', 'crucial 264@gmail.com', 'damonjaimitchellsr@yahoo@com', 'dan theman.kundell@gmail.com', 'daniel clausen.usa@gmail.com', 'danielbloodline 24@gmail.com', 'danielbloodline 24@gmail.com', 'danielhyacinthe 123@gmail.com', 'daniellemaman1@gmail.com@gmail.com', 'david.k.rolon@gmail..com', 'dbolte 1214@gmail.com', 'dcher9802@gmail.com.', 'dee.fig602@gmail', 'deebrown 1224@gmail.com', 'dlrlogistics@2@gmail.com', 'dontaebrooks .db.db@gmail.com', 'dpresley8822@gmail.com to', 'dukeboyant255@gmail.com@gmail.com', 'e t.diaz329@gmail.com', 'eduardolbck@gmail.com garciavictor468@yahoo.com', 'eıizaıde02r2@gmail.com', 'entreprenew22@gmail@com', 'envyme .sb@gmail.com', 'ericvan229@gmail@com', 'evr101287@.icloud.com', 'familiatrujillo1993@gmail@com', 'fernandosalas705@gmailcom', 'flip.sam.8382@gmail.com@gmail.com', 'fowler.outwork@gmail@com', 'fox@kaleb@aol.com', 'funmiadesany53@gmail.com@gmail.com', 'ga mcmichael@gmail.com', 'gabrielmu20032003@gmail@com', 'garlandallen00@com', 'gary hambers@hotmail.com', 'george craig173624@gmail.com', 'gerald@gibbons112@yahoo.com', 'gordon brandow5@gmail.com', 'gugutrevino@hotmail@com', 'hamzapico04@gmail@com', 'heàtherstrommen@gmail.com', 'helım1976@gmail.com', 'heywesss@icloud@com', 'hhusen44@yahoo.com hhusen44@yahoo.com', 'hmarc 121480@gmail.com', 'hochy.gratereaux .ortega@gmail.com', 'hvn21934@gmail.', 'ihereck@gmail@com', 'imorales1935@gmail@com', 'iñanavelazquez04@gmail.com', 'ivalissa23@gmzy', 'iván.moreno100990@gmail.com', 'ivanpolanco@026@gmail.com', 'j. loretto14@gmail.com', 'jadenleighb123@gmail.com@gmail.com', 'jairosolares74@.gmail.com', 'Jameseburke99@gmail', 'jarredbowie14@gmail.comjarredbowie14@gmail.com', 'jay.opoku@yahoo@com', 'jbowers215@icloud@com', 'jdizzy2]]3@icloud.com', 'jeremeinw@.gmail.com', 'JessUrban2021', 'jguadarrama1819@yahoo.co.', 'jjermand n8@gmail.com', 'ĵkayla305@gmail.com', 'jl transportation.usa@gmail.com', 'joe.2326@yahoo.co.', 'jonesandbrittany@1118@gmail.com', 'jordydelgadofifa@fifa@gmail.com', 'josé.ortiz.newworld@gmail.com', 'joshuahinson82@gmail@com', 'jrr@gmail@gmail.com', 'jtjonathan@091@gmail.com', 'justin.meeker 1@gmail.com', 'jvredblvck@gmail.com@gmail.com', 'k b_business709@yahoo.com', 'k citocordova@gmail.com', 'kalanda.booker 4cdlmail@gmail.com', 'keddricklabady@gmail@com', 'ketoyadobbins@gmail@com', 'kevinfarris174@gmail@com', 'kevinfarris174@gmail@com', 'kh@aliljm98@gmail.com', 'kobef477@gmail@com', 'kristonchristopher@gmail@gmail.com', 'kydtzsen@yahoo@com', 'kytiarajackson@yahoo@com', 'l.dean1307@gmail@com', 'lanna townley19@gmail.com', 'laquishalee989@gmail..com', 'lashea.baker@.gmail.com', 'lbrb66@66@yahoo.com', 'leon .stone@me.com', 'leonard transport@yahoo.com', 'll llmarkpaynell@gmail.com', 'lramos97877@icloud@icloud.com', 'makengson 119@yahoo.fr', 'maliquewebster64@gmail@com', 'malvinnunez14@gmail@com', 'mannyboy@1@yahoo.com', 'mannyboy@1@yahoo.com', 'markmanicad0328@icloud@com', 'martinez fred51@gmail.com', 'martínezmercedes120@yahoo.com', 'Matt.Maxcy@ gmail.com', 'mconeyjr@omnitransllc.netmconeyjr@omnitransllc.net', 'michael kocak 42@gmail.com', 'michaelyeboah227@gmail.com@gmail.com', 'michelaraluce 57@gmail.com', 'miguelangelolaguiver96@gmail.com@gmail.com', 'miguelmoralesss2021@gmail@com', 'mike samuel 66@gmail.com', 'misslovely364@gmail@com', 'mjmason0[12@icloud.com', 'mmoore.workflow@gmail@com', 'mrsedchase@gmail@com', 'msfgadson@gmail@com', 'nazrul islam228+curri@gmail.com', 'nca@13@uakron.edu', 'norgem.perijatours@gmail.comjhhh j jgtht', 'null@null', 'nwc1107@gmail.com@gmail.com', 'oacounts@chingarande@gmail.com', 'olando@wisetek@gmail.com', 'omar_banales@yahoo@com', 'oneoffcustombillet@billet@yahoo.com', 'organdonor 54@gmail.com', 'p_ gilliam@yahoo.com', 'pablo@empresaswansonllc@gmail.com', 'pamela k doyle@gmail.com', 'parksm@91@yahoo.com', 'patríciasenna@protonmail.com', 'pauld transportation@gmail.com', 'peter.@1388@gmail.com', 'phonz1313@gmail@com', 'pickleroy@yahoo.co.', 'picnicparties@oh@gmail.com', 'piper t19731415@aol.com', 'plan4u services@gmail.com', 'princefrankline@21@gmail.com', 'pullenteejay@yahoo@com', 'quantae.davis@icloud@com', 'queonda777@live.com hi', 'raúl.lumbi@yahoo.com', 'ray.wilson@pandtlinc@com', 'razi.lawrence@yahoo@com', 'renemolina21@.gmail.com', 'rivera.pedro82@yahoo@com', 'rmarshall@blytheville@schools.net', 'robert 20k@yahoo.com', 'robert44 bunker@gmail.com', 'robriley43@yahoo.com@yahoo.com', 'rockcity1000@gmail@com', 'rocthedoc420@yahoo..com', 'rtblandscaping@gmail@gmail.com', 'ruthmateo 1774@gmail.com', 's davidbenedictla@gmail.com', 's m.fyne7@gmail.com', 's@dipina@me.com', 'sam cornett710@gmail.com', 'sammysalman1990@gmail.com u', 'samoh2336@gmail@com', 'samuelserrab032629@gmail@com', 'scotte.eric@72@outlook.com', 'sean@hitrefreshcom', 'seegmooregmail@com', 'seegmooregmail@com', 'Sethcantrll19&gmail.com', 'shaw_tiffanyn@yahoo@com', 'sheila hunter.az@icloud.com', 'shinikataylor@yahoo@com', 'simbasir@gmail@com', 'sirrichard’sllc2023@gmail.com', 'soccerchepe@yahoo..com', 'southernpearl100@icloud@com', 'stelhanwatts@02@yahoo.com', 'stelizyco@gmail..com', 'stephanied518@gmail@gmail.com', 'stowers james 772@gmail.com', 'Stumpnocker40@gmailcom', 'stunna@1@icloud.com', 'sue_chambers 1023@yahoo.com', 'sweetjsz@yahoo@com', 't. dan.kyle@gmail.com', 'tàm_brooks5888@gmail.com', 'tbozeman44@icloud@com', 'telskoe60:@gmail', 'telskoe60:@gmail', 'teresa.arteaga@1111@gmail.com', 'tesfalem mengustu@gmail.com', 'thedorsecodeshow@gmail@com', 'trapperfisbeck@com', 'traylon919@gmail@com', 'trucogm@gmail', 'tsmith1575@gmail@com', 'tylanbrinson25@gmail@com', 'tynosown1977@yahoo@com', 'v81722@gmail.com@gmail.com', 'vanishia30@gmail@com', 'venittoreji@icloud@gmail.com', 'victoria .lazos@yahoo.com', 'wevestinc@gmail@com', 'wildfricke78@gmail.com@gmail.com', 'wildfricke78@gmail.com@gmail.com', 'wiseguylogisticsllc@.gmail.com', 'workishettetime9@gmail.com@gmail.com', 'wrong ##', 'yanniel valier quian@gmail.com', 'yanniel valier quian@gmail.com', 'Yes', 'yngjrañer85@gmail.com', 'yolandag 414@yahoo.com', 'ysleta13@com', 'zayterrelll@gmail@com', 'zuleikawright49@gmail@com')
	and phone_number not like '%del%'
qualify row_number() over (partition by analytics.data_drivers.driver_external_id order by last_request_viewed desc nulls last) = 1
order by analytics.data_drivers.driver_external_id