select 
--	account basics
	analytics.data_carriers.external_id as pk
    , analytics.data_carriers.name as Name
	, analytics.data_carriers.external_id as Carrier_External_ID__c
	, analytics.data_drivers.driver_external_id as Driver_External_ID__c
	, analytics.data_carriers.email_address::string as email_address
	, analytics.data_carriers.denial_context as Denial_Context__c	
	, case 
	    when analytics.data_carriers.phone_number ilike 'tel://%' 
	         and regexp_replace(analytics.data_carriers.phone_number, 'tel://.+(\\+1)?(\\d{10})', '\\2') is not null
	         then '+1' || regexp_replace(analytics.data_carriers.phone_number, 'tel://.+(\\+1)?(\\d{10})', '\\2')
		when regexp_like(analytics.data_carriers.phone_number, '(\\+1[\\s-]*)*(\\(\\d{3}\\)|\\d{3})([\\s.-]*)(\\d{3})([\\s.-]*)(\\d{4})')
			then case 
				when regexp_like(analytics.data_carriers.phone_number, '^\\+?1?[\\s-]*\\(\\d{3}\\)')
				  then regexp_replace(
						 analytics.data_carriers.phone_number, 
						 '^(\\+1[\\s-]*)?\\((\\d{3})\\)[\\s.-]*(\\d{3})[\\s.-]*(\\d{4})$', 
						 '+1\\2\\3\\4'
					   )
				else regexp_replace(
						 analytics.data_carriers.phone_number, 
						 '^(\\+1[\\s-]*)?(\\d{3})[\\s.-]*(\\d{3})[\\s.-]*(\\d{4})$', 
						 '+1\\2\\3\\4'
					   )
				end
	    when analytics.data_carriers.phone_number ilike 'tel%' then null
	    when analytics.data_carriers.phone_number ilike '%del%' then null
	    when analytics.data_carriers.phone_number ilike '--' then null
	    when analytics.data_carriers.phone_number = '' then null
	    when regexp_like(analytics.data_carriers.phone_number, '^\\d{10}$') then '+1' || analytics.data_carriers.phone_number
	    when regexp_like(analytics.data_carriers.phone_number, '^1\\d{10}$') then '+' || analytics.data_carriers.phone_number
	    when regexp_like(analytics.data_carriers.phone_number, '^\\+1\\d{10}$') then analytics.data_carriers.phone_number
	    when regexp_like(analytics.data_carriers.phone_number, '^1\\d{10}.*') then '+' || left(analytics.data_carriers.phone_number, 11)
	    when regexp_like(analytics.data_carriers.phone_number, '^\\d{10}.*') then '+1' || left(analytics.data_carriers.phone_number, 10)
	    else analytics.data_carriers.phone_number end as Phone
	, analytics.data_carriers.note as Note__c
	, analytics.data_carriers.activity_level_status as Activity_Level_Status__c
	, analytics.data_carriers.activity_level as Activity_Level__c
	, analytics.data_carriers.status as Status__c
	, array_to_string(analytics.data_carriers.preferences, ';') as Preferences__c
	--	dates
	,  to_varchar(convert_timezone('UTC', analytics.data_carriers.approved_on), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Approved_On__c
	,  to_varchar(convert_timezone('UTC', analytics.data_carriers.created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Created_At__c
	,  to_varchar(convert_timezone('UTC', analytics.data_carriers.last_request_sent), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Request_Sent__c
	,  to_varchar(convert_timezone('UTC', analytics.data_carriers.last_request_accepted), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Request_Accepted__c
	,  to_varchar(convert_timezone('UTC', analytics.data_carriers.last_delivery_assignment), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Delivery_Assignment__c
	,  to_varchar(convert_timezone('UTC', analytics.data_carriers.last_delivery_completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Delivery_Completed_At__c
	,  to_varchar(convert_timezone('UTC', analytics.data_carriers.last_hotshot_completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Hotshot_Completed_At__c
	,  to_varchar(convert_timezone('UTC', analytics.data_carriers.last_msd_completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_MSD_Completed_At__c
	,  to_varchar(convert_timezone('UTC', analytics.data_carriers.last_route_completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Route_Completed_At__c
	--	driver & vehicle info
	, analytics.data_carriers.team_driver_count as Team_Driver_Count__c
	, analytics.data_carriers.team_vehicle_count as Team_Vehicle_Count__c
	, analytics.data_carriers.team_member_zips as Team_Member_Zips__c
	, analytics.data_carriers.zips_with_complete_orders as Zips_with_Complete_Orders__c
	, analytics.data_carriers.team_box_truck_count as Team_Box_Truck_Count__c
	, analytics.data_carriers.team_box_truck_with_liftgate_count as Team_Box_Truck_with_Liftgate_Count__c
	, analytics.data_carriers.team_box_truck_with_ramp_count as Team_Box_Truck_with_Ramp_Count__c
	, analytics.data_carriers.team_box_truck_with_liftgate_and_ramp_count as Team_Box_Truck_with_Liftgate_and_Ramp_Co__c
	, analytics.data_carriers.team_car_count as Team_Car_Count__c
	, analytics.data_carriers.team_cargo_van_count as Team_Cargo_Van_Count__c
	, analytics.data_carriers.team_cargo_van_high_roof_count as Team_Cargo_Van_High_Roof_Count__c
	, analytics.data_carriers.team_flatbed_count as Team_Flatbed_Count__c
	, analytics.data_carriers.team_rack_vehicle_count as Team_Rack_Vehicle_Count__c
	, analytics.data_carriers.team_stakebed_count as Team_Stakebed_Count__c
	, analytics.data_carriers.team_stakebed_with_liftgate_and_ramp_count as Team_Stakebed_with_Liftgate_and_Ramp_Cou__c
	, analytics.data_carriers.team_stakebed_with_liftgate_count as Team_Stakebed_with_Liftgate_Count__c
	, analytics.data_carriers.team_stakebed_with_ramp_count as Team_Stakebed_with_Ramp_Count__c
	, analytics.data_carriers.team_suv_count as Team_SUV_Count__c
	, analytics.data_carriers.team_trailer_count as Team_Trailer_Count__c
	, analytics.data_carriers.team_truck_count as Team_Truck_Count__c
	--	accounts stats
	, analytics.data_carriers.total_orders as Total_Orders__c
	, analytics.data_carriers.total_hotshots as Total_Hotshots__c
	, analytics.data_carriers.total_msds as Total_MSDs__c
	, analytics.data_carriers.total_routes as Total_Routes__c
	--	geolocation
	, analytics.data_carriers.address_line_1 as Address_Line_1__c
	, analytics.data_carriers.city as City__c
	, case 
		when lower(trim(analytics.data_carriers.state)) like '%alabama%' then 'AL'
		when lower(trim(analytics.data_carriers.state)) like '%alaska%' then 'AK'
		when lower(trim(analytics.data_carriers.state)) like '%arizona%' then 'AZ'
		when lower(trim(analytics.data_carriers.state)) like '%arkansas%' then 'AR'
		when lower(trim(analytics.data_carriers.state)) like '%california%' then 'CA'
		when lower(trim(analytics.data_carriers.state)) like '%colorado%' then 'CO'
		when lower(trim(analytics.data_carriers.state)) like '%connecticut%' then 'CT'
		when lower(trim(analytics.data_carriers.state)) like '%delaware%' then 'DE'
		when lower(trim(analytics.data_carriers.state)) like '%district%of%columbia%' then 'DC'
		when lower(trim(analytics.data_carriers.state)) like '%florida%' then 'FL'
		when lower(trim(analytics.data_carriers.state)) like '%georgia%' then 'GA'
		when lower(trim(analytics.data_carriers.state)) like '%hawaii%' then 'HI'
		when lower(trim(analytics.data_carriers.state)) like '%idaho%' then 'ID'
		when lower(trim(analytics.data_carriers.state)) like '%illinois%' then 'IL'
		when lower(trim(analytics.data_carriers.state)) like '%indiana%' then 'IN'
		when lower(trim(analytics.data_carriers.state)) like '%iowa%' then 'IA'
		when lower(trim(analytics.data_carriers.state)) like '%kansas%' then 'KS'
		when lower(trim(analytics.data_carriers.state)) like '%kentucky%' then 'KY'
		when lower(trim(analytics.data_carriers.state)) like '%louisiana%' then 'LA'
		when lower(trim(analytics.data_carriers.state)) like '%maine%' then 'ME'
		when lower(trim(analytics.data_carriers.state)) like '%maryland%' then 'MD'
		when lower(trim(analytics.data_carriers.state)) like '%massachusetts%' then 'MA'
		when lower(trim(analytics.data_carriers.state)) like '%michigan%' then 'MI'
		when lower(trim(analytics.data_carriers.state)) like '%minnesota%' then 'MN'
		when lower(trim(analytics.data_carriers.state)) like '%mississippi%' then 'MS'
		when lower(trim(analytics.data_carriers.state)) like '%missouri%' then 'MO'
		when lower(trim(analytics.data_carriers.state)) like '%montana%' then 'MT'
		when lower(trim(analytics.data_carriers.state)) like '%nebraska%' then 'NE'
		when lower(trim(analytics.data_carriers.state)) like '%nevada%' then 'NV'
		when lower(trim(analytics.data_carriers.state)) like '%new%hampshire%' then 'NH'
		when lower(trim(analytics.data_carriers.state)) like '%new%jersey%' then 'NJ'
		when lower(trim(analytics.data_carriers.state)) like '%new%mexico%' then 'NM'
		when lower(trim(analytics.data_carriers.state)) like '%new%york%' then 'NY'
		when lower(trim(analytics.data_carriers.state)) like '%north%carolina%' then 'NC'
		when lower(trim(analytics.data_carriers.state)) like '%north%dakota%' then 'ND'
		when lower(trim(analytics.data_carriers.state)) like '%ohio%' then 'OH'
		when lower(trim(analytics.data_carriers.state)) like '%oklahoma%' then 'OK'
		when lower(trim(analytics.data_carriers.state)) like '%oregon%' then 'OR'
		when lower(trim(analytics.data_carriers.state)) like '%pennsylvania%' then 'PA'
		when lower(trim(analytics.data_carriers.state)) like '%rhode%island%' then 'RI'
		when lower(trim(analytics.data_carriers.state)) like '%south%carolina%' then 'SC'
		when lower(trim(analytics.data_carriers.state)) like '%south%dakota%' then 'SD'
		when lower(trim(analytics.data_carriers.state)) like '%tennessee%' then 'TN'
		when lower(trim(analytics.data_carriers.state)) like '%texas%' then 'TX'
		when lower(trim(analytics.data_carriers.state)) like '%utah%' then 'UT'
		when lower(trim(analytics.data_carriers.state)) like '%vermont%' then 'VT'
		when lower(trim(analytics.data_carriers.state)) like '%virginia%' then 'VA'
		when lower(trim(analytics.data_carriers.state)) like '%washington%' then 'WA'
		when lower(trim(analytics.data_carriers.state)) like '%west%virginia%' then 'WV'
		when lower(trim(analytics.data_carriers.state)) like '%wisconsin%' then 'WI'
		when lower(trim(analytics.data_carriers.state)) like '%wyoming%' then 'WY'
		when upper(trim(analytics.data_carriers.state)) in ('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC') then upper(trim(analytics.data_carriers.state))
		else null end as State__c
	, analytics.data_carriers.zip_code as Zip_Code__c
	, analytics.data_carriers.zcta as ZCTA__c
	, analytics.data_carriers.cbsa_name as CBSA__c 
	, analytics.data_carriers.major_region as Major_Region__c
	--	certification
	, analytics.data_carriers.dot as DOT__c
	, analytics.data_carriers.mc as MC__c
	, analytics.data_carriers.ia as IA__c
	, analytics.data_carriers.phmsa as PHMSA__c
	, analytics.data_carriers.stripe_account_id as Stripe_Account_ID__c
	, analytics.data_carriers.stripe_default_payout_account as Stripe_Default_Payout_Account__c
	, analytics.data_carriers.email_notifications_enabled as Email_Notifications_Enabled__c
	, case when analytics.data_carriers.has_app_installed = true then true else false end as Has_App_Installed__c
	, analytics.data_carriers.has_insurance as Has_Insurance__c
	, analytics.data_carriers.has_vehicles_without_photos as Has_Vehicles_Without_Photos__c
	, analytics.data_carriers.is_fleets_approved as Is_Fleets_Approved__c
	, analytics.data_carriers.is_vetted as Is_Vetted__c
	, analytics.data_carriers.motor_cargo_coverage as Motor_Cargo_Coverage__c
	, analytics.data_carriers.occupational_accident_coverage as Occupational_Accident_Coverage__c
	, analytics.data_carriers.workers_compensation_coverage as Workers_Compensation_Coverage__c
	, analytics.data_carriers.stripe_payouts_enabled as Stripe_Payouts_Enabled__c
from analytics.data_carriers
left join analytics.data_drivers	
	on analytics.data_carriers.email_address = analytics.data_drivers.email_address
	and analytics.data_carriers.external_id = analytics.data_drivers.carrier_external_id
	and analytics.data_drivers.carrier_permissions = 'owner'
where 1=1
  and name is not null
  and name <> ''
  and name <> ' '