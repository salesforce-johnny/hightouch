with
	domains as (
		select
			id
			, trim(
				regexp_replace(
					regexp_replace(                         -- 5) turn pipes into comma-space
					  regexp_replace(                       -- 4) remove literal backslashes used to escape dots
						regexp_replace(                     -- 3) remove trailing ")$" (literal )
						  regexp_replace(                   -- 2) remove leading "(.*)("
							regexp_replace(                 -- 1) remove leading caret ^ if present
							  account_regex,
							'^\\^', ''),                    -- 1: remove leading ^
						  '^\\(\\.\\*\\)\\(', ''),          -- 2: remove leading (.*)(
						'\\)\\$', ''),                      -- 3: remove trailing ")$" (literal ) then literal $)
					  '\\\\', ''),                          -- 4: remove backslashes that escape dots
					'\\|', ', '),							-- 5: replace alternation pipes with comma+space
				'@', '')									-- 6: remove @
			) as domain_list
		from accounts
	)
select distinct
--	account basics
	analytics.data_accounts.account_external_id as pk
	, parent.external_id as parent_account_external_id
    , analytics.data_accounts.root_account_external_id as ultimate_parent_account_external_id
	, analytics.data_accounts.account_external_id as Account_External_ID__c
	, analytics.data_accounts.account_type as Account_Type__c
	, left(
		case 
			when analytics.data_accounts.account_name = ' ' or analytics.data_accounts.account_name is null then concat('Unnamed: ', analytics.data_accounts.company)
			else 
				case 
					when analytics.data_accounts.account_type = 'company' or parent.external_id is null then analytics.data_accounts.account_name
					else concat(analytics.data_accounts.company, ' | ', analytics.data_accounts.account_name)
				end
		end, 255) as Name
	, analytics.data_accounts.location_number as Location_Number__c
--	geolocation
	, analytics.data_accounts.location_address_line_1 as Address_Line_1__c
	, analytics.data_accounts.location_address_line_2 as Address_Line_2__c
	, left(analytics.data_accounts.location_city, 40) as City__c
	, case 
		when lower(trim(location_state)) like '%alabama%' then 'AL'
		when lower(trim(location_state)) like '%alaska%' then 'AK'
		when lower(trim(location_state)) like '%arizona%' then 'AZ'
		when lower(trim(location_state)) like '%arkansas%' then 'AR'
		when lower(trim(location_state)) like '%california%' then 'CA'
		when lower(trim(location_state)) like '%colorado%' then 'CO'
		when lower(trim(location_state)) like '%connecticut%' then 'CT'
		when lower(trim(location_state)) like '%delaware%' then 'DE'
		when lower(trim(location_state)) like '%district%of%columbia%' then 'DC'
		when lower(trim(location_state)) like '%florida%' then 'FL'
		when lower(trim(location_state)) like '%georgia%' then 'GA'
		when lower(trim(location_state)) like '%hawaii%' then 'HI'
		when lower(trim(location_state)) like '%idaho%' then 'ID'
		when lower(trim(location_state)) like '%illinois%' then 'IL'
		when lower(trim(location_state)) like '%indiana%' then 'IN'
		when lower(trim(location_state)) like '%iowa%' then 'IA'
		when lower(trim(location_state)) like '%kansas%' then 'KS'
		when lower(trim(location_state)) like '%kentucky%' then 'KY'
		when lower(trim(location_state)) like '%louisiana%' then 'LA'
		when lower(trim(location_state)) like '%maine%' then 'ME'
		when lower(trim(location_state)) like '%maryland%' then 'MD'
		when lower(trim(location_state)) like '%massachusetts%' then 'MA'
		when lower(trim(location_state)) like '%michigan%' then 'MI'
		when lower(trim(location_state)) like '%minnesota%' then 'MN'
		when lower(trim(location_state)) like '%mississippi%' then 'MS'
		when lower(trim(location_state)) like '%missouri%' then 'MO'
		when lower(trim(location_state)) like '%montana%' then 'MT'
		when lower(trim(location_state)) like '%nebraska%' then 'NE'
		when lower(trim(location_state)) like '%nevada%' then 'NV'
		when lower(trim(location_state)) like '%new%hampshire%' then 'NH'
		when lower(trim(location_state)) like '%new%jersey%' then 'NJ'
		when lower(trim(location_state)) like '%new%mexico%' then 'NM'
		when lower(trim(location_state)) like '%new%york%' then 'NY'
		when lower(trim(location_state)) like '%north%carolina%' then 'NC'
		when lower(trim(location_state)) like '%north%dakota%' then 'ND'
		when lower(trim(location_state)) like '%ohio%' then 'OH'
		when lower(trim(location_state)) like '%oklahoma%' then 'OK'
		when lower(trim(location_state)) like '%oregon%' then 'OR'
		when lower(trim(location_state)) like '%pennsylvania%' then 'PA'
		when lower(trim(location_state)) like '%rhode%island%' then 'RI'
		when lower(trim(location_state)) like '%south%carolina%' then 'SC'
		when lower(trim(location_state)) like '%south%dakota%' then 'SD'
		when lower(trim(location_state)) like '%tennessee%' then 'TN'
		when lower(trim(location_state)) like '%texas%' then 'TX'
		when lower(trim(location_state)) like '%utah%' then 'UT'
		when lower(trim(location_state)) like '%vermont%' then 'VT'
		when lower(trim(location_state)) like '%virginia%' then 'VA'
		when lower(trim(location_state)) like '%washington%' then 'WA'
		when lower(trim(location_state)) like '%west%virginia%' then 'WV'
		when lower(trim(location_state)) like '%wisconsin%' then 'WI'
		when lower(trim(location_state)) like '%wyoming%' then 'WY'
      -- check if already 2-letter valid state code
    	when upper(trim(location_state)) in (
    		'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
    		'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
    		'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
    		'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
    		'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY',
    		'DC'
    	) then upper(trim(location_state))
    
    	else NULL
		end as State__c
	, location_zip_code as Zip_Code__c
	, location_cbsa_name as CBSA__c 
	, round(analytics.data_accounts.location_latitude,3) as Latitude__c
	, round(analytics.data_accounts.location_longitude,3) as Longitude__c
--	stats
	, to_varchar(convert_timezone('UTC', analytics.data_accounts.first_signup), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Location_First_Signup__c
	, to_varchar(convert_timezone('UTC', analytics.data_accounts.first_ordered_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Location_First_Ordered_At__c
	, to_varchar(convert_timezone('UTC', analytics.data_accounts.first_order_booked_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Location_First_Order_Booked_At__c
	, to_varchar(convert_timezone('UTC', analytics.data_accounts.last_ordered_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Location_Last_Ordered_At__c
    , to_varchar(convert_timezone('UTC', analytics.data_accounts.last_order_booked_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Location_Last_Order_Booked_At__c
	, analytics.data_accounts.pending_invites as Location_Pending_Invites__c
	, analytics.data_accounts.total_revenue_dollars as Total_Revenue__c
	, analytics.data_accounts.hotshot_revenue_dollars as Total_Hotshot_Revenue__c
	, analytics.data_accounts.route_revenue_dollars as Total_Route_Revenue__c
	, analytics.data_accounts.cms_revenue_dollars as Total_CMS_Revenue__c
	, analytics.data_accounts.total_orders as Total_Orders__c
	, analytics.data_accounts.hotshot_orders as Total_Hotshot_Orders__c
	, analytics.data_accounts.route_orders as Total_Route_Orders__c
	, analytics.data_accounts.cms_orders as Total_CMS_Orders__c
--	serviceability
	, round(analytics.serviceability_v4.box_truck_score,2) as Box_Trucks__c
	, round(analytics.serviceability_v4.car_suv_score,2) as Cars_SUVs__c
	, round(analytics.serviceability_v4.cargo_sprinter_score,2) as Cargos_Sprinters__c
	, round(analytics.serviceability_v4.opendeck_score,2) as Open_Decks__c
	, round(analytics.serviceability_v4.overall_score,2) as Serviceability_Score__c
	, round(analytics.serviceability_v4.tractor_score,2) as Tractor_Trailers__c
	, round(analytics.serviceability_v4.truck_score,2) as Trucks__c
	, analytics.serviceability_v4.box_truck_grade as Box_Trucks_Grade__c
	, analytics.serviceability_v4.car_suv_grade as Cars_SUVs_Grade__c
	, analytics.serviceability_v4.cargo_sprinter_grade as Cargos_Sprinters_Grade__c
	, analytics.serviceability_v4.opendeck_grade as Open_Decks_Grade__c
	, analytics.serviceability_v4.overall_grade as Serviceability_Grade__c
	, analytics.serviceability_v4.tractor_grade as Tractor_Trailers_Grade__c
	, analytics.serviceability_v4.truck_grade as Trucks_Grade__c
-- churn risk
    , analytics.int_account_churn_risk.priority_score as Churn_Priority_Score__c
    , analytics.int_account_churn_risk.REVENUE_LAST_90_DAYS as Revenue_Last_90_Days__c
    , analytics.int_account_churn_risk.REVENUE_PRIOR_90_DAYS as Churn_Revenue_Prior_90_Day_Period__c
    , analytics.int_account_churn_risk.PCT_CHANGE_LAST90_VS_PRIOR90 as Percent_Revenue_Change_Last_90_Prior_90__c
    , analytics.int_account_churn_risk.DELTA_UNIQUE_USERS_LAST30_VS_PRIOR30 as Delta_Unique_Users_Last_30_v_Prior_30__c
	, case 
		when analytics.data_accounts.account_type = 'company' or parent.external_id is null then 'Company'
		else 'Team Account' end as account_record_type
	, losing_account_external_ids as Losing_Account_External_Ids__c 
	, domains.domain_list
from analytics.data_accounts
left join accounts acct
	on analytics.data_accounts.account_id = acct.id
	and (acct.is_deleted != true or acct.is_deleted is null)
left join accounts parent
	on acct.parent_account_id = parent.id
	and (parent.is_deleted != true or parent.is_deleted is null)
left join analytics.serviceability_v4
	on analytics.data_accounts.location_zip_code = analytics.serviceability_v4.zip_code
left join analytics.int_account_churn_risk
    on analytics.data_accounts.account_external_id = analytics.int_account_churn_risk.account_external_id
left join domains
	on domains.id = analytics.data_accounts.account_id
where 1=1
	and (analytics.data_accounts.is_deleted != true or analytics.data_accounts.is_deleted is null)
order by 1