with active_discounts as (
	select 
		user_id
		, array_agg(concat(discount_templates.name, ' (', discount_templates.external_id, ') EXP:', discounts.ends_at::date)) as discount
	from discounts
	left join discount_templates
		on discounts.discount_template_id = discount_templates.id
	where 1=1
		and discount_templates.deleted_at is null
		and discounts.deleted_at is null
		and discounts.ends_at <= current_date
	group by 1
	)
	, signup_discounts as (
	    select 
	      promo_code 
	      , discounts.created_at as discount_created_at
	      , discounts.enabled_at as discount_enabled_at
	      , discounts.updated_at as discount_updated_at
	      , u.external_id as user_external_id
	      , originating.email_address as discount_applied_by
	      , discounts.*
	    from discount_templates
	    left join discounts on discounts.discount_template_id = discount_templates.id
	    join users u on u.id = discounts.user_id
	    left join users originating on discounts.originating_user_id = originating.id
	    where 1=1
	      and promo_code is not null
	      and u.signup_completed_at is not null
	    order by u.signup_completed_at desc
	  )
	, ranked_invites as (
		select
		    u.email_address
		    , uu.email_address as team_account_inviter_email_address
		    , concat(uu.first_name, ' ', uu.last_name) as Inviter_Name__c
		    , to_varchar(convert_timezone('UTC', ami.created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Invited_At__c
		    , to_varchar(convert_timezone('UTC', ami.expires_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Invite_Expired_At__c
		    , to_varchar(convert_timezone('UTC', ami.accepted_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Invite_Accepted_At__c
		    , to_varchar(convert_timezone('UTC', ami.declined_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Invite_Declined_At__c
		    , row_number() over (
		      partition by u.email_address
		      order by ami.accepted_at nulls last, ami.expires_at nulls last) as rn
            , case 
		        when (
		                datediff('second', convert_timezone('UTC', u.signup_completed_at), convert_timezone('UTC', ami.accepted_at))/60 between -1 and 1 
		                and utm_source = 'direct'
		                and lower(trim(analytics.curri_employee_information.department)) in ('account management', 'customer operations', 'customer success')) then 'accounts_invite'
		        when (
		                datediff('second', convert_timezone('UTC', u.signup_completed_at), convert_timezone('UTC', ami.accepted_at))/60 between -1 and 1 
		                and utm_source = 'direct'
		                and lower(trim(analytics.curri_employee_information.department)) in ('enterprise', 'executive')) then 'enterprise_invite'
		        when (
		                datediff('second', convert_timezone('UTC', u.signup_completed_at), convert_timezone('UTC', ami.accepted_at))/60 between -1 and 1 
		                and utm_source = 'direct'
		                and lower(trim(analytics.curri_employee_information.department)) in ('sales')) then 'sales_invite'
		        when (
		                datediff('second', convert_timezone('UTC', u.signup_completed_at), convert_timezone('UTC', ami.accepted_at))/60 between -1 and 1 
		                and utm_source = 'direct'
		                and lower(trim(analytics.curri_employee_information.department)) in ('marketing')) then 'marketing_invite'
		        when (
		                datediff('second', convert_timezone('UTC', u.signup_completed_at), convert_timezone('UTC', ami.accepted_at))/60 between -1 and 1 
		                and utm_source = 'direct'
		                and analytics.curri_employee_information.work_email is not null) then 'curri_invite'
		        when (
		                datediff('second', convert_timezone('UTC', u.signup_completed_at), convert_timezone('UTC', ami.accepted_at))/60 between -1 and 1 
		                and utm_source = 'direct') then 'team_invite'
		        else utm_source end as Signup_UTM_Source__c
            , user_campaign_attributions.utm_medium as Signup_UTM_Medium__c
		    , user_campaign_attributions.utm_campaign as Signup_UTM_Campaign__c
		    , user_campaign_attributions.utm_term as Signup_UTM_Term__c
		    , user_campaign_attributions.utm_content as Signup_UTM_Content__c
			, case when (lower(trim(promo_code)) = lower(trim(utm_campaign)) or discount_applied_by = u.email_address or datediff('second', date_trunc('day', u.signup_completed_at), date_trunc('day', discount_created_at))/60/60/24 between -1 and 1) then promo_code else null end as Signup_Promo_Code__c
	  from account_membership_invitations as ami
	  left join users as u
	    on ami.invitee_id = u.id
	  left join users as uu
	    on ami.originating_user_id = uu.id
	  left join accounts
	    on ami.account_id = accounts.id
      left join user_campaign_attributions
		on u.id = user_campaign_attributions.user_id
	  left join analytics.curri_employee_information
	  	on uu.email_address = analytics.curri_employee_information.work_email
	  left join signup_discounts
	  	on signup_discounts.user_external_id = u.external_id
	)
	, team_account_invite_information as (
		select
		  *
		from ranked_invites
		where rn = 1
	)
	, sfdc_contacts as (
		SELECT distinct
			email
			, analytics.zip_to_cbsa.cbsa_name as CBSA__c
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
		from salesforce.contact
		left join analytics.serviceability_v4
			on salesforce.contact.zip_code__c = analytics.serviceability_v4.zip_code
		left join analytics.zip_to_cbsa
			on analytics.serviceability_v4.zip_code = analytics.zip_to_cbsa.zip_code
		where salesforce.contact.isdeleted != true
			and salesforce.contact.zip_code__c is not null
	)
, final as (
select 
		--	account basics
	lower(trim(coalesce(case
		when analytics.data_signups.email_address like '%:%@%' then split_part(analytics.data_signups.email_address, ':', 2)
		when analytics.data_signups.email_address like '%@%.%' then analytics.data_signups.email_address
		when lower(analytics.data_signups.email_address) = 'order: dunnedwards:@eliteextra.com' then 'dunnedwards@eliteextra.com'
		else null end, sfdc_contacts.email))) as Email
	, case 
		when analytics.data_signups.email_address like '%:%@%' then left(split_part(split_part(analytics.data_signups.email_address, ':', 2), '@', 1), 40)
		when (coalesce(users.first_name, split_part(analytics.data_signups.user_name, ' ', 1)) is null or coalesce(users.first_name, split_part(analytics.data_signups.user_name, ' ', 1)) = '')
		then left(split_part(analytics.data_signups.email_address, '@', 1),40) 
		when trim(coalesce(users.first_name, analytics.data_signups.user_name)) is null 
			or trim(coalesce(users.first_name, analytics.data_signups.user_name)) = '' then left(split_part(analytics.data_signups.email_address, '@', 1),40)
		when lower(analytics.data_signups.email_address) = 'order: dunnedwards:@eliteextra.com' then 'Dunnedwards'
		else left(initcap(coalesce(users.first_name, split_part(analytics.data_signups.user_name, ' ', 1))),40) end as FirstName
	, case 
		when analytics.data_signups.email_address like '%:%@%' 
			then left(split_part(analytics.data_signups.email_address, ':', 2), 40)
		when (coalesce(users.last_name, substring(analytics.data_signups.user_name, position(' ' in analytics.data_signups.user_name) + 1 )) is null or coalesce(users.last_name, substring(analytics.data_signups.user_name, position(' ' in analytics.data_signups.user_name) + 1 )) = '')
			then left(split_part(analytics.data_signups.email_address, '@', 2),40) 
		when trim(coalesce(users.last_name, analytics.data_signups.user_name)) is null 
			or trim(coalesce(users.last_name, analytics.data_signups.user_name)) = '' 
			or trim(coalesce(users.last_name, analytics.data_signups.user_name)) = ' ' 
			then left(split_part(analytics.data_signups.email_address, '@', 2),40) 
		when lower(analytics.data_signups.email_address) = 'order: dunnedwards:@eliteextra.com' 
			then 'eliteextra.com'
		else left(initcap(coalesce(users.last_name, substring(analytics.data_signups.user_name, position(' ' in analytics.data_signups.user_name) + 1 ))), 40) end as LastName
	, analytics.data_signups.user_external_id as User_External_ID__c
	, analytics.data_signups.account_external_id as Account_External_ID__c
	, analytics.data_signups.books_with_api as Books_with_API__c
	, analytics.data_signups.total_revenue/100 as Total_Revenue__c
	, analytics.data_signups.hotshot_revenue/100 as Hotshot_Revenue__c
	, analytics.data_signups.route_revenue/100 as Route_Revenue__c
	, analytics.data_signups.order_count as Order_Count__c
	, analytics.data_signups.completed_routes as Completed_Routes__c
	, analytics.data_signups.completed_hotshots as Completed_Hotshots__c
--	, wallet_balance
	, array_to_string(active_discounts.discount,';') as Discounts__c
	, Signup_UTM_Source__c
	, Signup_UTM_Medium__c
	, Signup_UTM_Campaign__c
	, Signup_UTM_Term__c
	, Signup_UTM_Content__c
	, Signup_Promo_Code__c
--	 stats
	, to_varchar(convert_timezone('UTC', analytics.data_signups.banned_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Banned_At__c
	, to_varchar(convert_timezone('UTC', analytics.data_signups.first_hotshot_delivered_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as First_Hotshot_Delivered_At__c
	, to_varchar(convert_timezone('UTC', analytics.data_signups.first_route_delivered_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as First_Route_Delivered_At__c
	, to_varchar(convert_timezone('UTC', analytics.data_signups.first_order_booked_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as First_Order_Booked_At__c
	, to_varchar(convert_timezone('UTC', analytics.data_signups.first_ordered_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as First_Ordered_At__c
	, to_varchar(convert_timezone('UTC', analytics.data_signups.last_hotshot_delivered_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Hotshot_Delivered_At__c
	, to_varchar(convert_timezone('UTC', analytics.data_signups.last_route_delivered_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Route_Delivered_At__c
	, to_varchar(convert_timezone('UTC', analytics.data_signups.last_incomplete_order_booked_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Incomplete_Order_Booked_At__c
	, to_varchar(convert_timezone('UTC', analytics.data_signups.last_order_booked_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Order_Booked_At__c
	, to_varchar(convert_timezone('UTC', analytics.data_signups.last_ordered_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Ordered_At__c
	, to_varchar(convert_timezone('UTC', analytics.data_signups.signup_completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Signup_Completed_At__c
	, to_varchar(convert_timezone('UTC', analytics.data_signups.last_login_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as Last_Login_At__c
    , Invite_Accepted_At__c
	, Invite_Declined_At__c
	, Invite_Expired_At__c
	, Invited_At__c
	, Inviter_Name__c
	, team_account_inviter_email_address
--	geolocation
	, account_address_line_1 as Address_Line_1__c
	, account_address_line_2 as Address_Line_2__c
	, left(account_city, 40) as City__c
	, case 
		when lower(trim(account_state)) like '%alabama%' then 'AL'
		when lower(trim(account_state)) like '%alaska%' then 'AK'
		when lower(trim(account_state)) like '%arizona%' then 'AZ'
		when lower(trim(account_state)) like '%arkansas%' then 'AR'
		when lower(trim(account_state)) like '%california%' then 'CA'
		when lower(trim(account_state)) like '%colorado%' then 'CO'
		when lower(trim(account_state)) like '%connecticut%' then 'CT'
		when lower(trim(account_state)) like '%delaware%' then 'DE'
		when lower(trim(account_state)) like '%district%of%columbia%' then 'DC'
		when lower(trim(account_state)) like '%florida%' then 'FL'
		when lower(trim(account_state)) like '%georgia%' then 'GA'
		when lower(trim(account_state)) like '%hawaii%' then 'HI'
		when lower(trim(account_state)) like '%idaho%' then 'ID'
		when lower(trim(account_state)) like '%illinois%' then 'IL'
		when lower(trim(account_state)) like '%indiana%' then 'IN'
		when lower(trim(account_state)) like '%iowa%' then 'IA'
		when lower(trim(account_state)) like '%kansas%' then 'KS'
		when lower(trim(account_state)) like '%kentucky%' then 'KY'
		when lower(trim(account_state)) like '%louisiana%' then 'LA'
		when lower(trim(account_state)) like '%maine%' then 'ME'
		when lower(trim(account_state)) like '%maryland%' then 'MD'
		when lower(trim(account_state)) like '%massachusetts%' then 'MA'
		when lower(trim(account_state)) like '%michigan%' then 'MI'
		when lower(trim(account_state)) like '%minnesota%' then 'MN'
		when lower(trim(account_state)) like '%mississippi%' then 'MS'
		when lower(trim(account_state)) like '%missouri%' then 'MO'
		when lower(trim(account_state)) like '%montana%' then 'MT'
		when lower(trim(account_state)) like '%nebraska%' then 'NE'
		when lower(trim(account_state)) like '%nevada%' then 'NV'
		when lower(trim(account_state)) like '%new%hampshire%' then 'NH'
		when lower(trim(account_state)) like '%new%jersey%' then 'NJ'
		when lower(trim(account_state)) like '%new%mexico%' then 'NM'
		when lower(trim(account_state)) like '%new%york%' then 'NY'
		when lower(trim(account_state)) like '%north%carolina%' then 'NC'
		when lower(trim(account_state)) like '%north%dakota%' then 'ND'
		when lower(trim(account_state)) like '%ohio%' then 'OH'
		when lower(trim(account_state)) like '%oklahoma%' then 'OK'
		when lower(trim(account_state)) like '%oregon%' then 'OR'
		when lower(trim(account_state)) like '%pennsylvania%' then 'PA'
		when lower(trim(account_state)) like '%rhode%island%' then 'RI'
		when lower(trim(account_state)) like '%south%carolina%' then 'SC'
		when lower(trim(account_state)) like '%south%dakota%' then 'SD'
		when lower(trim(account_state)) like '%tennessee%' then 'TN'
		when lower(trim(account_state)) like '%texas%' then 'TX'
		when lower(trim(account_state)) like '%utah%' then 'UT'
		when lower(trim(account_state)) like '%vermont%' then 'VT'
		when lower(trim(account_state)) like '%virginia%' then 'VA'
		when lower(trim(account_state)) like '%washington%' then 'WA'
		when lower(trim(account_state)) like '%west%virginia%' then 'WV'
		when lower(trim(account_state)) like '%wisconsin%' then 'WI'
		when lower(trim(account_state)) like '%wyoming%' then 'WY'
		when upper(trim(account_state)) in ('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC') then upper(trim(account_state))
		else null end as State__c
	, account_postal_code as Zip_Code__c
	, coalesce(account_cbsa_name, sfdc_contacts.CBSA__c) as CBSA__c
	, account_zcta as ZCTA__c
	, round(account_latitude,3) as Latitude__c
	, round(account_longitude,3) as Longitude__c
--	serviceability
	, coalesce(round(analytics.serviceability_v4.box_truck_score,2), sfdc_contacts.Box_Trucks__c) as Box_Trucks__c
	, coalesce(round(analytics.serviceability_v4.car_suv_score,2), sfdc_contacts.Cars_SUVs__c) as Cars_SUVs__c
	, coalesce(round(analytics.serviceability_v4.cargo_sprinter_score,2), sfdc_contacts.Cargos_Sprinters__c) as Cargos_Sprinters__c
	, coalesce(round(analytics.serviceability_v4.opendeck_score,2), sfdc_contacts.Open_Decks__c) as Open_Decks__c
	, coalesce(round(analytics.serviceability_v4.overall_score,2), sfdc_contacts.Serviceability_Score__c) as Serviceability_Score__c
	, coalesce(round(analytics.serviceability_v4.tractor_score,2), sfdc_contacts.Tractor_Trailers__c) as Tractor_Trailers__c
	, coalesce(round(analytics.serviceability_v4.truck_score,2), sfdc_contacts.Trucks__c) as Trucks__c
	, coalesce(analytics.serviceability_v4.box_truck_grade, sfdc_contacts.Box_Trucks_Grade__c) as Box_Trucks_Grade__c
	, coalesce(analytics.serviceability_v4.car_suv_grade, sfdc_contacts.Cars_SUVs_Grade__c) as Cars_SUVs_Grade__c
	, coalesce(analytics.serviceability_v4.cargo_sprinter_grade, sfdc_contacts.Cargos_Sprinters_Grade__c) as Cargos_Sprinters_Grade__c
	, coalesce(analytics.serviceability_v4.opendeck_grade, sfdc_contacts.Open_Decks_Grade__c) as Open_Decks_Grade__c
	, coalesce(analytics.serviceability_v4.overall_grade, sfdc_contacts.Serviceability_Grade__c) as Serviceability_Grade__c
	, coalesce(analytics.serviceability_v4.tractor_grade, sfdc_contacts.Tractor_Trailers_Grade__c) as Tractor_Trailers_Grade__c
	, coalesce(analytics.serviceability_v4.truck_grade, sfdc_contacts.Trucks_Grade__c) as Trucks_Grade__c
	, true as Is_Customer__c
	, case when user_tags.tag_id = '20376' then 1 else 0 end as vip__c
	, case when analytics.data_signups.order_count = 0 then 1 else 0 end as baby_booker__c
	, 'User' as contact_record_type
from analytics.data_signups
left join analytics.serviceability_v4
	on analytics.data_signups.account_postal_code = analytics.serviceability_v4.zip_code
left join active_discounts
	on analytics.data_signups.user_id = active_discounts.user_id
left join users
	on users.id = analytics.data_signups.user_id
left join team_account_invite_information
	on analytics.data_signups.email_address = team_account_invite_information.email_address
left join user_tags
	on users.id = user_tags.user_id 
	and tag_id = '20376'
left join sfdc_contacts
	on analytics.data_signups.email_address = sfdc_contacts.email
where 1=1
	and analytics.data_signups.email_address not like '%@ferguson.com_%'
	and analytics.data_signups.email_address not in ('info@otmfreight', 'info@wdwarrenllc', 'faizanahmedhere218@gmail%2ecom', 'virgilray32@promysselogistics', 'elohim@transportatiollc1')
	and users.is_deleted is null
)
select *
from final
qualify row_number() over (partition by email order by Order_Count__c desc nulls last) = 1
order by Email