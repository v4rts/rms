DROP TABLE IF EXISTS mart_stages;

create table mart_stages as
WITH dl as (
select
	d.id as transition_id,
	'DEAL' as entity_type,
	owner_id as entity_id,
	type_id,
	category_id,
	case
		when category_id = 0 then 'Contracting'
		when category_id = 1 then 'Verification'
		else NULL
	end as process_code,
	d.stage_id as stage_id,
	d_st.name as stage_name,
	stage_semantic_id,
	null as status_semantic_id,
	lag(category_id) over (partition by owner_id order by created_time) as previous_category_id,
	lag(d.stage_id) over (partition by owner_id order by created_time) as previous_stage_id,
	row_number() over (partition by owner_id order by created_time) as transition_number,
	created_time as stage_entered_at,
	lead(created_time) over (partition by owner_id order by created_time) as stage_exited_at,
	min(created_time) FILTER (WHERE stage_id = 'C1:NEW')
	    OVER (PARTITION BY owner_id)
	    AS first_deal_ver_entered_at
from public.b_ods_crm_deal_stage_history as d
left join (
		select * from public.b_ods_crm_status where entity_id in ('DEAL_STAGE', 'DEAL_STAGE_1')
	) as d_st on d_st.status_id = d.stage_id

union all

select
	l.id as transition_id,
	'LEAD' as entity_type,
	owner_id as entity_id,
	type_id,
	null as category_id,
	'Qualification' as process_code,
	l.status_id as stage_id,
	l_st.name as stage_name,
	null as stage_semantic_id,
	status_semantic_id,
	null as previous_category_id,
	lag(l.status_id) over (partition by owner_id order by created_time) as previous_stage_id,
	row_number() over (partition by owner_id order by created_time) as transition_number,
	created_time as stage_entered_at,
	lead(created_time) over (partition by owner_id order by created_time) as stage_exited_at,
	null AS first_deal_ver_entered_at
from public.b_ods_crm_lead_status_history as l
left join (
		select * from public.b_ods_crm_status where entity_id = 'STATUS'
	) as l_st on l_st.status_id = l.status_id
),


new_stages AS (
    SELECT
        transition_id,
		entity_type,
        ROW_NUMBER() OVER (
                PARTITION BY
                entity_type,
                entity_id
                ORDER BY stage_entered_at
            ) AS target_rn
    FROM dl
    where (entity_type = 'LEAD' AND stage_id = 'NEW') OR
          (entity_type = 'DEAL' AND stage_id IN ('NEW', 'C1:NEW'))
)



select
	dl.entity_type,
	case
		when dl.entity_type = 'DEAL' then dl.entity_id
		else NULL
	end as deal_id,
	case
		when dl.entity_type = 'LEAD' then dl.entity_id
		else NULL
	end as lead_id,
	dl.entity_id,
	dl.transition_id,
	dl.category_id,
	dl.process_code,
	dl.stage_id,
	dl.stage_name,
	dl.previous_category_id,
	dl.previous_stage_id,
	dl.transition_number,
	dl.stage_entered_at,
	dl.stage_exited_at,
	dl.first_deal_ver_entered_at,
	row_number() over (partition by entity_id, process_code, stage_id order by stage_entered_at) as stage_visit_number,
	extract(EPOCH from (stage_exited_at - stage_entered_at))/86400 as stage_visit_duration_days,
	case
		when dl.stage_exited_at is null then 1
		else 0
	end as is_open_visit,
	CASE
        WHEN new_stages.target_rn = 1 then 1
        ELSE 0
    END AS is_first_process_entry,
    case
    	when process_code = 'Qualification' and stage_id = 'CONVERTED' then 1
    	when process_code = 'Contracting' and stage_id = 'WON' and category_id = 0 then 1
    	when process_code = 'Verification' and stage_id = 'C1:WON' and category_id = 1 then 1
    	else 0
    end as is_successful_completion,
    case
    	when process_code = 'Qualification' and stage_id = 'JUNK' then 1
    	when process_code = 'Contracting' and stage_id = 'LOSE' and category_id = 0 then 1
    	when process_code = 'Verification' and stage_id = 'C1:LOSE' and category_id = 1 then 1
    	else 0
    end as is_refusal_completion,
    (select max(stage_entered_at) as data_as_of from dl) as data_as_of,
    NOW() as loaded_at


from dl
left join new_stages on new_stages.transition_id::text = dl.transition_id::text and new_stages.entity_type = dl.entity_type
order by dl.entity_type, dl.transition_id
