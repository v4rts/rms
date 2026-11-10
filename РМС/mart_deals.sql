DROP TABLE IF EXISTS mart_deals;

CREATE TABLE mart_deals AS

WITH deal_hist AS (
    SELECT
        owner_id,
        max(created_time) FILTER (WHERE stage_id = 'WON' or stage_id = 'C1:NEW') AS last_won,
        min(created_time) FILTER (WHERE stage_id = 'WON' or stage_id = 'C1:NEW')
            AS first_contracting_won_at,
        max(created_time) FILTER (WHERE stage_id = 'C1:WON') AS last_1cwon,
        min(created_time) FILTER (WHERE stage_id = 'C1:WON')
            AS first_verification_won_at,
        min(created_time) FILTER (WHERE stage_id = 'NEW')
            AS first_contracting_entered_at,
        min(created_time) FILTER (WHERE stage_id = 'C1:NEW')
            AS first_verification_entered_at,
        max(created_time) FILTER (WHERE stage_id = 'LOSE') AS last_lose,
        min(created_time) FILTER (WHERE stage_id = 'LOSE') AS first_lose,
        max(created_time) FILTER (WHERE stage_id = 'C1:LOSE') AS last_1close,
        min(created_time) FILTER (WHERE stage_id = 'C1:LOSE') AS first_1close,
        extract(EPOCH FROM (
            max(created_time) FILTER (
                WHERE (stage_id = 'WON' or stage_id = 'C1:NEW') AND category_id = 0
            )
            - max(created_time) FILTER (
                WHERE stage_id = 'NEW' AND category_id = 0
            )
        )) / 86400 AS successfull_contracting_days
    FROM public.b_ods_crm_deal_stage_history
    GROUP BY owner_id
    ORDER BY owner_id
),

lose_bounds AS (
    --техническая таблица
    -- Находим время первого и последнего отказа для каждого owner_id
    SELECT
        owner_id,
        min(created_time) AS first_lose_time,
        max(created_time) AS last_lose_time
    FROM public.b_ods_crm_deal_stage_history
    WHERE stage_id = 'LOSE'
    GROUP BY owner_id
),

ranked_stages AS (
    --техническая таблица
    -- Размечаем строки для первого и последнего отказа отдельно
    SELECT
        t.*,
        -- Нумерация для стадии перед первым отказом
        row_number() OVER (
            PARTITION BY t.owner_id
            ORDER BY
                CASE
                    WHEN t.created_time < fl.first_lose_time THEN t.created_time
                END DESC NULLS LAST
        ) AS rn_first,

        -- Нумерация для стадии перед последним отказом
        row_number() OVER (
            PARTITION BY t.owner_id
            ORDER BY
                CASE
                    WHEN t.created_time < fl.last_lose_time THEN t.created_time
                END DESC NULLS LAST
        ) AS rn_last
    FROM public.b_ods_crm_deal_stage_history AS t
    INNER JOIN lose_bounds AS fl ON t.owner_id = fl.owner_id
),

stages_bf_lose AS (
    SELECT
        owner_id,
        max(CASE WHEN rn_first = 1 THEN stage_id END)
            AS stage_before_first_lose,
        max(CASE WHEN rn_first = 1 THEN created_time END)
            AS time_before_first_lose,
        max(CASE WHEN rn_last = 1 THEN stage_id END) AS stage_before_last_lose,
        max(CASE WHEN rn_last = 1 THEN created_time END)
            AS time_before_last_lose
    FROM ranked_stages
    WHERE rn_first = 1 OR rn_last = 1
    GROUP BY owner_id
),

lose_bounds_1c AS (
    -- время первого и последнего отказа для каждого owner_id
    SELECT
        owner_id,
        min(created_time) AS first_lose_time_1c,
        max(created_time) AS last_lose_time_1c
    FROM public.b_ods_crm_deal_stage_history
    WHERE stage_id = 'C1:LOSE'
    GROUP BY owner_id
),

ranked_stages_1c AS (
    -- Размечаем строки для первого и последнего отказа
    SELECT
        t.*,
        -- Нумерация для стадии перед первым отказом
        row_number() OVER (
            PARTITION BY t.owner_id
            ORDER BY
                CASE
                    WHEN
                        t.created_time < fl.first_lose_time_1c
                        THEN t.created_time
                END DESC NULLS LAST
        ) AS rn_first_1c,

        -- Нумерация для стадии перед последним отказом
        row_number() OVER (
            PARTITION BY t.owner_id
            ORDER BY
                CASE
                    WHEN
                        t.created_time < fl.last_lose_time_1c
                        THEN t.created_time
                END DESC NULLS LAST
        ) AS rn_last_1c
    FROM public.b_ods_crm_deal_stage_history AS t
    INNER JOIN lose_bounds_1c AS fl ON t.owner_id = fl.owner_id
),

stages_bf_lose_1c AS (
    SELECT
        owner_id,
        max(CASE WHEN rn_first_1c = 1 THEN stage_id END)
            AS stage_before_first_lose_1c,
        max(CASE WHEN rn_first_1c = 1 THEN created_time END)
            AS time_before_first_lose_1c,
        max(CASE WHEN rn_last_1c = 1 THEN stage_id END)
            AS stage_before_last_lose_1c,
        max(CASE WHEN rn_last_1c = 1 THEN created_time END)
            AS time_before_last_lose_1c
    FROM ranked_stages_1c
    WHERE rn_first_1c = 1 OR rn_last_1c = 1
    GROUP BY owner_id
),

leads_new AS (
    SELECT
        id,
        max(created_time) AS first_lead_new
    FROM public.b_ods_crm_lead_status_history
    WHERE status_id = 'NEW'
    GROUP BY id
),

history_with_prev AS (
    SELECT
        h.owner_id::text AS owner_id,
        h.category_id::text AS category_id,
        h.stage_id::text AS stage_id,
        h.created_time,
        lag(h.stage_id) OVER (
            PARTITION BY h.owner_id, h.category_id
            ORDER BY h.created_time ASC, h.id ASC
        ) AS prev_stage_id
    FROM public.b_ods_crm_deal_stage_history AS h
),

last_real_entry AS (
    SELECT
        owner_id,
        category_id,
        stage_id,
        max(created_time) AS current_stage_entered_at
    FROM history_with_prev
    WHERE
        prev_stage_id IS NULL
        OR prev_stage_id <> stage_id
    GROUP BY owner_id, category_id, stage_id
),

first_won_attrs AS (
    SELECT DISTINCT
        deal_id,
        first_value(deal_opportunity)
            OVER (
                PARTITION BY deal_id ORDER BY version_id, deal_source_event_ts
            )
    FROM public.hist_entity_attributes
    WHERE deal_stage_id = 'WON' or deal_stage_id = 'C1:NEW'
),


first_1cwon_attrs AS (
    SELECT DISTINCT
        deal_id,
        first_value(deal_opportunity)
            OVER (
                PARTITION BY deal_id ORDER BY version_id, deal_source_event_ts
            )
    FROM public.hist_entity_attributes
    WHERE deal_stage_id = 'C1:WON'
),


st AS (
    SELECT *
    FROM b_ods_crm_status
    WHERE entity_id = 'DEAL_STAGE' OR entity_id = 'DEAL_STAGE_1'
),


product_row_agg AS (
    SELECT DISTINCT ON (owner_id)
        owner_id::text  AS owner_id,
        product_id::text AS product_id
    FROM public.b_ods_crm_product_row_deals
    ORDER BY
        owner_id,
        id
)

SELECT
    d.id::int AS deal_id,
    d.opportunity::decimal AS current_amount,
    d.currency_id,
    d.title AS deal_title,
    d.category_id::text AS current_category_id,
    d.stage_id AS current_stage_id,
    st.name AS stage_name,
    d.stage_semantic_id AS current_stage_semantic_id,
    d.company_id,
    c.title AS company_name,
    c.company_type,
    d.contact_id::text AS contact_id,
    d.lead_id::text AS lead_id,
    d.closedate,
    d.assigned_by_id::text AS assigned_by_id,
    d.date_create::date AS created_at,
    d.date_modify::date AS date_modify,
    d.source_id::text AS source_id,

    cur.amount::decimal AS currency_denominator,
    d.probability::decimal AS probability,
    d.source_description,
    d.last_activity_time,

    ud.uf_user_svkia,
    ud.uf_lose_verification_description,
    ud.uf_lose_reason_verification,
    ud.uf_confirmation_rms_role_by_do,
    ud.uf_contract_amount_rms,
    ud.uf_vat_amount,
    ud.uf_contract_amount_vat,
    --    ud.uf_crm_amount                           AS uf_crm_amount,
    ud.uf_date_dogovor,
    ud.uf_dogovor_number,
    ud.uf_expected_date_signing_contract,
    ud.uf_date_response_kp,
    ud.uf_date_sent_kp,
    ud.uf_probability,
    ud.uf_has_been_agreed,
    ud.uf_county_of_supply AS supply_country_ids,
    ud.uf_lose_reason_notes_deal,
    ud.uf_crm_lose_reason_deal,
    ufe.value AS lose_reason_name,
    ud.uf_status AS current_business_status_id,
    ud.uf_status_date AS current_business_status_date,
    extract(EPOCH FROM (now() - (nullif(ud.uf_status_date, ''))::timestamp))
    / 86400 AS deal_business_status_changed_days_ago,
    ufe_s.value AS current_business_status,
    dim_bsn.norm AS deal_business_status_norm,
    CASE
        WHEN
            extract(EPOCH FROM (now() - lre.current_stage_entered_at)) / 86400
            <= dim_bsn.norm
            THEN TRUE
        WHEN
            extract(EPOCH FROM (now() - lre.current_stage_entered_at)) / 86400
            > dim_bsn.norm
            THEN FALSE
    END AS deal_business_status_in_days_norm,

    ud.uf_head_office_employee,
    concat(u_ho.last_name, ' ', u_ho.name, ' ', u_ho.second_name)
        AS head_office_employee_name,
    ud.uf_product_stream AS product_stream_ids,
    aa.name AS product_stream_name,
    ud.uf_subsidiary_manager,
    concat(u_sm.last_name, ' ', u_sm.name, ' ', u_sm.second_name)
        AS subsidiary_manager_name,
    ud.uf_manager_rc_co,
    concat(u_rc.last_name, ' ', u_rc.name, ' ', u_rc.second_name)
        AS manager_rc_co_name,
    concat(u_aid.last_name, ' ', u_aid.name, ' ', u_aid.second_name)
        AS assigned_by_id_name,
    dep_aid_user_dep.name AS assignee_department_ids,
    dep_aid_user_reg.name AS assignee_regional_center_ids,
    ud.uf_lead_country_ofice,
    dep.name AS department,
    ud.uf_type_business,
    ud.uf_type_contracting,
    ud.uf_category,
    ib_serv_sec.name AS catalogue_element_category,
    --products_agg.catalogue_element_category as catalogue_element_category,
    sc.name AS catalogue_element,
    ud.uf_brief_description,
    ud.uf_integrator_do AS division_ids,
    di.title AS integrator_do,
    ctry.title AS country_of_supply,
    CASE
        WHEN st.name IN ('Active Negotiation', 'Awaiting Decision') THEN 0.5
        WHEN st.name IN ('Key terms negotiated', 'Contract Finalization') THEN 1
    END AS forecast_scenario,

    deal_hist.last_won,
    deal_hist.first_contracting_won_at::date AS first_contracting_won_at,
    deal_hist.last_1cwon,
    deal_hist.first_verification_won_at,
    deal_hist.first_contracting_entered_at,
    deal_hist.first_verification_entered_at,
    deal_hist.last_lose,
    deal_hist.first_lose,
    deal_hist.last_1close,
    deal_hist.first_1close,
    fwa.first_value::decimal AS signed_amount,
    f1cwa.first_value::decimal AS signed_ver_amount,
    deal_hist.successfull_contracting_days,
    extract(
        EPOCH FROM (
            deal_hist.first_contracting_won_at - leads_new.first_lead_new
        )
    ) / 86400 AS full_cycle_days,
    stages_bf_lose.stage_before_first_lose,
    stages_bf_lose.time_before_first_lose,
    stages_bf_lose.stage_before_last_lose,
    stages_bf_lose.time_before_last_lose,
    stages_bf_lose_1c.stage_before_first_lose_1c,
    stages_bf_lose_1c.time_before_first_lose_1c,
    stages_bf_lose_1c.stage_before_last_lose_1c,
    stages_bf_lose_1c.time_before_last_lose_1c,
    --    last_stage_entry_days.last_stage_entry_days AS last_stage_entry_days,
    --    last_stage_entry_days.last_real_entry_time as current_stage_entered_at,
    lre.current_stage_entered_at,
    extract(EPOCH FROM (now() - lre.current_stage_entered_at))
    / 86400 AS current_stage_age_days,
    --    extract(
    --    	EPOCH from (NOW() - last_stage_entry_days.last_real_entry_time
    --    	)) / 86400                             as current_stage_age_days,
    now()::date AS data_as_of
FROM b_ods_crm_deal AS d
LEFT JOIN b_ods_uts_crm_deal AS ud ON ud.id::text = d.id::text
LEFT JOIN st ON st.status_id::text = d.stage_id::text
LEFT JOIN b_ods_crm_company AS c ON c.id::text = d.company_id::text
LEFT JOIN
    b_ods_catalog_currency AS cur
    ON cur.currency::text = d.currency_id::text
LEFT JOIN b_ods_user AS u_ho ON u_ho.id::text = ud.uf_head_office_employee::text
LEFT JOIN b_ods_user AS u_sm ON u_sm.id::text = ud.uf_subsidiary_manager::text
LEFT JOIN b_ods_user AS u_rc ON u_rc.id::text = ud.uf_manager_rc_co::text
LEFT JOIN b_ods_user AS u_aid ON u_aid.id::text = d.assigned_by_id::text
LEFT JOIN b_ods_uts_user AS u_uts_aid ON u_uts_aid.id::text = u_aid.id::text

LEFT JOIN
    b_ods_iblock_activity_area AS aa
    ON aa.id::text = ud.uf_product_stream::text
LEFT JOIN
    b_ods_iblock_department AS dep
    ON dep.id::text = ud.uf_lead_country_ofice::text
LEFT JOIN
    b_ods_iblock_department AS dep_aid_user_dep
    ON dep_aid_user_dep.id::text = u_uts_aid.uf_department::text
LEFT JOIN
    b_ods_iblock_department AS dep_aid_user_reg
    ON dep_aid_user_reg.id::text = u_uts_aid.uf_usr_regional_center::text
LEFT JOIN
    b_ods_crm_dynamic_items_1090 AS di
    ON di.id::text = ud.uf_integrator_do::text
LEFT JOIN
    b_ods_crm_dynamic_items_1102 AS ctry
    ON ctry.id::text = ud.uf_county_of_supply::text
LEFT JOIN
    b_ods_user_field_enum AS ufe
    ON ufe.id::text = ud.uf_crm_lose_reason_deal::text
LEFT JOIN b_ods_user_field_enum AS ufe_s ON ufe_s.id::text = ud.uf_status::text
LEFT JOIN
    b_ods_iblock_service_categories AS sc
    ON sc.id::text = ud.uf_category::text

LEFT JOIN deal_hist ON deal_hist.owner_id::text = d.id::text
LEFT JOIN stages_bf_lose ON stages_bf_lose.owner_id::text = d.id::text
LEFT JOIN stages_bf_lose_1c ON stages_bf_lose_1c.owner_id::text = d.id::text
--LEFT JOIN last_stage_entry_days ON last_stage_entry_days.id::text          = d.id::text
LEFT JOIN leads_new ON leads_new.id::text = d.lead_id::text

LEFT JOIN
    product_row_agg AS p_row_d
    ON p_row_d.owner_id::text = d.id::text
LEFT JOIN
    public.b_ods_iblock_service_categories AS ib_serv_cat
    ON ib_serv_cat.id::text = p_row_d.product_id::text
LEFT JOIN
    public.b_ods_catalog_service_sections AS ib_serv_sec
    ON ib_serv_sec.id::text = ib_serv_cat.iblock_section_id::text

LEFT JOIN last_real_entry AS lre
    ON
        lre.owner_id = d.id::text
        AND lre.category_id = d.category_id::text
        AND lre.stage_id = d.stage_id::text

LEFT JOIN first_won_attrs AS fwa ON d.id = fwa.deal_id
LEFT JOIN first_1cwon_attrs AS f1cwa ON d.id = f1cwa.deal_id
LEFT JOIN
    dim_business_status_norm AS dim_bsn
    ON ufe_s.value = dim_bsn.business_status
ORDER BY deal_id
