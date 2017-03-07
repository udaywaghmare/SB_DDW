--DELETE FROM sas_pulse_stg.comm_nps_smx_audience ALL;
INSERT INTO sas_pulse_stg.comm_nps_smx_audience    
    SELECT
        cntct.frst_nm AS CONTACT_FIRST_NAME,
        cntct.last_nm AS CONTACT_LAST_NAME,
        cntct.full_nm AS CONTACT_NAME,
        NULL AS CONTACT_ID,
        cntct.email_addr_id AS EMAIL_ADDRESS,
        NULL AS GROUP_ACCOUNT_ID,
        NULL AS ACCOUNT_ID,
        cntct.cust_nbr AS CUSTOMER_NUMBER,
        cntct.cust_co_nm AS COMPANY_NAME,
        NULL AS COMPANY_NAME_LOCAL,
        NULL AS GROUP_COMPANY_NAME,
        cntct.iso_lang_cd AS ISO_LANGUAGE_CODE,
        cntct.iso_ctry_cd_2 AS ISO_COUNTRY_CODE,
        cl.SFDC_ctry_nm AS COUNTRY_NAME, 
        'SB' AS GLOBAL_SEGMENT,
        NULL AS SUB_SEGMENT,
        cl.rgn_desc AS REGION,
        'DDW' AS SOURCE_SYSTEM,
        NULL AS PERSON_GREETING,
        NULL AS CONTACT_JOB_TITLE,
        NULL AS DECISION_MAKING_ROLE,
        NULL AS CONTACT_TYPE,
        NULL AS ACCT_EXEC_NAME,
        NULL AS ACCT_EXEC_EMAIL,
        NULL AS SALES_REP_NAME,
        sls_rep_email_addr_id AS SALES_REP_EMAIL,
        cntct.sls_rep_bdge_nbr AS SALES_REP_BADGE_NUMBER,
        CASE WHEN cntct.sls_chnl_cd = 'OFFLINE' THEN 'PHONE-INBOUND' ELSE 'WEBSITE' END AS SALES_SEGMENT,
        NULL AS SALES_VERTICAL,
        NULL AS ISM_ASSIGNED_NAME,
        NULL AS RSM_EMAIL,
        NULL AS RSM_ASSIGNED_NAME,
        NULL AS ISM_EMAIL,
        NULL AS ORSD_NAME,
        NULL AS ORSD_EMAIL,
        NULL AS GAM_EMAIL_ADDR_ID,
        NULL AS TAM_EMAIL_ADDR_ID,
        NULL AS SERVICES_ACCT_TEAM_EMAIL, /* add correct value here */
            
        NULL AS RAD_CODE,
         NULL AS RAD_NUMBER,
        'SMB Trans Pulse' AS SURVEY_TYPE,
        NULL AS SIGNATORY_NAME_1,
        NULL AS SIGNATORY_TITLE_1,
		
		
        NULL AS SIGNATORY_NAME_2,
        NULL AS SIGNATORY_TITLE_2,
        cl.survey_close_date AS SURVEY_CLOSE_DATE,
        cl.first_reminder_date AS FIRST_REMINDER_DATE,
        cl.second_reminder_date AS SECOND_REMINDER_DATE,
        cl.wave_desc AS WAVE_DESCRIPTION,
        NULL AS WEIGHTING_GROUP,

        NULL AS ACCOUNT_RANKING,
        NULL AS GMM_NUMBER,
        cl.ce_email_addr_id AS CE_EMAIL,
        'Dell' AS SURVEY_SOURCE,

        CAST(cntct.so_dt AS VARCHAR(10)) AS ORDER_DATE,    
        cntct.recency AS RECENCY,
        cntct.so_nbr AS ORDER_NUMBER,
        cntct.cust_bu_id AS BUSINESS_UNIT_ID,
        'Local Channel: ' || cntct.cust_lcl_chnl_cd AS SPARE_FIELD1,
        CASE WHEN sc.cust_nbr IS NOT NULL THEN 'CSMB' ELSE NULL END AS SPARE_FIELD2,
        NULL AS SPARE_FIELD3,
		'NO' AS SERVICES_FLAG,
		
        NULL AS ACCOUNT_EXEC_BADGE_NUMBER,
        NULL AS Det_Task_Owner_Badge_Number,
        NULL AS Pas_Task_Owner_Badge_Number,
        NULL AS Pro_Task_Owner_Badge_Number,

        NULL AS SFDC_TASK_LINK,
        NULL AS BOUNCED_ALERT_FLAG,
        NULL AS NO_RESPONSE_ALERT_FLAG,
        cntct.ph_nbr AS CONTACT_TEL_NO,
        NULL AS HIGHEST_ACCOUNT_ID,
        NULL AS HIGHEST_ACCOUNT_NAME,
        NULL AS GTM_NUMBER,
        NULL AS PRIMARY_CONTACT,
        NULL  AS CONTACT_STATUS,
        NULL AS SFDC_ACCOUNT_OWNER_EMAIL,
        CAST(cntct.cust_bu_id AS VARCHAR(10)) || cntct.so_nbr AS GLOBAL_CE_CONTACT_ID
    FROM sas_pulse_stg.comm_nps_sb_trans_smpl cntct
    INNER JOIN (SELECT * FROM sas_pulse_stg.comm_nps_cntct_ctry_lkup WHERE second_rpt_grp IN ( 'SB', 'US SB')) cl 
        ON cntct.iso_ctry_cd_2 = cl.iso_ctry_2_cd
    LEFT OUTER JOIN ( SELECT so_bu_id, cust_nbr FROM sas_pulse_stg.comm_nps_sb_trans_svcs sc GROUP BY 1,2) sc ON cntct.so_bu_id = sc.so_bu_id AND cntct.cust_nbr = sc.cust_nbr
	WHERE CL.TOP_RPT_GRP = 'EMEA'
;