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
        cl.COUNTRY AS COUNTRY_NAME, 
        'SB' AS GLOBAL_SEGMENT,
        NULL AS SUB_SEGMENT,
        cl.REGION AS REGION,
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
        NULL AS SURVEY_CLOSE_DATE,
        NULL AS FIRST_REMINDER_DATE,
        NULL AS SECOND_REMINDER_DATE,
        NULL AS WAVE_DESCRIPTION,
        NULL AS WEIGHTING_GROUP,
        NULL AS ACCOUNT_RANKING,
        NULL AS GMM_NUMBER,
        NULL AS CE_EMAIL,
        'Dell' AS SURVEY_SOURCE,
        CAST(cntct.so_dt AS VARCHAR(10)) AS ORDER_DATE,    
        cntct.recency AS RECENCY,
        cntct.so_nbr AS ORDER_NUMBER,
        cntct.cust_bu_id AS BUSINESS_UNIT_ID,
        'Local Channel: ' || cntct.cust_lcl_chnl_cd AS SPARE_FIELD1,
        NULL AS SPARE_FIELD2,
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
INNER JOIN (SELECT REGION, COUNTRY, ISO_CTRY_CD FROM sas_pulse_stg.comm_nps_acct_geo_map group by 1,2,3) CL ON cntct.iso_ctry_cd_2 = cl.iso_ctry_cd
--	WHERE CL.TOP_RPT_GRP = 'EMEA'
;

--FY17H2M10
update sas_pulse_stg.comm_nps_smx_audience
set signatory_nm_1 = 'Phil Bryant',
signatory_ttl_1 = 'Vice President & General Manager Consumer & Small Business'
--,ce_email_addr_id = 'Luke_Latham@Dell.com'
where iso_ctry_2_cd in ('US')
;
--FY17H2M10
update sas_pulse_stg.comm_nps_smx_audience
set signatory_nm_1 = 'Kevin Peesker',
signatory_ttl_1 = 'Vice President of Sales and President of Dell Canada'
--,ce_email_addr_id = 'Heather_strong@dell.com'
where iso_ctry_2_cd='CA'
;
--FY17H2M10
update sas_pulse_stg.comm_nps_smx_audience
set signatory_nm_1 = 'Amit Midha',
signatory_ttl_1 = 'President, Dell Asia Pacific and Japan (APJ)',
ce_email_addr_id = 'Darshan_Rai@dell.com'
where iso_ctry_2_cd in ('IN')
;
--FY17H2M10
update sas_pulse_stg.comm_nps_smx_audience
set signatory_nm_1 = 'Amit Midha',
signatory_ttl_1 = 'President, Dell Asia Pacific and Japan (APJ)',
ce_email_addr_id = 'Lolo_Cai@dell.com'
where iso_ctry_2_cd in ('CN')
;
--FY17H2M10
update sas_pulse_stg.comm_nps_smx_audience
set signatory_nm_1 = 'Amit Midha',
signatory_ttl_1 = 'President, Dell Asia Pacific and Japan (APJ)',
ce_email_addr_id = 'Tetsuya_saito@dell.com'
where iso_ctry_2_cd in ('JP')
;
--FY17H2M10
update sas_pulse_stg.comm_nps_smx_audience
set signatory_nm_1 = 'Amit Midha',
signatory_ttl_1 = 'President, Dell Asia Pacific and Japan (APJ)',
ce_email_addr_id = 'Anna_Tan@dell.com'
where iso_ctry_2_cd in ('AU','NZ')
;
--FY17H2M10
update sas_pulse_stg.comm_nps_smx_audience
set signatory_nm_1 = 'Stefan Alariksson',
signatory_ttl_1 = 'General Manager, Dell Sweden'
where iso_ctry_2_cd in ('SE')
;
/*
--FY17H2M10
update sas_pulse_stg.comm_nps_smx_audience
set signatory_nm_1 = 'Amit Midha',
signatory_ttl_1 = 'President, Dell Asia Pacific and Japan (APJ)'
ce_email_addr_id = ''
where iso_ctry_2_cd in ('AU')
;
-- FY17H2M10
update sas_pulse_stg.comm_nps_smx_audience
set signatory_nm_1 = 'Anwar Dahab',
signatory_ttl_1 = 'VP & GM Dell France'
where iso_ctry_2_cd='FR'
;
-- FY17H2M10
update sas_pulse_stg.comm_nps_smx_audience
set signatory_nm_1 = 'Doris Albiez',
signatory_ttl_1 = 'VP & GM Dell Deutschland'
where iso_ctry_2_cd='DE'
;
*/
-- FY17H2M10
update sas_pulse_stg.comm_nps_smx_audience
set signatory_nm_1 = 'Anwar Dahab',
signatory_ttl_1 = 'VP & GM Dell France'
where iso_ctry_2_cd='FR'
;
-- FY17H2M10
update sas_pulse_stg.comm_nps_smx_audience
set signatory_nm_1 = 'Doris Albiez',
signatory_ttl_1 = 'VP & GM Dell Deutschland'
where iso_ctry_2_cd='DE'
;
-- FY17H2M10
update sas_pulse_stg.comm_nps_smx_audience
set signatory_nm_1 = 'Jamil Nathoo',
signatory_ttl_1 = 'UK Consumer and Small Medium Business General Manager',
ce_email_addr_id = 'Chitra_Rajan@dell.com'
where iso_ctry_2_cd='GB'
;
-- FY17H2M10
update sas_pulse_stg.comm_nps_smx_audience
set signatory_nm_1 = 'Diego Majdalani',
signatory_ttl_1 = 'Presidente Dell América Latina',
ce_email_addr_id = 'laura_sica@dell.com'
where iso_ctry_2_cd in ('BR','MX')
;

select max(adnc_seltn_id) from
(
select max(adnc_seltn_id) as adnc_seltn_id from  SAS_PULSE_STG.b2b_adnc_seltn_ids union
select max(adnc_seltn_id) as adnc_seltn_id from  SAS_PULSE_STG.b2c_adnc_seltn_ids
) xc

insert into SAS_PULSE_STG.b2b_adnc_seltn_ids
select 
	(ROW_NUMBER() OVER (ORDER BY gce_contact_id, 0) + 811869984),
	 gce_contact_id
from sas_pulse_stg.comm_nps_smx_audience

;

update SAS_PULSE_STG.b2b_adnc_seltn_ids  
set gce_contact_id = trim(trailing '.' from trim(both from gce_contact_id))
;

UPDATE sbddw
FROM sas_pulse_stg.comm_nps_smx_audience sbddw, (SEL COUNTRY, REGION,iso_ctry_cd FROM sas_pulse_stg.comm_nps_acct_geo_map GROUP BY 1,2,3) geo 
SET 
ctry_nm=geo.country,
rgn_desc= geo.region
WHERE sbddw.iso_ctry_2_cd= geo.iso_ctry_cd
;

UPDATE  AUD
FROM sas_pulse_stg.comm_nps_smx_audience AUD, sas_pulse_stg.comm_nps_prsn_greeting_df pg
SET 
prsn_greeting_txt = pg.prsn_greeting_txt
WHERE aud.rgn_desc = pg.rgn_desc 
    AND aud.iso_lang_cd = pg.iso_lang_cd
    AND aud.prsn_greeting_txt IS NULL
;


DEL FROM SAS_PULSE_STG.CSB_NPS_AUDIENCE_SB_DDW ALL;
INSERT INTO SAS_PULSE_STG.CSB_NPS_AUDIENCE_SB_DDW
SEL 
SE.ADNC_SELTN_ID	 AS 	EVNT_ID	,
SE.ADNC_SELTN_ID	 AS 	PULSE_ADNC_SELTN_ID	,
SBDDW.FULL_NM	 AS 	CONTACT_NAME	,
SBDDW.EMAIL_ADDR_ID	 AS 	EMAIL_ADDRESS	,
td_sysfnlib.OREPLACE(td_sysfnlib.OREPLACE(SBDDW.PH_NBR,'09'XC,' '),'0D0A'XC,' ')	 AS 	PHONE_NUMBER	,
lnk_lkup.enum_lang	 AS 	'LANGUAGE'n	,
lnk_lkup.Survey_Close_DT 	 AS 	SURVEY_CLOSE_DATE	,
CASE WHEN direct_flg = 'x' THEN lnk_lkup.supp_link  ELSE NULL END  	 AS 	SUPPORT_LINK	,
CASE WHEN direct_flg = 'x' THEN lnk_lkup.prv_link  ELSE NULL END  	 AS 	PRIVACY_LINK	,
TRIM(TRAILING '.' FROM CAST(SBDDW.SO_BU_ID AS VARCHAR(20))) || '-' || TRIM(SUBSTRING(SBDDW.SPARE_FIELD1 FROM POSITION(':' IN SBDDW.SPARE_FIELD1) + 2))	 AS 	ACCOUNT_ID	,
'B2B NPS'	 AS 	SURVEY	,
'FY18 H1'	 AS 	WAVE	,
'No'	 AS 	OVERSAMPLE_FLAG	,
SBDDW.SO_RECENCY	 AS 	PURCHASE_RECENCY	,
TRIM(SBDDW.SO_BU_ID)|| '-' ||TRIM(SBDDW.SO_NBR)	 AS 	ORDER_NUMBER	,
NULL	 AS 	SVC_TAG	,
CAST(CAST(TRIM(SUBSTR(SBDDW.SO_DT,1,10))	 AS  DATE FORMAT'YYYY-MM-DD')	 AS VARCHAR(10)) AS  ORDER_DATE	,
TRIM(SBDDW.SO_BU_ID)|| '-' ||TRIM(SBDDW.CUST_NBR)	 AS 	CUSTOMER_NUMBER	,

COALESCE( RSP_ST.rsp_status, 'Not-Invited') AS REPEAT_RESPONDERS,
'Direct' AS ACCOUNT_TYPE,
NULL	 AS 	CONTACT_ID	,

'Unknown/Blank'	 AS 	DECISION_MAKING_ROLE	,
PRSN_GREETING_TXT	 AS 	SALUTATION	,
SIGNATORY_NM_1	 AS 	SIGNATORY_NAME_1	,
SIGNATORY_TTL_1	 AS 	SIGNATORY_TITLE_1	,
NULL	 AS 	SIGNATORY_NAME_2	,
NULL	 AS 	SIGNATORY_TITLE_2	,
'Business Partner'	 AS 	LETTER_CUST_TYPE	,
'No'	 AS 	SERVICES_CONTACT_FLAG	,
'No'	 AS 	SOFTWARE_CONTACT_FLAG	,
NULL	 AS 	SFDC_TASK_LINK	,
NULL	 AS 	SR_EMAIL	,
NULL	 AS 	AE_EMAIL	,
NULL	 AS 	ISM_EMAIL	,
NULL	 AS 	RSM_EMAIL	,
NULL	 AS 	GAM_EMAIL	,
NULL	 AS 	TAM_EMAIL	,
NULL	 AS 	CAM_EMAIL	,
NULL	 AS 	PDM_EMAIL	,
NULL	 AS 	GCP_PM_EMAIL	,
NULL	 AS 	OEM_SE_EMAIL	,
NULL	 AS 	OEM_PM_EMAIL	,
NULL	 AS 	SERVICES_PPOC_EMAIL	,
NULL	 AS 	SERVICES_DELEGATE_EMAIL	,
NULL	 AS 	SERVICES_OPS_LEAD_EMAIL	,
NULL	 AS 	SERVICES_MAILBOX_EMAIL	,
CE_EMAIL_ADDR_ID	 AS 	CX_EMAIL	,
NULL	 AS 	DET_TASK_OWNER_ID	,
NULL	 AS 	PAS_TASK_OWNER_ID	,
NULL	 AS 	PRO_TASK_OWNER_ID	,
NULL	 AS 	SUB_ACCOUNT_ID	,
NULL	 AS 	SUB_ACCOUNT_NAME	,
NULL	 AS 	JOB_TITLE	,
CASE WHEN SBDDW.SLS_SEG_DESC = 'WEBSITE' THEN 'Online' ELSE 'Offline' end 	 AS 	TRANS_SALES_CHANNEL	,
NULL	 AS 	PRODUCT_LVL1	,
NULL	 AS 	PRODUCT_LVL2	,
NULL	 AS 	PRODUCT_LVL3	,
NULL	 AS 	PRODUCT_LVL4	,
NULL	 AS 	PRODUCT_LVL5	
FROM  SAS_PULSE_STG.b2b_adnc_seltn_ids  se 
INNER JOIN SAS_PULSE_STG.comm_nps_smx_audience  sbddw ON se.gce_contact_id=TRIM(TRAILING '.' FROM CAST(sbddw.gce_contact_id  AS VARCHAR(20)))
LEFT OUTER JOIN sas_pulse_stg.comm_nps_link_lkup lnk_lkup ON sbddw.ctry_nm = lnk_lkup .CTRY_NM AND sbddw.iso_lang_cd=lnk_lkup .ISO_LANG_CD
LEFT OUTER JOIN SAS_PULSE_STG.COMM_NPS_PREV_RESP_STATUS rsp_st ON sbddw.email_addr_id = rsp_st.email_address
--WHERE iso_ctry_2_cd IN ('US' ,'CA')
--WHERE  se.EVNT_TYPE_CD IN ( 'COMMNPS') AND se.EVNT_SUBTYPE_CD IN ( 'FY16H2PULSE')
;

/*
UPDATE  AUD
FROM tmp_work_db.FY16H2_NPS_AUDIENCE_2 AUD, ce_base.SURVEY_ADNC_seltn_log asl
SET PULSE_ADNC_SELTN_ID=adnc_seltn_id
WHERE AUD.evnt_id=asl.evnt_id
;
*/
INSERT INTO tmp_work_db.b2b_acct_medallia_3 
--SB DDW 
--DEL FROM   tmp_work_db.b2b_acct_fy17h2_medallia_2 ALL;
--INSERT INTO  tmp_work_db.b2b_acct_fy17h2_medallia_2
SELECT
	sbddw.account_id AS ACCOUNT_ID,	
	ch.lcl_chnl_desc AS ACCOUNT_NAME,	
	'Transactional' AS CUSTOMER_REL_TYPE,	
	'Direct' AS ACCOUNT_TYPE,	
	NULL AS ACCOUNT_RANKING,	
	'CSB12' AS SALES_SEG_LVL1, 
	pgh.ctry_desc  AS SALES_SEG_LVL2,
	'Direct'  AS SALES_SEG_LVL3,
	'SB' AS SALES_SEG_LVL4,
	NULL AS SALES_SEG_LVL5,	
	'[' || sbddw.account_id || '] ' || ch.lcl_chnl_desc AS SALES_SEG_LVL6,
	'SB' AS GLOBAL_SEGMENT,	
	wt.weightid AS WEIGHTING_ID,
	CASE WHEN pgh.ctry_desc IN ('United States','Canada') THEN 'NA'	
		WHEN pgh.rgn_desc = 'Americas' THEN 'LA'
		ELSE pgh.rgn_desc end AS REGION,
	CASE WHEN pgh.ctry_desc = 'Australia' THEN 'ANZ' ELSE pgh.ctry_desc end AS SUB_REGION,	
	pgh.ctry_desc AS COUNTRY,	
	NULL AS RAD_CODE,	
	NULL AS GLOBAL_ACCOUNT_ID,	
	NULL AS GLOBAL_ACCOUNT_NAME,	
	NULL AS GMM,	
	NULL AS PARTNER_REL_TYPE,	
	NULL AS PARTNER_TYPE,	
	NULL AS PARTNER_TIER,	
	NULL AS PARTNER_CERT,	
	NULL AS SERVICES_INDUSTRY	,
	'Yes' AS Act_flg
--SEL COUNT(*)
FROM  SAS_PULSE_STG.CSB_NPS_AUDIENCE_SB_DDW sbddw
INNER JOIN sas_pulse_stg.comm_nps_sb_trans_smpl smpl ON sbddw.email_address = smpl.EMAIL_ADDR_ID 
LEFT OUTER JOIN corp_drm_pkg.phys_geo_hier pgh ON CAST(CASE WHEN POSITION('-' IN  sbddw.account_id) > 0 THEN SUBSTRING(sbddw.account_id,1,POSITION('-' IN  sbddw.account_id)-1) end AS INTEGER) = pgh.bu_id		
LEFT OUTER JOIN corp_drm_pkg.chnl_hier ch ON pgh.bu_id = ch.bu_id 
	AND CASE WHEN POSITION('-' IN  sbddw.account_id) > 0 THEN SUBSTRING(sbddw.account_id,POSITION('-' IN  sbddw.account_id)+1,CHARACTER_LENGTH(sbddw.account_id)) end = ch.lcl_chnl_code		
LEFT OUTER JOIN SAS_GBL_CEANALYTICS_STAG.rsc_FY17_rev_SSL4_BU wt ON  '[' || sbddw.account_id || '] ' || ch.lcl_chnl_desc =WT.SSL6
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
;
;

