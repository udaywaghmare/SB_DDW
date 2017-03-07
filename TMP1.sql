update inc 
from sas_pulse_stg.comm_nps_sb_trans_final inc , SLS_BASE.ORD_ONLN_OFFLINE_CATG onln_flg 
set sls_chnl_cd = case when onln_desc is not null then 'ONLINE' else 'OFFLINE' end
WHERE inc.so_nbr = onln_flg.ord_nbr and inc.so_bu_id = onln_flg.src_bu_id

ct	tmp_work_db.comm_nps_sb_trans_tmp1	as 	sas_pulse_stg.comm_nps_sb_trans_tmp1	with no data ;
ct	tmp_work_db.comm_nps_sb_trans_tmp2	as 	sas_pulse_stg.comm_nps_sb_trans_tmp2	with no data ;
ct	tmp_work_db.comm_nps_sb_trans_tmp3	as 	sas_pulse_stg.comm_nps_sb_trans_tmp3	with no data ;
ct	tmp_work_db.comm_nps_sb_trans_tmp4	as 	sas_pulse_stg.comm_nps_sb_trans_tmp4	with no data ;
ct	tmp_work_db.comm_nps_sb_trans_tmp5	as 	sas_pulse_stg.comm_nps_sb_trans_tmp5	with no data ;
ct	tmp_work_db.comm_nps_sb_trans_tmp6	as 	sas_pulse_stg.comm_nps_sb_trans_tmp6	with no data ;
ct	tmp_work_db.comm_nps_sb_trans_tmp7	as 	sas_pulse_stg.comm_nps_sb_trans_tmp7	with no data ;


delete from tmp_work_db.comm_nps_sb_trans_tmp1 all;

INSERT INTO tmp_work_db.comm_nps_sb_trans_tmp1

(
    evnt_dt,
    evnt_dts,
    cnsld_src_txn_id,
    cnsld_src_txn_bu_id,
    so_nbr,
    so_bu_id,
    so_lcl_chnl_cd,
    so_dt,
    so_inv_dt,
    qte_nbr,
    qte_bu_id,
    qte_lcl_chnl_cd,
    qte_dt,
    prim_sls_assoc_nbr,
    secnd_sls_assoc_nbr,
    sys_itm_cls_nm,
    sys_flg,
    snp_flg,
    svcs_flg,
    sls_chnl_cd,
    tot_revn_disc_amt,
    ord_dispos_stat_cd,
    sls_type_cd,
    cust_bu_id,
    cust_lcl_chnl_cd,
    bilt_cust_nbr,
    bilt_src_cust_id,
    bilt_addr_seq_id,
    bilt_cntct_id,
    shpto_cust_nbr,
    shpto_src_cust_id,
    shpto_addr_seq_id,
    shpto_cntct_id)
SELECT
    odf.ord_dt AS evnt_dt,    
    odf.ord_dts AS evnt_dts,
    odf.ord_nbr,
    odf.src_bu_id,
    odf.ord_nbr,
    odf.src_bu_id,
    shf.src_lcl_chnl_cd,
    odf.ord_dt,
    odf.inv_dt,
    shf.qte_nbr,
    CASE WHEN shf.qte_nbr IS NOT NULL THEN shf.src_bu_id ELSE NULL END AS qte_bu_id,
    CASE WHEN shf.qte_nbr IS NOT NULL THEN shf.src_lcl_chnl_cd ELSE NULL END AS qte_lcl_chnl_cd,
    CASE WHEN shf.qte_nbr IS NOT NULL THEN shf.ord_dt ELSE NULL END AS qte_dt,
    shf.prim_sls_assoc_nbr,
    shf.secnd_sls_assoc_nbr,
    MAX(odf.sys_cls_cd) AS sys_itm_cls_nm,
    MAX(odf.sys_flg) AS sys_flg,
    MAX(CASE WHEN odf.snp_flg = 'Y' THEN 'Y' ELSE 'N' END) AS snp_flg,
    MAX(odf.svc_flg) AS svcs_flg,
    --CASE WHEN COALESCE(shf.onln_lvl_cd, shf.intrnt_revn_cd,'N') IN ('2','N') THEN 'OFFLINE' ELSE 'ONLINE' END AS sls_chnl_cd,
    case when onln_flg.onln_desc is not null then 'ONLINE' else 'OFFLINE' end AS sls_chnl_cd,
    SUM(odf.revn_disc_txn_amt*odf.revn_usd_rt) AS tot_revn_disc_amt, 
    shf.ord_dspsn_stat_cd AS ord_dispos_stat_cd,
    CASE WHEN odf.sls_type_cd = 'SYSTEM POS' THEN 'SYSTEM' ELSE odf.sls_type_cd END AS sls_type_cd,
    shf.ref_bu_id,
    shf.ref_lcl_chnl_cd,
    shf.sldt_cust_nbr,
    shf.sldt_src_cust_id,
    CASE WHEN shf.sldt_cust_nbr = shf.bilt_cust_nbr THEN shf.bilt_addr_seq_id
        WHEN shf.sldt_cust_nbr = shf.shpto_cust_nbr THEN shf.shpto_addr_seq_id
        ELSE NULL END AS sldto_addr_seq_id,
    shf.sldt_cntct_id,
    shf.shpto_cust_nbr,
    shf.shpto_src_cust_id,
    shf.shpto_addr_seq_id,
    shf.shpto_cntct_id
FROM sls_pkg.so_dtl_fact odf  
INNER JOIN sls_pkg.so_hdr_fact shf ON odf.ORD_NBR = shf.ORD_NBR AND odf.SRC_BU_ID = shf.SRC_BU_ID
INNER JOIN corp_drm_pkg.chnl_hier ch ON shf.ref_bu_id = ch.bu_id AND shf.ref_lcl_chnl_cd = ch.lcl_chnl_code
LEFT OUTER JOIN SLS_BASE.ORD_ONLN_OFFLINE_CATG onln_flg ON odf.ORD_NBR = onln_flg.ord_nbr AND  odf.SRC_BU_ID = onln_flg.src_bu_id
WHERE 
    shf.ord_type_cd = 'I'
    AND odf.ord_inv_ind = 'A'
    AND shf.ord_dspsn_stat_cd = 'CLOSED'
    AND odf.ord_dt BETWEEN '2015-12-05' AND '2016-12-04'
		/*'2015-11-01' AND '2016-11-05'*/
		/*'2015-10-03' AND  '2016-10-01'*/
		/*'2015-09-05' AND  '2016-09-03'*/
		/*'2015-08-07' AND  '2016-08-05'*/
		/*'2015-03-28' AND  '2016-03-25' */
		/*'2014-10-11' AND  '2015-10-09' */
		/*'2014-10-11' AND  '2015-10-09' */
		/*'2015-02-07' AND '2014-02-09'*/
		/*'2013-08-03' AND '2014-08-01'*/
		/*'2012-05-12' and '2013-05-11' -- keep */
        /* FY13Q3 '2011-08-06' AND '2012-08-10' */
        /* FY13Q2 '2011-05-07' AND '2012-05-11' */
        /* Q2 '2010-05-12' and '2011-05-11' */
        /* Q3 '2010-08-14' and '2011-08-12' */
        /* Q4 '2010-11-13' and '2011-11-11' */
        /* Q1 '2011-02-05' AND '2012-02-10' */
    AND odf.sls_type_cd <> '24 HOUR'
    AND odf.tmzn_loc_id IN (1,2,3,4)  -- keep 
    AND (
	(ch.cust_type_code = 'GPSEG'    AND (ch.vert_code IN ('SMBST', 'SMBSO','CSBSO') ))
    OR (ch.lcl_chnl_code in ('1800','29015','LVPSE','SBPSE','SDDE','24542') and ch. bu_id in (1212, 4065))
	)
    GROUP BY 
    odf.ord_dt,    
    odf.ord_dts,
    odf.ord_nbr,
    odf.src_bu_id,
    shf.src_lcl_chnl_cd,
    odf.inv_dt,
    shf.qte_nbr,
    CASE WHEN shf.qte_nbr IS NOT NULL THEN shf.src_bu_id ELSE NULL END,
    CASE WHEN shf.qte_nbr IS NOT NULL THEN shf.src_lcl_chnl_cd ELSE NULL END,
    CASE WHEN shf.qte_nbr IS NOT NULL THEN shf.ord_dt ELSE NULL END,
    shf.prim_sls_assoc_nbr,
    shf.secnd_sls_assoc_nbr,
    --CASE WHEN COALESCE(shf.onln_lvl_cd, shf.intrnt_revn_cd,'N') IN ('2','N') THEN 'OFFLINE' ELSE 'ONLINE' END,
    case when onln_flg.onln_desc is not null then 'ONLINE' else 'OFFLINE' end,
    shf.ord_dspsn_stat_cd,
    CASE WHEN odf.sls_type_cd = 'SYSTEM POS' THEN 'SYSTEM' ELSE odf.sls_type_cd END,
    shf.ref_bu_id,
    shf.ref_lcl_chnl_cd,
    shf.sldt_cust_nbr,
    shf.sldt_src_cust_id,
    CASE WHEN shf.sldt_cust_nbr = shf.bilt_cust_nbr THEN shf.bilt_addr_seq_id
        WHEN shf.sldt_cust_nbr = shf.shpto_cust_nbr THEN shf.shpto_addr_seq_id
        ELSE NULL END,
    shf.sldt_cntct_id,
    shf.shpto_cust_nbr,
    shf.shpto_src_cust_id,
    shf.shpto_addr_seq_id,
    shf.shpto_cntct_id
;


