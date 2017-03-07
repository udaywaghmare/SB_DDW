SELECT
SO_DT(FORMAT 'MMM')(CHAR(3)) AS MTH,
CUST_BU_ID,
sls_chnl_cd,
pgh.ctry_desc,
'new' AS WAVE,
COUNT(*),
COUNT(email_addr_id) CNTCT_CNT,
SUM(TOT_REVN_DISC_AMT) AS TOT_REV
FROM tmp_work_db.comm_nps_sb_trans_tmp7 T
INNER JOIN corp_drm_pkg.phys_geo_hier pgh     ON    T.CUST_BU_ID = pgh.BU_ID
GROUP BY 1,2,3,4,5
UNION
SELECT
SO_DT(FORMAT 'MMM')(CHAR(3)) AS MTH,
CUST_BU_ID,
sls_chnl_cd,
pgh.ctry_desc,
'old' AS WAVE,
COUNT(*),
COUNT(email_addr_id) CNTCT_CNT,
SUM(TOT_REVN_DISC_AMT) AS TOT_REV
FROM sas_pulse_stg.comm_nps_sb_trans_tmp7 T
INNER JOIN corp_drm_pkg.phys_geo_hier pgh     ON    T.CUST_BU_ID = pgh.BU_ID
GROUP BY 1,2,3,4,5
;

SELECT
ch.bu_id,
ch.lcl_chnl_code,
ch.type_code,
ch.cust_type_code,
ch.seg_code, 
ch.vert_code,
pgh.ctry_desc,
count(*)
FROM SAS_PULSE_STG.comm_nps_sb_trans_tmp2 T
INNER JOIN corp_drm_pkg.phys_geo_hier pgh     ON    T.CUST_BU_ID = pgh.BU_ID
INNER JOIN corp_drm_pkg.chnl_hier ch ON T.CUST_BU_ID = ch.bu_id AND T.cust_lcl_chnl_cd = ch.lcl_chnl_code
LEFT JOIN tmp_work_db.comm_nps_sb_trans_tmp2 O ON T.CNSLD_SRC_TXN_ID=O.CNSLD_SRC_TXN_ID AND T.CNSLD_SRC_TXN_BU_ID=O.CNSLD_SRC_TXN_BU_ID
--WHERE CTRY_DESC ='United States' AND T.evnt_dt > '2015-06-01'
WHERE O.CUST_BU_ID is null -- and t.tot_revn_disc_amt > 1000
GROUP BY 1,2,3,4,5,6,7


SELECT
t.BILT_CUST_NBR
FROM SAS_PULSE_STG.comm_nps_sb_trans_tmp1 T
LEFT JOIN tmp_work_db.comm_nps_sb_trans_tmp1 O ON T.CNSLD_SRC_TXN_ID=O.CNSLD_SRC_TXN_ID AND T.CNSLD_SRC_TXN_BU_ID=O.CNSLD_SRC_TXN_BU_ID
where t.cust_bu_id =3696 and o.cust_bu_id is null and T.evnt_dt > '2015-06-01'




SEL ch.cust_type_code, ch.vert_code
FROM sls_pkg.so_dtl_fact odf  
INNER JOIN sls_pkg.so_hdr_fact shf ON odf.ORD_NBR = shf.ORD_NBR AND odf.SRC_BU_ID = shf.SRC_BU_ID
INNER JOIN corp_drm_pkg.chnl_hier ch ON shf.ref_bu_id = ch.bu_id AND shf.ref_lcl_chnl_cd = ch.lcl_chnl_code
WHERE     shf.ord_type_cd = 'I'
    AND odf.ord_inv_ind = 'A'
    AND shf.ord_dspsn_stat_cd = 'CLOSED'
    AND odf.ord_dt BETWEEN  '2015-03-28' AND  '2016-03-25' 
	AND shf.sldt_cust_nbr IN 
	('108070913','108519401','109169576','109011967','108949431','108540008'	)

	
('108070913','108519401','109169576','109011967','108949431','108540008'	)