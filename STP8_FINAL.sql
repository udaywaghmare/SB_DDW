ct tmp_work_db.comm_nps_sb_trans_final as sas_pulse_stg.comm_nps_sb_trans_final with no data;

delete from tmp_work_db.comm_nps_sb_trans_final all;

insert into tmp_work_db.comm_nps_sb_trans_final
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
        sls_rep_bdge_nbr,
        sls_rep_email_addr_id,
        sys_itm_cls_nm,
        sys_flg,
        snp_flg,
        svcs_flg,
        sls_chnl_cd,
        tot_revn_disc_amt,
        acct_id,
        acct_co_nm,
        cust_bu_id,
        cust_nbr,
        cust_lcl_chnl_cd,
        cust_co_nm,
        iso_ctry_cd_2,
        iso_lang_cd,
        frst_nm,
        last_nm,
        full_nm,
        email_addr_id,
        domain_email_addr_id,
        ph_nbr,
        recency
    )
    select *
    from tmp_work_db.comm_nps_sb_trans_tmp7
    qualify rank() over (partition by email_addr_id order by so_dt desc, so_bu_id, so_nbr desc) = 1;


update inc 
from sas_pulse_stg.comm_nps_sb_trans_final inc , SLS_BASE.ORD_ONLN_OFFLINE_CATG onln_flg 
set sls_chnl_cd = case when onln_desc is not null then 'ONLINE' else 'OFFLINE' end
WHERE inc.so_nbr = onln_flg.ord_nbr and inc.so_bu_id = onln_flg.src_bu_id
;
SELECT
iso_ctry_cd_2,
sls_chnl_cd,
'new' AS typ,
COUNT(*)
FROM tmp_work_db.comm_nps_sb_trans_final
GROUP BY 1,2
UNION
SELECT
iso_ctry_cd_2,
sls_chnl_cd,
'old' AS typ,
COUNT(*)
FROM sas_pulse_stg.comm_nps_sb_trans_final
GROUP BY 1,2
;

delete from sas_pulse_stg.comm_nps_sb_trans_final all ; 
insert into sas_pulse_stg.comm_nps_sb_trans_final sel * from tmp_work_db.comm_nps_sb_trans_final; 

