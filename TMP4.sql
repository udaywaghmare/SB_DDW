/*
Manual fix for duplicates 
CREATE MULTISET TABLE tmp_work_db.comm_nps_sb_trans_tmp4_x ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      CNSLD_SRC_TXN_ID VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      CNSLD_SRC_TXN_BU_ID INTEGER NOT NULL,
      ISO_CTRY_CD_2 CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS ('CA','CN','DE','FR','GB','JP','MX','US'),
      ISO_LANG_CD CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      CUST_CO_NM VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
      FRST_NM VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      LAST_NM VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      FULL_NM VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      EMAIL_ADDR_ID VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      WORK_PHONE VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC)
PRIMARY INDEX UPI_COMM_NPS_SB_TRANS_TMP4_x ( CNSLD_SRC_TXN_ID ,CNSLD_SRC_TXN_BU_ID );

select  CNSLD_SRC_TXN_ID ,
CNSLD_SRC_TXN_BU_ID, count(*)
from   tmp_work_db.comm_nps_sb_trans_tmp4_x
group by 1,2 
having count(CNSLD_SRC_TXN_BU_ID) > 1
;
select * from  tmp_work_db.comm_nps_sb_trans_tmp4_x where CNSLD_SRC_TXN_ID in 
(
select  CNSLD_SRC_TXN_ID 
from   tmp_work_db.comm_nps_sb_trans_tmp4_x
group by 1 
having count(CNSLD_SRC_TXN_BU_ID) > 1
)
and full_nm is null
;

INSERT INTO tmp_work_db.comm_nps_sb_trans_tmp4
    (
        cnsld_src_txn_id,
        cnsld_src_txn_bu_id,
        iso_ctry_cd_2,
        iso_lang_cd,
        cust_co_nm,
        frst_nm,
        last_nm,
        full_nm,
        email_addr_id,
        work_phone
    )
SEL
CNSLD_SRC_TXN_ID			,
CNSLD_SRC_TXN_BU_ID			,
MAX(	ISO_CTRY_CD_2	)	,
MAX(	ISO_LANG_CD	)	,
MAX(	CUST_CO_NM	)	,
MAX(	FRST_NM	)	,
MAX(	LAST_NM	)	,
MAX(	FULL_NM	)	,
MAX(	EMAIL_ADDR_ID	)	,
MAX(	WORK_PHONE	)	
FROM tmp_work_db.comm_nps_sb_trans_tmp4_x
GROUP BY 1,2

*/
delete from tmp_work_db.comm_nps_sb_trans_tmp4 all;

insert into tmp_work_db.comm_nps_sb_trans_tmp4
    (
        cnsld_src_txn_id,
        cnsld_src_txn_bu_id,
        iso_ctry_cd_2,
        iso_lang_cd,
        cust_co_nm,
        frst_nm,
        last_nm,
        full_nm,
        email_addr_id,
        work_phone
    )

    /*AMER*/
    select
        tmp3.cnsld_src_txn_id,
        tmp3.cnsld_src_txn_bu_id,
        cast(pgh.iso_ctry_code_2 as char(2)) as iso_ctry_cd_2,
        cast(case when tmp3.so_bu_id = 707 and coalesce(pa.state_prov_code,pas.state_prov_code) = 'QC' then 'FR' else null end as char(2)) as iso_lang_cd,
        cast(trim(coalesce((case when pa.company_name is null then full_nm else pa.company_name end), 
            (case when pas.company_name is null then full_nm else pas.company_name end))) as varchar(250)) as cust_co_nm,
        cast(trim(coalesce(c.first_name, sc.first_name)) as varchar(100)) as frst_nm,
        cast(trim(coalesce(c.last_name, sc.last_name)) as varchar(100)) as last_nm,
        cast(trim(coalesce((case when (c.first_name is not null or c.last_name is not null) 
                then (trim(coalesce(c.first_name, '')) || ' ' || trim(coalesce(c.last_name, ''))) else null end),
                (case when (sc.first_name is not null or sc.last_name is not null) 
                then (trim(coalesce(sc.first_name, '')) || ' ' || trim(coalesce(sc.last_name, ''))) else null end))) as varchar(200)) as full_nm,
        max(cast(trim(coalesce(case when ctm.media_type_code in ('EML', 'E') then ctm.media_value else null end, case when ctms.media_type_code in ('EML', 'E') then ctms.media_value else null end)) as varchar(254))) as email_addr_id,
        max(cast(trim(coalesce(case when ctm.media_type_code = 'PHW' then ctm.media_value else null end, case when ctms.media_type_code = 'PHW' then ctms.media_value else null end)) as varchar(254))) as work_phone
    from tmp_work_db.comm_nps_sb_trans_tmp3 tmp3
    left outer join finance.contact c 
        on tmp3.bilt_cust_nbr = c.customer_num 
        and tmp3.so_bu_id = c.business_unit_id
        and tmp3.bilt_addr_seq_id = c.address_seq_num
        and c.address_type_code = 'B'
    left outer join finance.contact_media ctm
        on c.customer_num = ctm.customer_num
        and c.business_unit_id = ctm.business_unit_id
        and c.contact_id = ctm.contact_id
        and ctm.media_type_code in ('PHW','EML','E')
    left outer join finance.postal_address pa
        on tmp3.bilt_cust_nbr = pa.customer_num 
        and tmp3.bilt_addr_seq_id = pa.address_seq_num 
        and tmp3.so_bu_id = pa.business_unit_id 
        and pa.address_type_code = 'B'
    left outer join finance.contact sc 
        on tmp3.bilt_cust_nbr = sc.customer_num 
        and tmp3.shpto_cust_nbr = sc.customer_num
        and tmp3.so_bu_id = sc.business_unit_id
        and tmp3.shpto_addr_seq_id = sc.address_seq_num
        and sc.address_type_code = 'S'
    left outer join finance.contact_media ctms
        on sc.customer_num = ctms.customer_num
        and sc.business_unit_id = ctms.business_unit_id
        and sc.contact_id = ctms.contact_id
        and ctms.media_type_code in ('PHW','EML','E')
    left outer join finance.postal_address pas
        on tmp3.bilt_cust_nbr = pas.customer_num 
        and tmp3.shpto_cust_nbr = pas.customer_num
        and tmp3.shpto_addr_seq_id = pas.address_seq_num 
        and tmp3.so_bu_id = pas.business_unit_id 
        and pas.address_type_code = 'S'
    inner join corp_drm_pkg.phys_geo_hier pgh on tmp3.cust_bu_id = pgh.bu_id
    where pgh.rgn_abbr = 'AMER'
    group by 1,2,3,4,5,6,7,8

    union

    /*APJ*/
    select
        tmp3.cnsld_src_txn_id,
        tmp3.cnsld_src_txn_bu_id,
        cast(coalesce(pgh.iso_ctry_code_2, (case 
            when bc.elec_addr is not null then bc.ctry_cd
            when sc.elec_addr is not null then sc.ctry_cd
            else null end)) as varchar(2)) as iso_ctry_2_cd,
        cast(null as varchar(2)) as iso_lang_cd,
        cast(trim(case 
            when bc.elec_addr is not null then coalesce(bcu.cust_nm, bc.cust_nm)
            when sc.elec_addr is not null then coalesce(scu.cust_nm, sc.cust_nm)
            else null end) as varchar(250)) as cust_co_nm,
        cast(trim(case 
            when bc.elec_addr is not null then bc.frst_nm
            when sc.elec_addr is not null then sc.frst_nm
            else null end) as varchar(100)) as x_frst_nm,
        cast(trim(case 
            when bc.elec_addr is not null then bc.last_nm
            when sc.elec_addr is not null then sc.last_nm
            else null end) as varchar(100)) as x_last_nm,
        cast(trim(case
            when x_frst_nm is not null or x_last_nm is not null then coalesce(x_frst_nm,'') || ' ' || coalesce(x_last_nm,'')
            else null end) as varchar(200)) as full_nm,
        cast(trim(coalesce(bc.elec_addr, sc.elec_addr)) as varchar(254)) as email_addr_id,
        cast(null as varchar(254)) as work_phone
    from tmp_work_db.comm_nps_sb_trans_tmp3 tmp3
    left outer join party_pkg.sls_svc_cust bcu on tmp3.bilt_src_cust_id = bcu.src_cust_id    and tmp3.cnsld_src_txn_bu_id = bcu.src_bu_id
    left outer join 
        (select    
                src_bu_id,
                src_cust_id,
                src_postal_addr_id,
                src_cntct_id,
                cust_nm,
                parnt_cust_nm,
                ctry_cd,
                lang_cd,
                frst_nm,
                last_nm,
                elec_addr
             from party_pkg.sls_svc_cust_ph_cntct_bridge
             where addr_cntct_role_cd = 'BILL_TO' and elec_addr is not null     
             group by 1,2,3,4,5,6,7,8,9,10,11)  bc     
    on tmp3.cnsld_src_txn_bu_id = bc.src_bu_id and tmp3.bilt_src_cust_id = bc.src_cust_id and tmp3.bilt_addr_seq_id = bc.src_postal_addr_id  and tmp3.bilt_cntct_id = bc.src_cntct_id        
    left outer join party_pkg.sls_svc_cust scu on tmp3.shpto_src_cust_id = scu.src_cust_id    and tmp3.cnsld_src_txn_bu_id = scu.src_bu_id    
    left outer join 
        (select    
                src_bu_id,
                src_cust_id,
                src_postal_addr_id,
                src_cntct_id,
                cust_nm,
                parnt_cust_nm,
                ctry_cd,
                lang_cd,
                frst_nm,
                last_nm,
                elec_addr
             from party_pkg.sls_svc_cust_ph_cntct_bridge
             where addr_cntct_role_cd = 'SHIP_TO' and elec_addr is not null     
             group by 1,2,3,4,5,6,7,8,9,10,11)  sc     
    on tmp3.cnsld_src_txn_bu_id = sc.src_bu_id and tmp3.shpto_src_cust_id = sc.src_cust_id and tmp3.shpto_addr_seq_id = sc.src_postal_addr_id  and tmp3.shpto_cntct_id = sc.src_cntct_id
    inner join corp_drm_pkg.phys_geo_hier pgh on tmp3.cust_bu_id = pgh.bu_id
--    where pgh.rgn_abbr = 'APJ'
    group by 1,2,3,4,5,6,7,8,9,10;

delete from tmp_work_db.comm_nps_sb_trans_tmp4 
where cnsld_src_txn_bu_id = 8270 and cnsld_src_txn_id in ( select so_nbr from sas_pulse_Stg.SB_DDW_CN_USERS )
;
insert into tmp_work_db.comm_nps_sb_trans_tmp4
    (
        cnsld_src_txn_id,
        cnsld_src_txn_bu_id,
        iso_ctry_cd_2,
        iso_lang_cd,
        cust_co_nm,
        frst_nm,
        last_nm,
        full_nm,
        email_addr_id,
        work_phone
    )
select
cnsld_src_txn_id,
cnsld_src_txn_bu_id,
iso_ctry_cd_2,
iso_lang_cd,
null as cust_co_nm,
null as frst_nm,
null as last_nm,
full_nm,
email_addr_id,
ph_nbr
from tmp_work_db.comm_nps_sb_trans_tmp3 tmp3
inner join sas_pulse_Stg.sb_ddw_cn_users cn_sb on tmp3.so_nbr = cn_sb.so_nbr and tmp3.so_bu_id = cn_sb.so_bu_id

    /*EMEA*/
/*
    select
        tmp3.cnsld_src_txn_id,
        tmp3.cnsld_src_txn_bu_id,
        cast(co.country_code_iso2 as char(2)) as iso_ctry_cd_2,
        cast(pcce.marketing_language_code as char(2)) as iso_lang_cd,
        cast(trim(case when (ce.customer_name is not null or ce.customer_name_2nd_line is not null) 
            then trim((coalesce(ce.customer_name,' ')) || ' ' || (coalesce(ce.customer_name_2nd_line,' ')))
            else null end) as varchar(250)) as cust_co_nm,
        cast(trim(cce.first_name) as varchar(100)) as frst_nm,
        cast(trim(cce.last_name) as varchar(100)) as last_nm,
        cast(trim(case when (cce.first_name is not null or cce.last_name is not null) 
                then (trim(coalesce(cce.first_name , '')) || ' ' || trim(coalesce(cce.last_name, ''))) else null end) as varchar(200)) as full_nm,
        cast(trim(case when pcce.email_flag = 'Y' then pcce.contact_email_address else null end) as varchar(254)) as email_addr_id,
        cast(null as varchar(254)) as work_phone
    from tmp_work_db.comm_nps_sb_trans_tmp3 tmp3
    left outer join euro_fin_mart.customer_contact_euro cce 
        on tmp3.bilt_addr_seq_id = cce.address_seq_num 
        and tmp3.bilt_cntct_id = cce.customer_contact_seq_num 
        and tmp3.cust_bu_id = cce.business_unit_id
    left outer join 
        (select business_unit_id, 
                order_num, 
                bill_to_pros_contact_id, 
                ship_to_pros_contact_id
                QUALIFY RANK() OVER (PARTITION BY business_unit_id, order_num ORDER BY ORDER_DATE DESC) = 1
          from euro_rt.quote_euro) qh 
        on tmp3.cnsld_src_txn_id = qh.order_num 
        and tmp3.cnsld_src_txn_bu_id = qh.business_unit_id
    left outer join euro_rt.prospect_cust_contact_euro pcce 
        on qh.bill_to_pros_contact_id = pcce.prospect_contact_id
        and qh.business_unit_id = pcce.business_unit_id
    left outer join euro_fin_mart.customer_euro ce
        on tmp3.bilt_cust_nbr = ce.customer_num 
        and tmp3.cust_bu_id = ce.business_unit_id
    inner join corp_drm_pkg.phys_geo_hier pgh on tmp3.cust_bu_id = pgh.bu_id
    left outer join corp.country co
        on ce.country_code = co.country_code
    where pgh.rgn_abbr = 'EMEA'
    group by 1,2,3,4,5,6,7,8,9

    union
	
	*/