delete from sas_pulse_stg.comm_nps_sb_trans_final all ; 
insert into sas_pulse_stg.comm_nps_sb_trans_final sel * from tmp_work_db.comm_nps_sb_trans_final


delete from sas_pulse_stg.comm_nps_sb_trans_smpl all;
insert into sas_pulse_stg.comm_nps_sb_trans_smpl
select *
from sas_pulse_stg.comm_nps_sb_trans_final cntct
where cntct.email_addr_id not in (select email_addr_id from sas_pulse_stg.comm_nps_sb_trans_excl)
sample
when iso_ctry_cd_2 = 'CN' and sls_chnl_cd = 'OFFLINE' then 80 --FY17H2M09
when iso_ctry_cd_2 = 'CN' and sls_chnl_cd = 'ONLINE' then 20  --FY17H2M09
when iso_ctry_cd_2 = 'JP' and sls_chnl_cd = 'OFFLINE' and so_lcl_chnl_cd <> '35023'  then 181 --FY17H2M09
when iso_ctry_cd_2 = 'JP' and sls_chnl_cd = 'ONLINE' and so_lcl_chnl_cd <> '35023' then 272 --FY17H2M09
when iso_ctry_cd_2 = 'IN' and sls_chnl_cd = 'OFFLINE' then 80 --FY17H2M09
when iso_ctry_cd_2 = 'US' and sls_chnl_cd = 'OFFLINE' then 3666 --FY17H2M09
when iso_ctry_cd_2 = 'US' and sls_chnl_cd = 'ONLINE' then 3666 --FY17H2M09
when iso_ctry_cd_2 = 'BR' and sls_chnl_cd = 'OFFLINE' then 215 --FY17H2M09
when iso_ctry_cd_2 = 'BR' and sls_chnl_cd = 'ONLINE' then 500 --FY17H2M09
when iso_ctry_cd_2 = 'MX' and sls_chnl_cd = 'OFFLINE' then 466 --FY17H2M09
when iso_ctry_cd_2 = 'MX' and sls_chnl_cd = 'ONLINE' then 154 --FY17H2M09
when iso_ctry_cd_2 = 'GB' and sls_chnl_cd = 'OFFLINE' then 288 --FY17H2M09
when iso_ctry_cd_2 = 'GB' and sls_chnl_cd = 'ONLINE' then 288 --FY17H2M09
end;


-- Old

delete from sas_pulse_stg.comm_nps_sb_trans_smpl all;
insert into sas_pulse_stg.comm_nps_sb_trans_smpl
select *
from sas_pulse_stg.comm_nps_sb_trans_final cntct
where cntct.email_addr_id not in (select email_addr_id from sas_pulse_stg.comm_nps_sb_trans_excl)
sample
when iso_ctry_cd_2 = 'CN' and sls_chnl_cd = 'OFFLINE' then 80 --FY17H2M09
when iso_ctry_cd_2 = 'CN' and sls_chnl_cd = 'ONLINE' then 20  --FY17H2M09
when iso_ctry_cd_2 = 'JP' and sls_chnl_cd = 'OFFLINE' and so_lcl_chnl_cd <> '35023' then 181 --FY17H2M09
when iso_ctry_cd_2 = 'JP' and sls_chnl_cd = 'ONLINE' and so_lcl_chnl_cd <> '35023' then 272 --FY17H2M09
/*
when iso_ctry_cd_2 = 'AU' and sls_chnl_cd = 'OFFLINE' then 425 --FY17H1
when iso_ctry_cd_2 = 'AU' and sls_chnl_cd = 'ONLINE' then 425 --FY17H1
*/
when iso_ctry_cd_2 = 'IN' and sls_chnl_cd = 'OFFLINE' then 80 --FY17H2M09

insert into sas_pulse_stg.comm_nps_sb_trans_smpl
select *
from sas_pulse_stg.comm_nps_sb_trans_final cntct
where cntct.email_addr_id not in (select email_addr_id from sas_pulse_stg.comm_nps_sb_trans_excl) 
and cntct.email_addr_id not in (select cntct.email_addr_id from sas_pulse_stg.comm_nps_cntct_final cntct)
and ( iso_ctry_cd_2 <> 'US' or (iso_ctry_cd_2 = 'US' and cust_lcl_chnl_cd = '04')) --  ALOT OF LCL_CHL_CD 04 FOR US IS NOW 12
sample
when iso_ctry_cd_2 = 'US' and sls_chnl_cd = 'OFFLINE' then 3666 --FY17H2M09
when iso_ctry_cd_2 = 'US' and sls_chnl_cd = 'ONLINE' then 3666 --FY17H2M09
/*
when iso_ctry_cd_2 = 'CA' and sls_chnl_cd = 'OFFLINE' then 2000 --FY17H1
when iso_ctry_cd_2 = 'CA' and sls_chnl_cd = 'ONLINE' then 2000 --FY17H1
*/
when iso_ctry_cd_2 = 'BR' and sls_chnl_cd = 'OFFLINE' then 215 --FY17H2M09
when iso_ctry_cd_2 = 'BR' and sls_chnl_cd = 'ONLINE' then 500 --FY17H2M09
when iso_ctry_cd_2 = 'MX' and sls_chnl_cd = 'OFFLINE' then 466 --FY17H2M09
when iso_ctry_cd_2 = 'MX' and sls_chnl_cd = 'ONLINE' then 154 --FY17H2M09

insert into sas_pulse_stg.comm_nps_sb_trans_smpl
select *
from sas_pulse_stg.comm_nps_sb_trans_final cntct
where cntct.email_addr_id not in (select email_addr_id from sas_pulse_stg.comm_nps_sb_trans_excl)
sample
when iso_ctry_cd_2 = 'GB' and sls_chnl_cd = 'OFFLINE' then 288 --FY17H2M09
when iso_ctry_cd_2 = 'GB' and sls_chnl_cd = 'ONLINE' then 288 --FY17H2M09
when iso_ctry_cd_2 = 'FR' and sls_chnl_cd = 'OFFLINE' then 622 --FY17H1
when iso_ctry_cd_2 = 'FR' and sls_chnl_cd = 'ONLINE' then 622 --FY17H1
when iso_ctry_cd_2 = 'DE' and sls_chnl_cd = 'OFFLINE' then 562 --FY17H1
when iso_ctry_cd_2 = 'DE' and sls_chnl_cd = 'ONLINE' then 562 --FY17H1

insert into sas_pulse_stg.comm_nps_sb_trans_smpl
select *
from sas_pulse_stg.comm_nps_sb_trans_final cntct
where cntct.email_addr_id not in (select email_addr_id from sas_pulse_stg.comm_nps_sb_trans_excl)
and cntct.cust_lcl_chnl_cd = '12' 
and cntct.email_addr_id not in (select cntct.email_addr_id from sas_pulse_stg.comm_nps_cntct_final cntct)
sample 750
;
