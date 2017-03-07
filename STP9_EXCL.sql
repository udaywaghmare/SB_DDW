delete from sas_pulse_stg.comm_nps_sb_trans_excl all;
insert into sas_pulse_stg.comm_nps_sb_trans_excl
select
cntct.email_addr_id,
max(ce.excl_src)
from tmp_work_db.comm_nps_sb_trans_final cntct 
inner join sas_pulse_stg.comm_nps_cntct_excl ce 
on cntct.email_addr_id = ce.email_addr_id
where ce.excl_src in ('SMX Opt Out')
and cntct.email_addr_id not in (select email_addr_id from sas_pulse_stg.comm_nps_sb_trans_excl)
group by 1;

insert into sas_pulse_stg.comm_nps_sb_trans_excl
select
cntct.email_addr_id,
'GEMS Opt Out' as excl_src
from tmp_work_db.comm_nps_sb_trans_final cntct
inner join marcom.email_subscriber es 
on cntct.email_addr_id = trim(es.email_address)
where es.emailable_flag = 'N'
and cntct.email_addr_id not in (select email_addr_id from sas_pulse_stg.comm_nps_sb_trans_excl)
group by 1,2;

insert into sas_pulse_stg.comm_nps_sb_trans_excl
select
cntct.email_addr_id,
'Pulse Opt Out' as excl_src
from tmp_work_db.comm_nps_sb_trans_final cntct
inner join ce_base.survey_do_not_dstrb_list dnd 
on cntct.email_addr_id = dnd.email_addr_id
where dnd.email_type_flg = 'E' 
and cntct.email_addr_id not in (select email_addr_id from sas_pulse_stg.comm_nps_sb_trans_excl)
group by 1,2;

insert into sas_pulse_stg.comm_nps_sb_trans_excl
select
cntct.email_addr_id,
'Pulse Opt Out' as excl_src
from tmp_work_db.comm_nps_sb_trans_final cntct
inner join ce_base.survey_do_not_dstrb_list dnd 
on cntct.domain_email_addr_id = dnd.email_addr_id
where dnd.email_type_flg = 'D' 
and cntct.email_addr_id not in (select email_addr_id from sas_pulse_stg.comm_nps_sb_trans_excl)
group by 1,2;

insert into sas_pulse_stg.comm_nps_sb_trans_excl
select
cntct.email_addr_id,
'PREV Audience' as excl_src
from tmp_work_db.comm_nps_sb_trans_final cntct
where EMAIL_ADDR_ID IN (
sel email_address from  SAS_PULSE_STG.fy17h2m11_nps_audience union 
sel email_address from  SAS_PULSE_STG.FY17H2M12_NPS_AUDIENCE union 
sel email_addr_id from sas_pulse_stg.comm_nps_aud_inv union
sel email_address from  SAS_PULSE_STG.FY18H1M01_NPS_AUDIENCE  union
sel email_address from  SAS_PULSE_STG.FY18H1M02_NPS_AUDIENCE  
)
and cntct.email_addr_id not in (select email_addr_id from sas_pulse_stg.comm_nps_sb_trans_excl)
group by 1,2;

insert into sas_pulse_stg.comm_nps_sb_trans_excl
select
cntct.email_addr_id,
'Commercial SFDC' as excl_src
from tmp_work_db.comm_nps_sb_trans_final cntct
where  
EMAIL_ADDR_ID IN
(
sel email_addr_id 
from SAS_PULSE_STG.COMM_NPS_CNTCT_SLTN_1 
)
and cntct.email_addr_id not in (select email_addr_id from sas_pulse_stg.comm_nps_sb_trans_excl)
group by 1,2;

collect stats on sas_pulse_stg.comm_nps_sb_trans_excl;
