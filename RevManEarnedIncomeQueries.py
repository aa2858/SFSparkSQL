
#Note: This is used for AGR_VNDR_AGR_TRANS_RESEG

SqlVendorAgreement=""" SELECT vndragr.co_skey,
vndragr.vndr_skey,
vndragr.vndr_agr_trans_nbr,
vndragr.vndr_agr_trans_line_nbr,
vndragr.bil_bck_amt,
vndragr.incm_ern_dt,
vndragr.agr_bas_on_cd,
vndragr.trans_qty,
vndragr.incm_acru_dt,
vndragr.tot_net_wgt_qty,
vndragr.agr_typ_nm,
vndragr.vndr_agr_desc,
vndragr.agr_trk_only_ind,
vndragr.itm_skey,
itm_intrm_desc,
catch_wgt_ind,
unit_pr_case_qty,
curr_rec_ind
from edwp.agr_vndr_agr_trans_fact vndragr
left join (select itm_skey,
						itm_intrm_desc,
						catch_wgt_ind,
						unit_pr_case_qty,
						curr_rec_ind
						from  edwp.itm_dim itm
						where itm.curr_rec_ind = 'Y'
            ) itm on itm.itm_skey = vndragr.itm_skey
where incm_ern_dt>'06/01/2017' and  incm_ern_dt<'07/01/2017' """

filters1="co_skey in (7,56) and incm_ern_dt>'2017-06-01' and vndragr.itm_skey in  (394169,425281,377710,368931,874129,404300,373607,904799,76346) "
filters2="date format <'07/01/2017' "

# seedpro.intp.ei_src_vendoragreements is
#on_hold

sqlVendorAgrBillBackAmount="""
select vndr_agr_trans_nbr,
vndr_agr_trans_line_nbr,
SUM(bil_bck_amt)
bil_bck_amt
FROM rs_TMP_SQL_ei_src_vendor_agreements_mstr
    where agr_bas_on_cd = 'S' and agr_trk_only_ind <> 'Y'
    group by
    vndr_agr_trans_nbr,
    vndr_agr_trans_line_nbr
"""

#   _____      _       _      __          __  _       _     _
#  / ____|    | |     | |     \ \        / / (_)     | |   | |
# | |     __ _| |_ ___| |__    \ \  /\  / /__ _  __ _| |__ | |_
# | |    / _` | __/ __| '_ \    \ \/  \/ / _ \ |/ _` | '_ \| __|
# | |___| (_| | || (__| | | |    \  /\  /  __/ | (_| | | | | |_
#  \_____\__,_|\__\___|_| |_|     \/  \/ \___|_|\__, |_| |_|\__|
#                                                __/ |
#                                               |___/
#

sqlApplyCatchWeightIndicator = """ select 	a.co_skey, 	a.cal_actul_recpt_dt, 	a.vndr_skey, 	a.itm_skey,  trans_qty_hi 	as recv_qty,
	tot_net_wgt_qty_hi as recv_net_wgt_val, 	catch_wgt_ind, 	psmr_ei,
	ce01_ei,
	lsmr_ei,
	msmr_ei,
	flyr_ei,
	0 as vfix_ei,
	other_ei,
	lei_ei,
	freight_revenue_ei,
	freight_cost_ei,
	psmr_ei + ce01_ei as corp_ei,
	lsmr_ei + msmr_ei + flyr_ei + other_ei as local_ei,
	lsmr_ei + msmr_ei + flyr_ei + other_ei + psmr_ei + ce01_ei as total_ei,
	round(psmr_amt,2) as psmr_ei_amt,
	round(ce01_amt,2) as ce01_ei_amt,
	round(lsmr_amt,2) as lsmr_ei_amt,
	round(msmr_amt,2) as msmr_ei_amt,
	round(flyr_amt,2) as flyr_ei_amt,
	0 as vfix_ei_amt,
	round(other_amt,2) as other_ei_amt,
	round(lei_amt,2) as lei_ei_amt,
	round(freight_revenue_amt,2) as freight_revenue_ei_amt,
	round(freight_cost_amt,2) as freight_cost_ei_amt,
	round(psmr_amt,2) + round(ce01_amt,2) as corp_ei_amt,
	round(lsmr_amt,2) + round(msmr_amt,2) + round(flyr_amt,2) + round(other_amt,2) as local_ei_amt,
	round(psmr_amt,2) + round(ce01_amt,2) + round(lsmr_amt,2) + round(msmr_amt,2) + round(flyr_amt,2) + round(other_amt,2) as total_ei_amt,
	lei_case,
	lei_lbs
from    ( 	select 		agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,
		agr_vndr_agr_trans_reseg.co_skey,
		agr_vndr_agr_trans_reseg.itm_skey,
		agr_vndr_agr_trans_reseg.vndr_skey,
		catch_wgt_ind,
		sum(case	when agr_vndr_agr_trans_reseg.agr_typ_nm not in ('PSMR', 'CE01','LSMR','MSMR','FLYR')
					then agr_vndr_agr_trans_reseg.bil_bck_amt else 0 end) OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey) as other_amt,
		sum(case	when agr_vndr_agr_trans_reseg.agr_typ_nm = 'PSMR'
					then agr_vndr_agr_trans_reseg.bil_bck_amt else 0 end) OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey) as psmr_amt,
		sum(case	when agr_vndr_agr_trans_reseg.agr_typ_nm = 'CE01'
					then agr_vndr_agr_trans_reseg.bil_bck_amt else 0 end) OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey) as ce01_amt,
		sum(case	when agr_vndr_agr_trans_reseg.agr_typ_nm = 'LSMR'
					then agr_vndr_agr_trans_reseg.bil_bck_amt else 0 end) OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey) as lsmr_amt,
		sum(case	when agr_vndr_agr_trans_reseg.agr_typ_nm = 'MSMR'
					then agr_vndr_agr_trans_reseg.bil_bck_amt else 0 end) OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey) as msmr_amt,
		sum(case	when agr_vndr_agr_trans_reseg.agr_typ_nm = 'FLYR'
					then agr_vndr_agr_trans_reseg.bil_bck_amt else 0 end) OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey)	as flyr_amt,
		sum(lei_amt_line) 		OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey) as lei_amt,
		sum(freight_revenue) 	OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey) as freight_revenue_amt,
		sum(freight_cost) 		OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey) as freight_cost_amt,
		sum(co_recv_qty) 		OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey) as lei_case,
		sum(co_recv_net_wgt_val)OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey) as lei_lbs,
		max(trans_qty) 			OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey) as trans_qty_hi,
		max(tot_net_wgt_qty) 	OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey) as tot_net_wgt_qty_hi,
		incm_ern_dt 														as cal_actul_recpt_dt,
		ROUND(sum(case when agr_vndr_agr_trans_reseg.agr_typ_nm = 'PSMR' and agr_vndr_agr_trans_reseg.bil_bck_amt != 0 and trans_qty != 0
				then
					case when catch_wgt_ind = 'Y' and tot_net_wgt_qty != 0
						 then agr_vndr_agr_trans_reseg.bil_bck_amt/tot_net_wgt_qty
						 else agr_vndr_agr_trans_reseg.bil_bck_amt/trans_qty
					end
				else 0
				end)
				OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey)
				,3) 														as psmr_ei,
		ROUND(sum(case when agr_vndr_agr_trans_reseg.agr_typ_nm = 'CE01' and agr_vndr_agr_trans_reseg.bil_bck_amt != 0 and trans_qty != 0
				then
					case when catch_wgt_ind = 'Y' and tot_net_wgt_qty != 0
						 then agr_vndr_agr_trans_reseg.bil_bck_amt/tot_net_wgt_qty
						 else agr_vndr_agr_trans_reseg.bil_bck_amt/trans_qty
					end
				else 0
				end)
				OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey)
				,3)														as ce01_ei,
		ROUND(sum(case when agr_vndr_agr_trans_reseg.agr_typ_nm = 'LSMR' and agr_vndr_agr_trans_reseg.bil_bck_amt != 0 and trans_qty != 0
				then
					case when catch_wgt_ind = 'Y' and tot_net_wgt_qty != 0
						 then agr_vndr_agr_trans_reseg.bil_bck_amt/tot_net_wgt_qty
						 else agr_vndr_agr_trans_reseg.bil_bck_amt/trans_qty
					end
				else 0
				end)
				OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey)
				,3)														as lsmr_ei,
		ROUND(sum(case when agr_vndr_agr_trans_reseg.agr_typ_nm = 'MSMR' and agr_vndr_agr_trans_reseg.bil_bck_amt != 0 and trans_qty != 0
				then
					case when catch_wgt_ind = 'Y' and tot_net_wgt_qty != 0
						 then agr_vndr_agr_trans_reseg.bil_bck_amt/tot_net_wgt_qty
						 else agr_vndr_agr_trans_reseg.bil_bck_amt/trans_qty
					end
				else 0
				end)
				OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey)
				,3)														as msmr_ei,
		ROUND(sum(case when agr_vndr_agr_trans_reseg.agr_typ_nm = 'FLYR' and agr_vndr_agr_trans_reseg.bil_bck_amt != 0 and trans_qty != 0
				then
					case when catch_wgt_ind = 'Y' and tot_net_wgt_qty != 0
						 then agr_vndr_agr_trans_reseg.bil_bck_amt/tot_net_wgt_qty
						 else agr_vndr_agr_trans_reseg.bil_bck_amt/trans_qty
					end
				else 0
				end)
				OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey)
				,3)														as flyr_ei,
		ROUND(sum(case when agr_vndr_agr_trans_reseg.agr_typ_nm = 'OTHER' and agr_vndr_agr_trans_reseg.bil_bck_amt != 0 and trans_qty != 0
				then
					case when catch_wgt_ind = 'Y' and tot_net_wgt_qty != 0
						 then agr_vndr_agr_trans_reseg.bil_bck_amt/tot_net_wgt_qty
						 else agr_vndr_agr_trans_reseg.bil_bck_amt/trans_qty
					end
				else 0
				end)
				OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey)
				,3)														as other_ei,
		ROUND(sum(case when lei_amt_line != 0
				then
					case when catch_wgt_ind = 'Y' and co_recv_net_wgt_val != 0
						 then lei_amt_line / co_recv_net_wgt_val
						 else lei_amt_line / co_recv_qty
					end
				else 0
			 	end)
				OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey)
				,3)															as lei_ei,
		ROUND(sum(case when freight_revenue != 0
				then
					case 	when catch_wgt_ind ='Y' and co_recv_net_wgt_val != 0
					 		then freight_revenue/co_recv_net_wgt_val
					 		else freight_revenue/co_recv_qty
					end
				else 0
				end)
				OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey)
				,3) 															as freight_revenue_ei,
		ROUND(sum(case when freight_cost != 0
				THEN
					case 	when catch_wgt_ind = 'Y' and co_recv_net_wgt_val != 0
					 		then freight_cost/co_recv_net_wgt_val
						 	else freight_cost/co_recv_qty
					end
				else 0
				end)
				OVER (PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey)
				,3)															as freight_cost_ei,
		ROW_NUMBER() OVER (	PARTITION BY agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr,agr_vndr_agr_trans_reseg.co_skey,agr_vndr_agr_trans_reseg.itm_skey
							ORDER BY vndr_agr_trans_line_nbr) AS row_num
	from 	    ( 		select 	vndragr.co_skey,
			vndragr.vndr_agr_trans_nbr,
			vndragr.vndr_agr_trans_line_nbr,
			vndragr.vndr_skey,
			vndragr.bil_bck_amt,
			vndragr.agr_typ_nm,
			vndragr.incm_ern_dt,
			vndragr.agr_bas_on_cd,
			vndragr.trans_qty,
			vndragr.incm_acru_dt,
			vndragr.tot_net_wgt_qty,
			vndragr.agr_trk_only_ind,
			vndragr.catch_wgt_ind,
			vndragr.itm_skey
           from rs_TMP_SQL_ei_src_vendor_agreements_mstr  vndragr
		where 	vndragr.incm_ern_dt != '9999-12-31' and
				vndragr.agr_bas_on_cd != 'P' and
				vndragr.trans_qty != 0 and
				vndragr.agr_typ_nm != 'TRAK'
		)agr_vndr_agr_trans_reseg
	left join 	(select 	co_skey,
					po_nbr,
					ordr_seq_nbr,
					co_itm_nbr_skey,
					lei_amt_line,
					freight_revenue,
					freight_cost,
					co_recv_qty,
					co_recv_net_wgt_val
			from rs_TMP_SQL_ei_logistic_earned_income_mstr
		)sus_lei
		ON 	agr_vndr_agr_trans_reseg.co_skey = sus_lei.co_skey and
			agr_vndr_agr_trans_reseg.vndr_agr_trans_nbr = sus_lei.po_nbr and
			agr_vndr_agr_trans_reseg.vndr_agr_trans_line_nbr = sus_lei.ordr_seq_nbr and
			agr_vndr_agr_trans_reseg.itm_skey = sus_lei.co_itm_nbr_skey
	)a
where row_num = 1 """

sqlPurchaseOrdersWeekly= """   SELECT week_ending,
co_skey,
itm_skey,
round(sum(psmr_ei_amt), 2)    as psmr_ei_amt,
round(sum(ce01_ei_amt), 2)    as ce01_ei_amt,
round(sum(lsmr_ei_amt), 2)    as lsmr_ei_amt,
round(sum(msmr_ei_amt), 2)    as msmr_ei_amt,
round(sum(flyr_ei_amt), 2)    as flyr_ei_amt,
round(sum(other_ei_amt), 2)    as other_ei_amt,
round(sum(lei_ei_amt), 2)    as lei_ei_amt,
round(sum(freight_revenue_ei_amt), 2)    as freight_revenue_ei_amt,
round(sum(freight_cost_ei_amt), 2)        as freight_cost_ei_amt,
round(sum(recv_qty), 2)        as recv_qty,
round(sum(recv_net_wgt_val), 2)            as recv_net_wgt_val,
round(sum(lei_case), 2)        as lei_case,
round(sum(lei_lbs), 2)        as lei_lbs
from rs_TMP_SQL_ei_purchase_order_item_level_mstr
group by week_ending,co_skey, itm_skey  """

#                      _
#  ___ _   _ ___     ___(_)
# / __| | | / __|   / _ \ |
# \__ \ |_| \__ \  |  __/ |
# |___/\__,_|___/___\___|_|
#              |_____|
#
#ALIAS oblig_dtl / SOURCE
#pending find and add the missing fields

sql_src_oblig_dtl= """ select 	cust_skey,itm_skey,
                                vndr_skey,
                                oblig_dt,
                                oblig_nbr,
                                oblig_line_nbr,
                                --legacy
                                cst_of_good_sold_unit_amt,
                                adj_cst_of_good_sold_unit_amt,
                                splt_cd,
						        pcs_sold_qty,
						        catch_wgt_val
					from edwp.sale_oblig_dtl_fact
                    where oblig_dt>'06/01/2017' and  oblig_dt<'07/01/2017' """

filter87=" and itm_skey in  (394169,425281,377710,368931,874129,404300,373607,904799,76346) """


#ALIAS oblig_head / SOURCE
sql_src_oblig_head= """ select  cust_skey,
                                oblig_dt,
                                oblig_nbr,
                                oblig_dt,
                                oblig_nbr
                        from edwp.sale_oblig_head_fact
                        where  oblig_dt>'06/01/2017' and  oblig_dt<'07/01/2017'  """


#ALIAS itm / SOURCE
sql_src_itm="""	select 	unit_pr_case_qty,
						itm_rec_eff_dt,
						itm_skey,
						catch_wgt_ind,
						curr_rec_ind
					from edwp.itm_dim
					where curr_rec_ind = 'Y' """

itmfilter1=" and item_skey in  (394169,425281,377710,368931,874129,404300,373607,904799,76346) "


#ALIAS itm_co_itm / SOURCE

sql_src_itm_co_itm=""" 	select 	co_skey,
							itm_skey,
							co_itm_rec_eff_dt,
							unit_pr_case_qty,
							curr_rec_ind
					from edwp.itm_co_itm_rel
					where curr_rec_ind = 'Y'  """

co_itmfilter1="	and itm_skey in  (394169,425281,377710,368931,874129,404300,373607,904799,76346) """

#ALIAS calendar / SOURCE
#pending correct missing fields

sql_src_calendar=  """  select day_dt, fisc_qtr_id, fisc_qtr_desc, fisc_qtr_strt_dt 	from edwp.cal_day_dim """

sql_src_cal_day_dim=  """  select  day_dt, fisc_qtr_id, fisc_qtr_desc, fisc_qtr_strt_dt  from edwp.cal_day_dim """

#alias cust_ship_to / SOURCE
sql_src_cust_ship_to=""" select 	co_skey,cust_skey, cust_ship_to_rec_eff_dt, prm_ship_to_nbr,curr_rec_ind
                            from edwp.cust_ship_to_dim
                          where curr_rec_ind = 'Y'  """

#alias agr_sum / STAGING
#pending_AGGREGATION_ON_HOLD

sql_stg_agr_sum="""  select  vndr_agr_trans_nbr, vndr_agr_trans_line_nbr, SUM(bil_bck_amt) bil_bck_amt
                        from intp.ei_agr_vndr_agr_trans_reseg vndragr
                        where agr_bas_on_cd = 'S' and agr_trk_only_ind <> 'Y'
                        group by vndr_agr_trans_nbr, vndr_agr_trans_line_nbr  """


#ALIAS sus_weekly /#STAGING
sql_stg_sus_weekly= """ select 	co_skey,
                            itm_skey,
							week_ending,
							psmr_ei_amt,
							ce01_ei_amt,
							lsmr_ei_amt,
							msmr_ei_amt,
							flyr_ei_amt,
							other_ei_amt,
							lei_ei_amt,
							freight_revenue_ei_amt,
							freight_cost_ei_amt,
							recv_qty,
							recv_net_wgt_val,
							lei_case,
							lei_lbs
					from intp.ei_sus_weekly """

filter45="					where itm_skey in  (394169,425281,377710,368931,874129,404300,373607,904799,76346) 	"""

#main query
#pending_calculate_field_week_ending_sale

sql_ei_main_full = """ select 	co_skey,
	oblig_dt,
	prm_ship_to_nbr 						as sold_to_cust_nbr,
	cust_skey 								as ship_to_cust_skey,
	oblig_nbr,
	oblig_line_nbr,
	itm_skey,
	vndr_skey,
	dev_amt_vndr							as dev_amt_vndr,
	dev_amt_vndr_unit_amt					as dev_amt_vndr_unit_amt,
	dev_amt_vndr - (cogs - acogs) 			as dev_amt_int,
	cogs - acogs							as dev_amt_tot,
	'SUS' as dtbs_src,
	case_eq_sold_qty,
	catch_wgt_val,
	catch_wgt_ind,
	psmr_ei,
	ce01_ei,
	lsmr_ei,
	msmr_ei,
	flyr_ei,
	0 as vfix_ei,
	other_ei,
	lei_ei,
	freight_revenue_ei,
	freight_cost_ei,
	corp_ei,
	local_ei,
	total_ei,
	psmr_ei_ext,
	lsmr_ei_ext,
	msmr_ei_ext,
	flyr_ei_ext,
	other_ei_ext,
	lei_ei_ext,
	freight_revenue_ei_ext,
	freight_cost_ei_ext,
	corp_ei_ext,
	local_ei_ext,
	total_ei_ext,
	99.99 as lump_sum,
	cust_skey
from  	( SELECT
		cust_skey,
		vndr_skey,
		co_skey,
		itm_skey,
		prm_ship_to_nbr,
		oblig_dt,
		oblig_nbr,
		oblig_line_nbr,
		catch_wgt_val,
		catch_wgt_ind,
		case_eq_sold_qty,
		psmr_ei,
		ce01_ei,
		lsmr_ei,
		msmr_ei,
		flyr_ei,
		other_ei,
		lei_ei,
		freight_revenue_ei,
		freight_cost_ei,
		psmr_ei + ce01_ei 																					as corp_ei,
		lsmr_ei + msmr_ei + flyr_ei + other_ei 																as local_ei,
		psmr_ei + ce01_ei + lsmr_ei + msmr_ei + flyr_ei + other_ei  										as total_ei,
		psmr_ei_ext,
		ce01_ei_ext,
		lsmr_ei_ext,
		msmr_ei_ext,
		flyr_ei_ext,
		other_ei_ext,
		lei_ei_ext,
		freight_revenue_ei_ext,
		freight_cost_ei_ext,
		psmr_ei_ext + ce01_ei_ext 																			as corp_ei_ext,
		lsmr_ei_Ext + msmr_ei_ext + flyr_ei_ext + other_ei_ext												as local_ei_ext,
		psmr_ei_ext + ce01_ei_ext + lsmr_ei_Ext + msmr_ei_ext + flyr_ei_ext + other_ei_ext					as total_ei_ext,
		case	when catch_wgt_ind = 'Y'
				then catch_wgt_val * cst_of_good_sold_unit_amt
				else case_eq_sold_qty * cst_of_good_sold_unit_amt
				end 																						as cogs,
		case 	when catch_wgt_ind = 'Y'
				then catch_wgt_val * adj_cst_of_good_sold_unit_amt
				else case_eq_sold_qty * adj_cst_of_good_sold_unit_amt
				end																							as acogs,
		NVL(bil_bck_amt_agr,0)																				as dev_amt_vndr,
		NVL(case when catch_wgt_ind = 'Y' and recv_net_wgt_val != 0
				then bil_bck_amt_agr / recv_net_wgt_val
				when recv_qty = 0 then 0
				else bil_bck_amt_agr / recv_qty
				end,0)																						as dev_amt_vndr_unit_amt,
		'x' as include,
		99.99 as ei_allocate,
		99.99 as ei_pct
	from 	( SELECT
			cust_skey,
			co_skey,
			vndr_skey,
			itm_skey,
			oblig_dt,
			oblig_nbr,
			oblig_line_nbr,
			cst_of_good_sold_unit_amt,
			adj_cst_of_good_sold_unit_amt,
			bil_bck_amt_agr,
			case_eq_sold_qty,
			recv_net_wgt_val,
			recv_qty,
			prm_ship_to_nbr,
			catch_wgt_val,
			catch_wgt_ind,
			psmr_ei,
			ce01_ei,
			lsmr_ei,
			msmr_ei,
			flyr_ei,
			other_ei,
			lei_ei,
			freight_cost_ei,
			freight_revenue_ei,
			psmr_ei + ce01_ei 																				as corp_ei,
			lsmr_ei + msmr_ei + flyr_ei + other_ei 															as local_ei,
			psmr_ei + ce01_ei + lsmr_ei + msmr_ei + flyr_ei + other_ei  									as total_ei,
			NVL(case when catch_wgt_ind = 'Y'
					then psmr_ei * catch_wgt_val
					else psmr_ei * case_eq_sold_qty
			end,0) 																							as psmr_ei_ext,
			NVL(case when catch_wgt_ind = 'Y'
					then ce01_ei * catch_wgt_val
					else ce01_ei * case_eq_sold_qty
			end,0) 																							as ce01_ei_ext,
			NVL(case when catch_wgt_ind = 'Y'
					then lsmr_ei * catch_wgt_val
					else lsmr_ei * case_eq_sold_qty
			end,0) 																							as lsmr_ei_ext,
			NVL(case when catch_wgt_ind = 'Y'
					then msmr_ei * catch_wgt_val
					else msmr_ei * case_eq_sold_qty
			end,0) 																							as msmr_ei_ext,
			NVL(case when catch_wgt_ind = 'Y'
					then flyr_ei * catch_wgt_val
					else flyr_ei * case_eq_sold_qty
			end,0) 																							as flyr_ei_ext,
			NVL(case when catch_wgt_ind = 'Y'
					then other_ei * catch_wgt_val
					else other_ei * case_eq_sold_qty
			end,0) 																							as other_ei_ext,
			NVL(case when catch_wgt_ind = 'Y'
					then lei_ei * catch_wgt_val
					else lei_ei * case_eq_sold_qty
			end,0) 																							as lei_ei_ext,
			NVL(case when catch_wgt_ind = 'Y'
					then freight_revenue_ei * catch_wgt_val
					else freight_revenue_ei * case_eq_sold_qty
			end,0) 																							as freight_revenue_ei_ext,
			NVL(case when catch_wgt_ind = 'Y'
					then freight_cost_ei * catch_wgt_val
					else freight_cost_ei * case_eq_sold_qty
			end,0) 																							as freight_cost_ei_ext
		from ( 	select 	cust_skey,
				co_skey,
				vndr_skey,
				itm_skey,
				oblig_dt,
				oblig_nbr,
				oblig_line_nbr,
				catch_wgt_ind,
				catch_wgt_val,
				cst_of_good_sold_unit_amt,
				adj_cst_of_good_sold_unit_amt,
				bil_bck_amt_agr,
				recv_net_wgt_val,
				recv_qty,
				prm_ship_to_nbr,
				case 	when psmr_ei_amt != 0 and recv_qty != 0 then
							case 	when catch_wgt_ind = 'Y' and recv_net_wgt_val != 0
									then psmr_ei_amt / recv_net_wgt_val
									else psmr_ei_amt / recv_qty
							end
						else 0 end																			as psmr_ei,
				case 	when ce01_ei_amt != 0 and recv_qty != 0 then
							case 	when catch_wgt_ind = 'Y' and recv_net_wgt_val != 0
									then ce01_ei_amt / recv_net_wgt_val
									else ce01_ei_amt / recv_qty
							end
						else 0 end																			as ce01_ei,
				case 	when lsmr_ei_amt != 0 and recv_qty != 0 then
							case 	when catch_wgt_ind = 'Y' and recv_net_wgt_val != 0
									then lsmr_ei_amt / recv_net_wgt_val
									else lsmr_ei_amt / recv_qty
							end
						else 0 end																			as lsmr_ei,
				case 	when msmr_ei_amt != 0 and recv_qty != 0 then
							case 	when catch_wgt_ind = 'Y' and recv_net_wgt_val != 0
									then msmr_ei_amt / recv_net_wgt_val
									else msmr_ei_amt / recv_qty
							end
						else 0 end																			as msmr_ei,
				case 	when flyr_ei_amt != 0 and recv_qty != 0 then
							case 	when catch_wgt_ind = 'Y' and recv_net_wgt_val != 0
									then flyr_ei_amt / recv_net_wgt_val
									else flyr_ei_amt / recv_qty
							end
						else 0 end																			as flyr_ei,
				case 	when other_ei_amt != 0 and recv_qty != 0 then
							case 	when catch_wgt_ind = 'Y' and recv_net_wgt_val != 0
									then other_ei_amt / recv_net_wgt_val
									else other_ei_amt / recv_qty
							end
						else 0 end																			as other_ei,
				case	when lei_ei_amt != 0 and lei_case !=0 then
							case 	when catch_wgt_ind = 'Y' and lei_lbs != 0
									then lei_ei_amt / lei_lbs
									else lei_ei_amt / lei_case
							end
						else 0 end 																			as lei_ei,
				case 	when freight_revenue_ei_amt != 0 and lei_case != 0 then
							case 	when catch_wgt_ind = 'Y' and lei_lbs != 0
									then freight_revenue_ei_amt / lei_lbs
									else freight_revenue_ei_amt / lei_case
							end
						else 0 end 																			as freight_revenue_ei,
				case 	when freight_cost_ei_amt != 0 and lei_case != 0 then
							case 	when catch_wgt_ind = 'Y' and lei_lbs != 0
									then freight_cost_ei_amt / lei_lbs
									else freight_cost_ei_amt / lei_case
							end
						else 0 end 																			as freight_cost_ei,
				case 	when splt_cd = '' OR splt_cd = 'N'
						then pcs_sold_qty
						else pcs_sold_qty / unit_pr_case_qty
						end		 																		as case_eq_sold_qty,
				0 as vfix_ei
			from 	( 	select
					oblig_dtl.week_ending_sale,
					oblig_dtl.cust_skey,
					oblig_dtl.co_skey,
					oblig_dtl.itm_skey,
					oblig_dtl.vndr_skey,
					oblig_dtl.oblig_dt,
					oblig_dtl.oblig_nbr,
					oblig_dtl.oblig_line_nbr,
					catch_wgt_ind,
					catch_wgt_val,
					cst_of_good_sold_unit_amt,
					adj_cst_of_good_sold_unit_amt,
					bil_bck_amt_agr,
					splt_cd,
					pcs_sold_qty,
					prm_ship_to_nbr,
					case 	when unit_pr_case_qty_im = 1
							then unit_pr_case_qty_icm
							else unit_pr_case_qty_im
							end  																			as unit_pr_case_qty,
					NVL(psmr_ei_amt_co, 0) 																	as psmr_ei_amt,
					NVL(ce01_ei_amt_co, 0) 																	as ce01_ei_amt,
					NVL(lsmr_ei_amt_co, 0) 																	as lsmr_ei_amt,
					NVL(msmr_ei_amt_co, 0) 																	as msmr_ei_amt,
					NVL(flyr_ei_amt_co, 0) 																	as flyr_ei_amt,
					NVL(other_ei_amt_co, 0) 																as other_ei_amt,
					NVL(lei_ei_amt_co, 0) 																	as lei_ei_amt,
					NVL(freight_revenue_ei_amt_co, 0) 														as freight_revenue_ei_amt,
					NVL(freight_cost_ei_amt_co, 0) 															as freight_cost_ei_amt,
					NVL(recv_qty_co, 0) 																	as recv_qty,
					NVL(recv_net_wgt_val_co, 0) 															as recv_net_wgt_val,
					NVL(lei_case_co, 0) 																	as lei_case,
					NVL(lei_lbs_co, 0) 																		as lei_lbs
				from 	( 		select
						week_ending_sale,
						oblig_dt,
						oblig_dtl.oblig_nbr,
						oblig_dtl.oblig_line_nbr,
						oblig_dtl.cust_skey,
						oblig_dtl.vndr_skey,
						itm_skey,
						cust_ship_to_dim.co_skey,
						cst_of_good_sold_unit_amt,
						adj_cst_of_good_sold_unit_amt,
						splt_cd,
						pcs_sold_qty,
						catch_wgt_val
					from edwp.sale_oblig_dtl_fact oblig_dtl
					join edwp.cust_ship_to_dim cust_ship_to_dim
						ON 	oblig_dtl.cust_skey = cust_ship_to_dim.cust_skey
          					AND cust_ship_to_dim.curr_rec_ind = 'Y' --PENDING	left join intp.ei_sap_go_live_dates sap_dates L2	on cust_ship_to_dim.co_skey = sap_dates.co_nbr L3 where oblig_dtl.oblig_dt < sap_dates.go_live_dt or oblig_dtl.oblig_dt >= sus_go_live_dt
					)oblig_dtl
				left join 	( 	select
						cust_ship_to_dim.co_skey,
						oblig_dt,
						oblig_nbr
					from edwp.sale_oblig_head_fact oblig_head
					join edwp.cust_ship_to_dim cust_ship_to_dim
						ON 	oblig_head.cust_skey = cust_ship_to_dim.cust_skey 	AND cust_ship_to_dim.curr_rec_ind = 'Y'
					)oblig_head
				on 	oblig_dtl.co_skey = oblig_head.co_skey and
					oblig_dtl.oblig_dt = oblig_head.oblig_dt and
					oblig_dtl.oblig_nbr = oblig_head.oblig_nbr
				left join 	( 		select 	unit_pr_case_qty as unit_pr_case_qty_im,
							itm_rec_eff_dt as itm_rec_eff_dt_im,
							itm_skey,
							catch_wgt_ind
					from edwp.itm_dim
					where curr_rec_ind = 'Y'
					)itm
				on 	oblig_dtl.itm_skey = itm.itm_skey and
					oblig_dtl.oblig_dt = itm.itm_rec_eff_dt_im
				left join ( select 	co_skey,
							itm_skey,
							co_itm_rec_eff_dt as co_itm_rec_eff_dt_icm,
							unit_pr_case_qty as unit_pr_case_qty_icm
					from edwp.itm_co_itm_rel
					where curr_rec_ind = 'Y'
					)itm_co_itm
				on 	oblig_dtl.co_skey = itm_co_itm.co_skey and
					oblig_dtl.itm_skey = itm_co_itm.itm_skey and
					oblig_dtl.oblig_dt = itm_co_itm.co_itm_rec_eff_dt_icm
				left join	( 		select 	co_skey,
							vndr_agr_trans_nbr,
							vndr_agr_trans_line_nbr,
							SUM(bil_bck_amt) bil_bck_amt_agr
					from edwp.agr_vndr_agr_trans_fact vndragr
					where	agr_bas_on_cd = 'S' and
							agr_trk_only_ind != 'Y'
					group by 	co_skey,
							vndr_agr_trans_nbr,
							vndr_agr_trans_line_nbr
					)agr_sum
				on 	oblig_dtl.co_skey = agr_sum.co_skey and
					oblig_dtl.oblig_nbr = agr_sum.vndr_agr_trans_nbr and
					oblig_dtl.oblig_line_nbr = agr_sum.vndr_agr_trans_line_nbr
				left join 	( 	select 	cust_skey,
							cust_ship_to_rec_eff_dt,
							prm_ship_to_nbr
					from edwp.cust_ship_to_dim
					where curr_rec_ind = 'Y'
					)cust_ship_to
				on 	oblig_dtl.cust_skey = cust_ship_to.cust_skey and
					oblig_dtl.oblig_dt = cust_ship_to.cust_ship_to_rec_eff_dt
				left join 	( 	select 	co_skey,
							itm_skey,
							week_ending,
							psmr_ei_amt 				as psmr_ei_amt_co,
							ce01_ei_amt 				as ce01_ei_amt_co,
							lsmr_ei_amt 				as lsmr_ei_amt_co,
							msmr_ei_amt 				as msmr_ei_amt_co,
							flyr_ei_amt 				as flyr_ei_amt_co,
							other_ei_amt 				as other_ei_amt_co,
							lei_ei_amt 					as lei_ei_amt_co,
							freight_revenue_ei_amt 		as freight_revenue_ei_amt_co,
							freight_cost_ei_amt 		as freight_cost_ei_amt_co,
							recv_qty 					as recv_qty_co,
							recv_net_wgt_val 			as recv_net_wgt_val_co,
							lei_case 					as lei_case_co,
							lei_lbs 					as lei_lbs_co
					from intp.ei_sus_weekly
					)sus_weekly
				on	oblig_dtl.co_skey = sus_weekly.co_skey and
					oblig_dtl.itm_skey = sus_weekly.itm_skey and
					oblig_dtl.week_ending_sale = sus_weekly.week_ending
				)a
			)b
		)c
	)d
left join ( select day_dt, fisc_qtr_id as fiscal_quarter
	from edwp.cal_day_dim day_dim
	)calendar
on d.oblig_dt = calendar.day_dt """
