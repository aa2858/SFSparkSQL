##Note: we are joining to the PO_HEAD table to get the co_cnfm_from_procurement_nbr value.
##While this value exists in the Detail table, there looks to be about 151 records where the values are different between the 2 tables

#sqlstatement1_po_detail
#sqlstatement2_po_header
#sqlstatement3_join

sqlstatement1_po_detail=""" select
f.co_skey,
f.co_cnfm_from_procurement_nbr as dtl_co_cnfm_from_procurement_nbr,
f.co_po_line_nbr,
f.itm_skey,
f.co_recv_qty,
f.co_cnfm_ap_unit_amt,
f.co_cnfm_fob_unit_amt,
f.co_po_nbr,
f.co_recv_net_wgt_val,
f.co_vndr_ship_from_skey,
co_bus_antcp_recpt_dt,
f.co_cnfm_from_procurement_nbr
from edwp.po_dtl_fact  F
where co_skey in (7,56) and  co_bus_ordr_dt>'06/01/2017'  """

sqlstatement2_po_header=""" 	SELECT
                co_cnfm_from_procurement_nbr,
				co_skey,
				co_po_nbr,
				co_vndr_ship_from_skey,
				co_bus_antcp_recpt_dt
		from edwp.po_head_fact
where co_skey in (7,56) and  co_bus_ordr_dt>'06/01/2017'  """

#input#1 rs_TMP_SQL_po_detail_mstr
#input#2 rs_TMP_TMP_SQL_po_header_mstr

sqlstatement3_join = """ select
	dtl.co_skey, --use to lookup OpCo Number
	dtl.co_po_line_nbr,
	dtl.itm_skey, --use to lookup Item Number
	dtl.co_recv_qty,
	dtl.co_cnfm_ap_unit_amt,
	dtl.co_cnfm_fob_unit_amt,
	dtl.co_po_nbr,
	dtl.co_recv_net_wgt_val,
	head.co_cnfm_from_procurement_nbr as co_cnfm_from_procurement_nbr_header
from
	(
	select 	f.co_skey,
			f.co_cnfm_from_procurement_nbr as dtl_co_cnfm_from_procurement_nbr,
			f.co_po_line_nbr,
			f.itm_skey,
			f.co_recv_qty,
			f.co_cnfm_ap_unit_amt,
			f.co_cnfm_fob_unit_amt,
			f.co_po_nbr,
			f.co_recv_net_wgt_val,
			f.co_vndr_ship_from_skey,
			co_bus_antcp_recpt_dt,
			f.co_cnfm_from_procurement_nbr
	from    rs_TMP_SQL_po_detail_mstr  f
	where co_cnfm_from_procurement_nbr != ''
	)dtl
 join
	(
		select  co_cnfm_from_procurement_nbr,
				co_skey,
				co_po_nbr,
				co_vndr_ship_from_skey,
				co_bus_antcp_recpt_dt
		from rs_TMP_SQL_po_header_mstr
		where co_cnfm_from_procurement_nbr != '' --and co_cnfm_from_procurement_nbr != 'NOSEND'
		group by
				co_cnfm_from_procurement_nbr,
				co_skey,
				co_po_nbr,
				co_vndr_ship_from_skey,
				co_bus_antcp_recpt_dt
	)head
	on 	dtl.co_skey = head.co_skey and
		dtl.co_po_nbr = head.co_po_nbr and
		dtl.co_vndr_ship_from_skey = head.co_vndr_ship_from_skey and
		dtl.co_bus_antcp_recpt_dt = head.co_bus_antcp_recpt_dt """
