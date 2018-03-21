#pending column name co_pckup_allw_rt to co_pckup_allw_rt_ovrd_amt



sql_src_po_dtl_fact = """ SELECT *
                            from edwp.po_dtl_fact where 1=1 """

f0321="co_po_nbr like '%351%'"
f45="co_po_typ_cd = 'DRP' and co_po_nbr > '00476040' and co_po_nbr < '00790830' "
f1=" where  co_bus_ordr_dt>'06/01/2017' xx  and co_bus_ordr_dt<'07/01/2017'  "


f33s="co_skey in (7,56) and itm_skey in  (394169,425281,377710,368931,874129,404300,373607,904799,76346) "

PurchaseOrderHeader = """ SELECT co_cnfm_ap_ext_amt,
                            co_cnfm_fob_ext_amt,
                            co_pckup_allw_rt            AS co_pf_pckup_allw_rt,
                            co_skey,
                            co_po_nbr,
                            co_po_typ_cd
                            from edwp.po_head_fact where 1=1 """

f0322b="where co_po_nbr like '%351%' "
f99="where co_po_typ_cd = 'DRP' and co_po_nbr > '00476040' and co_po_nbr < '00790830' "

f2="where co_bus_ordr_dt > '06/01/2017' and co_bus_ordr_dt < '07/01/2017'  "


PurchaseOrder1_rdc = """ SELECT itm_skey													as co_itm_skey_rdc,
							co_skey															as co_skey_rdc,
							co_po_nbr														as co_po_nbr_rdc,
							co_po_line_nbr													as co_po_line_nbr_rdc,
							co_recv_qty														as co_recv_qty_rdc,
							co_cnfm_frgt_unit_amt											as co_cnfm_frgt_unit_amt_rdc,
							co_cnfm_ap_unit_amt												as co_cnfm_ap_unit_amt_rdc,
							co_cnfm_fob_unit_amt											as co_cnfm_fob_unit_amt_rdc,
							co_recv_net_wgt_val												as co_recv_net_wgt_val_rdc,
							co_cnfm_from_procurement_nbr									as co_cnfm_from_procurement_nbr_header,
							co_po_typ_cd
					from rs_TMP_SQL_src_po_dtl_fact_mstr """

PurchaseOrder2_non_rdc = """ SELECT 	itm_skey												as co_itm_skey_non_rdc,
							co_skey															as co_skey_non_rdc,
							co_po_line_nbr,
							co_po_nbr,
							co_recv_qty														as co_recv_qty_non_rdc,
							co_cnfm_frgt_unit_amt											as co_cnfm_frgt_unit_amt_non_rdc,
							co_cnfm_ap_unit_amt												as co_cnfm_ap_unit_amt_non_rdc,
							co_cnfm_fob_unit_amt											as co_cnfm_fob_unit_amt_non_rdc,
							co_recv_net_wgt_val												as co_recv_net_wgt_val_non_rdc,
							co_po_typ_cd
					from rs_TMP_SQL_src_po_dtl_fact_mstr """


PurchaseOrder4 = """ SELECT 	co_skey,
						 	co_po_nbr,
						    total_freight_revenue
						from
							( SELECT  co_skey,
							  co_po_nbr,
					 		   sum((co_cnfm_ap_unit_amt-co_cnfm_fob_unit_amt)*co_recv_qty) as total_freight_revenue
							  from rs_TMP_SQL_src_po_dtl_fact_mstr
						GROUP BY co_skey, co_po_nbr  ) f
						where total_freight_revenue != 0 and total_freight_revenue is not null 	 """

TransactionsOrder1 = """  SELECT 	trns_ordr_line_itm_fact.tms_ordr_id							as tms_ordr_id,
						trns_ordr_line_itm_fact.line_itm_id									as line_itm_id,
						trns_facl_dim.co_skey 		 										as co_skey,
						trns_ordr_fact.tc_ordr_id 											as tc_ordr_id_header,
						trns_ordr_fact.actul_amt											as actul_amt_header,
						trns_ordr_fact.tms_ordr_id											as tms_ordr_id_header,
						trns_ordr_fact.dest_facl_id											as dest_facl_id_header,
						trns_ordr_fact.pckup_strt_dttm										as pckup_strt_dttm_header,
						cast(pckup_strt_dttm as date) 										as date,
						case when trns_ordr_fact.tc_ordr_id like 'P%' then substring(trns_ordr_fact.tc_ordr_id,5,len(trns_ordr_fact.tc_ordr_id))
							 when (trns_ordr_fact.tc_ordr_id like 'B%' or trns_ordr_fact.tc_ordr_id like 'A%') and charindex('_',trns_ordr_fact.tc_ordr_id)<2 then substring(trns_ordr_fact.tc_ordr_id,2,len(trns_ordr_fact.tc_ordr_id))
							 when (trns_ordr_fact.tc_ordr_id like 'B%' or trns_ordr_fact.tc_ordr_id like 'A%') and charindex('_',trns_ordr_fact.tc_ordr_id)>=2 then substring(trns_ordr_fact.tc_ordr_id,2,charindex('_',trns_ordr_fact.tc_ordr_id)-2)
							else trns_ordr_fact.tc_ordr_id end 								as ordr_po_nbr,
						substring(tc_ordr_id,5,len(tc_ordr_id))								as sub5_tc_ordr_id
					from ( select *
						        from edwp.trns_ordr_fact
						        where 1=1
        				 ) trns_ordr_fact
        				 join edwp.trns_ordr_line_itm_fact trns_ordr_line_itm_fact  on trns_ordr_line_itm_fact.tms_ordr_id = trns_ordr_fact.tms_ordr_id
					     join edwp.trns_facl_dim trns_facl_dim 	on trns_facl_dim.facl_id = trns_ordr_fact.dest_facl_id  """

f1="where tc_ordr_id like '%351-x%"
F3="""					WHERE  ordr_creat_dttm >'06/01/2017' and  ordr_creat_dttm <'07/01/2017'  """
f76="where substring(tc_ordr_id, 5, len(tc_ordr_id)) > '00476040' and substring(tc_ordr_id, 5, len(tc_ordr_id)) < '00790830'"


Join4Pieces=""" SELECT tmp.tms_ordr_id,
					tmp.line_itm_id,
					tmp.co_skey,
					tmp.tc_ordr_id_header,
					tmp.actul_amt_header,
					tmp.tms_ordr_id_header,
					tmp.dest_facl_id_header,
					tmp.pckup_strt_dttm_header,
					tmp.date,
					po_head.co_pf_pckup_allw_rt,
					case when co_itm_skey_rdc is not null then co_itm_skey_rdc else co_itm_skey_non_rdc end as co_itm_nbr_skey,
					CASE 	when (co_cnfm_ap_unit_amt_non_rdc = 0 or co_cnfm_ap_unit_amt_non_rdc is null) and co_cnfm_ap_unit_amt_rdc > 0 then co_cnfm_ap_unit_amt_rdc
							when (co_cnfm_ap_unit_amt_rdc=0 or co_cnfm_ap_unit_amt_rdc is null) and co_cnfm_ap_unit_amt_non_rdc > 0 then co_cnfm_ap_unit_amt_non_rdc
							when (co_cnfm_ap_unit_amt_non_rdc=0 and co_cnfm_ap_unit_amt_rdc = 0) then 0
							else co_cnfm_ap_unit_amt_non_rdc end as co_cnfm_ap_unit_amt,
					CASE 	when (co_cnfm_fob_unit_amt_non_rdc = 0 or co_cnfm_fob_unit_amt_non_rdc is null) and co_cnfm_fob_unit_amt_rdc > 0 then co_cnfm_fob_unit_amt_rdc
							when (co_cnfm_fob_unit_amt_rdc=0 or co_cnfm_fob_unit_amt_rdc is null) and co_cnfm_fob_unit_amt_non_rdc > 0 then co_cnfm_fob_unit_amt_non_rdc
							when (co_cnfm_fob_unit_amt_non_rdc=0 and co_cnfm_fob_unit_amt_rdc = 0) then 0
							else co_cnfm_fob_unit_amt_non_rdc end as co_cnfm_fob_unit_amt,
					CASE 	when (co_recv_qty_non_rdc = 0 or co_recv_qty_non_rdc is null) and co_recv_qty_rdc > 0 then co_recv_qty_rdc
							when (co_recv_qty_rdc=0 or co_recv_qty_rdc is null) and co_recv_qty_non_rdc > 0 then co_recv_qty_non_rdc
							when (co_recv_qty_non_rdc=0 and co_recv_qty_rdc = 0) then 0
							else co_recv_qty_non_rdc end as co_recv_qty,
					CASE 	when (co_recv_net_wgt_val_non_rdc = 0 or co_recv_net_wgt_val_non_rdc is null) and co_recv_net_wgt_val_rdc > 0 then co_recv_net_wgt_val_rdc
							when (co_recv_net_wgt_val_rdc=0 or co_recv_net_wgt_val_rdc is null) and co_recv_net_wgt_val_non_rdc > 0 then co_recv_net_wgt_val_non_rdc
							when (co_recv_net_wgt_val_non_rdc=0 and co_recv_net_wgt_val_rdc = 0) then 0
							else co_recv_net_wgt_val_non_rdc end as co_recv_net_wgt_val,
					case when tc_ordr_id_header like 'B%' then po_dtl_fact_rdc.co_po_nbr_rdc else tmp.ordr_po_nbr end as po_nbr,
					po_dtl.total_freight_revenue,
					po_head.co_cnfm_ap_ext_amt,
					po_head.co_cnfm_fob_ext_amt,
				    (case when co_recv_qty_rdc is not null 	then co_recv_qty_rdc != 0  else co_recv_qty_non_rdc != 0  end) as flag1,
				    (case when co_recv_qty_rdc > 0  or co_recv_qty_non_rdc >0 then 'Include' else 'Exclude'  end) as flag2,
				    sub5_tc_ordr_id
				from  rs_TMP_SQL_src_TransactionsOrder1_mstr  tmp
				 left join 	TMP_SQL_PurchaseOrder1_rdc  po_dtl_fact_rdc         on	tmp.co_skey = po_dtl_fact_rdc.co_skey_rdc         and 		po_dtl_fact_rdc.co_cnfm_from_procurement_nbr_header = tmp.ordr_po_nbr and tmp.line_itm_id=cast(po_dtl_fact_rdc.co_po_line_nbr_rdc as varchar)
				 left join  TMP_SQL_PurchaseOrder2_non_rdc  po_dtl_fact_non_rdc on 	tmp.co_skey = po_dtl_fact_non_rdc.co_skey_non_rdc and 		 po_dtl_fact_non_rdc.co_po_nbr=tmp.ordr_po_nbr                       and tmp.line_itm_id = cast(po_dtl_fact_non_rdc.co_po_line_nbr as varchar)
				 left join 	rs_TMP_SQL_src_PurchaseOrderHeader_mstr  po_head on	tmp.co_skey = po_head.co_skey and 	tmp.ordr_po_nbr =  po_head.co_po_nbr
    			 left join 	TMP_SQL_PurchaseOrder4  po_dtl 	on 	tmp.co_skey = po_dtl.co_skey  and   tmp.ordr_po_nbr =  po_dtl.co_po_nbr
				where 	case when co_recv_qty_rdc is not null 	then co_recv_qty_rdc != 0  else co_recv_qty_non_rdc != 0 	end """


filternew=""" where ( co_recv_qty_rdc > 0  or co_recv_qty_non_rdc >0 )  """


LogisticEarnedIncomeMainQry1=""" SELECT 	actul_amt_header
,	 co_cnfm_ap_ext_amt
,	 co_cnfm_ap_unit_amt
,	 co_cnfm_fob_ext_amt
,	 co_cnfm_fob_unit_amt
,	 co_itm_nbr_skey
,	 co_pf_pckup_allw_rt
,	 co_recv_net_wgt_val
,	 co_recv_qty
,	 co_skey
,	 date
,	 dest_facl_id_header
,	 flag1
,	 flag2
,	 line_itm_id
,	 pckup_strt_dttm_header
,	 po_nbr
,	 tc_ordr_id_header
,	 tms_ordr_id_header
,	 total_freight_revenue
,	 tms_ordr_id
,   '***->' as equation_step
,	 header_equation
,	 detail_equation
,   '^^^->' as last_step
,sub5_tc_ordr_id
,	ROUND((nvl(detail_equation*co_recv_qty,0)+ nvl((nvl(detail_equation*co_recv_qty,0) /total_freight_revenue )*co_pf_pckup_allw_rt,0)) -	 (nvl(actul_amt_header,0) * ((nvl(detail_equation*co_recv_qty,0)+ nvl((nvl(detail_equation*co_recv_qty,0) /total_freight_revenue )*co_pf_pckup_allw_rt,0)) /header_equation)),2)	as lei_amt_line
,	ROUND(nvl(detail_equation*co_recv_qty,0)+ nvl((nvl(detail_equation*co_recv_qty,0) /total_freight_revenue )*co_pf_pckup_allw_rt,0) ,2)  as freight_revenue
,	ROUND(nvl(actul_amt_header,0) *((nvl(detail_equation*co_recv_qty,0)+ nvl((nvl(detail_equation*co_recv_qty,0) /total_freight_revenue )*co_pf_pckup_allw_rt,0)) /header_equation), 2)	as freight_cost
,	ROUND(co_recv_net_wgt_val,1) co_recv_net_wgt_val2
FROM (select	*, 	(nvl(co_cnfm_ap_unit_amt,0) - nvl(co_cnfm_fob_unit_amt,0))   as detail_equation
	         ,  (nvl(co_cnfm_ap_ext_amt,0)  - nvl(co_cnfm_fob_ext_amt,0))  +  nvl(co_pf_pckup_allw_rt,0 ) as header_equation
        		from TMP_SQL_Join4Pieces
	  )MainOuterQuery """


LogisticEarnedIncomeMainQryAllCols=""" 	SELECT 	actul_amt_header
                                ,	 co_cnfm_ap_ext_amt
                                ,	 co_cnfm_ap_unit_amt
                                ,	 co_cnfm_fob_ext_amt
                                ,	 co_cnfm_fob_unit_amt
                                ,	 co_itm_nbr_skey
                                ,	 co_pf_pckup_allw_rt
                                ,	 co_recv_net_wgt_val
                                ,	 co_recv_qty
                                ,	 co_skey
                                ,	 date
                                ,	 dest_facl_id_header
                                ,	 flag1
                                ,	 flag2
                                ,	 line_itm_id
                                ,	 pckup_strt_dttm_header
                                ,	 po_nbr
                                ,	 tc_ordr_id_header
                                ,	 tms_ordr_id_header
                                ,	 total_freight_revenue
                                ,	 tms_ordr_id
                                ,   '***->' as equation_step
                                ,	 header_equation
                                ,	 detail_equation
                                ,   '^^^->' as last_step
                                ,    freight_revenue
                                ,    freight_cost
                                ,    freight_revenue-freight_cost	as lei_amt_line
 	                            ,    ROUND(co_recv_net_wgt_val,1) co_recv_net_wgt_val2
 	                            ,sub5_tc_ordr_id
                                    FROM
	                                (select *, ROUND( nvl(actul_amt_header,0) * (freight_revenue / header_equation), 2)	as freight_cost
	                                    FROM ( 	select *, ROUND(nvl(detail_equation*co_recv_qty,0)+ nvl((nvl(detail_equation*co_recv_qty,0) /total_freight_revenue )*co_pf_pckup_allw_rt,0) ,2)  as freight_revenue
   	 		                                    from
			                                    ( SELECT *, (nvl(co_cnfm_ap_ext_amt,0)  - nvl(co_cnfm_fob_ext_amt,0))  +  nvl(co_pf_pckup_allw_rt,0 ) as header_equation,
					                                        (nvl(co_cnfm_ap_unit_amt,0) - nvl(co_cnfm_fob_unit_amt,0))   as detail_equation
				                                    FROM TMP_SQL_Join4Pieces
			                                    ) q1
		                                    ) q2
                                	) q3 """

#OutputsAllColumns
LogisticEarnedIncomeMainQry2=""" 	SELECT *
                                    FROM
	                                (select *, ROUND( nvl(actul_amt_header,0) * (freight_revenue / header_equation), 2)	as freight_cost
	                                    FROM ( 	select *, ROUND(nvl(detail_equation*co_recv_qty,0)+ nvl((nvl(detail_equation*co_recv_qty,0) /total_freight_revenue )*co_pf_pckup_allw_rt,0) ,2)  as freight_revenue
   	 		                                    from
			                                    ( SELECT *, (nvl(co_cnfm_ap_ext_amt,0)  - nvl(co_cnfm_fob_ext_amt,0))  +  nvl(co_pf_pckup_allw_rt,0 ) as header_equation,
					                                        (nvl(co_cnfm_ap_unit_amt,0) - nvl(co_cnfm_fob_unit_amt,0))   as detail_equation
				                                    FROM TMP_SQL_Join4Pieces
			                                    ) q1
		                                    ) q2
                                	) q3 """

