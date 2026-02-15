
select cst_id,count(*) as cst 
   from silver.crm_cust_info
   group by cst_id
having count(*)>1 or cst_id is null

select * from silver.crm_cust_info
