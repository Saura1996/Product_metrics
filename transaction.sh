transaction.sh=$0
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi
v_dataset_name=project_product_metrics;
date

## scheduling_transaction_query_1 loading. Replace existing
v_query_transaction_Q1_new="SELECT a.orderid_header,
user_id,
platform,
ispaid,
totalprice_,
DATE(MSEC_TO_TIMESTAMP(createdat+19800000)) date_,
promocode,
promoamount_,
unitprice_,
flatcommission_,
marginpercentage,
order_status,
merchantid,
dealid,
nb_cashback,
voucher_id,
  CASE WHEN ispaid = 't' THEN unitprice_ END GB,
  CASE WHEN flatcommission_ is null then (marginpercentage * unitprice_)/100
       ELSE flatcommission_ END GR, 
FROM(SELECT
  orderid orderid_header,
  customerid user_id,
  source platform,
  ispaid,
  totalprice/100 totalprice_,
  createdat,
  promocode,
  (totalprice - payable)/100 promoamount_
FROM 
  [big-query-1233:Atom.order_header] )a
  INNER JOIN
  (SELECT 
  orderid orderid_2,
  unitprice/100 unitprice_,
  flatcommission/100 flatcommission_,
  marginpercentage,
  orderlineid voucher_id ,
  CASE
    WHEN status = 14 THEN 'Accepted'
    WHEN status = 15 THEN 'Redeemed'
    WHEN status = 16 THEN 'Refunded'
    WHEN status = 17 THEN 'Cancelled'
    END order_status,
  merchantid,
  dealid,
  cashbackamount/100 nb_cashback,
  FROM [big-query-1233:Atom.order_line] 
  )b
  ON 
  a.orderid_header = b.orderid_2
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18"
  ##echo -e "Query: \n $v_query_transaction_Q1_new";

tableName=transaction_headerline
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_transaction_Q1_new\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "v_query_transaction_Q1_new" &
v_first_pid=$!
v_transaction_sdl_pids+=" $v_first_pid"

wait $v_first_pid;

## transaction_query_2 loading. Replace existing
v_query_transaction_Q2_new="SELECT date_,
  a.a_orderid_header orderid_,
  user_id,
  platform,
  merchantid,
  dealid,
  ispaid,
  totalprice_,
  promocode,
  promoamount_,
  order_status,
  voucher_id,
  nb_cashback,
  merchant_cashback,
  GB,
  GR
FROM (
  SELECT
    *
  FROM
    [big-query-1233:temp.transaction_orderheaderline] )a
LEFT JOIN (
  SELECT
    orderid,
    SUM(cashbackamount/100) merchant_cashback
  FROM
    [big-query-1233:Atom.order_bom] 
    GROUP BY 1
    )b
ON
  a.a_orderid_header = b.orderid
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16"
##echo -e "Query: \n $v_query_transaction_Q2_new";

tableName=transaction_headerlinebom
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_transaction_Q2_new\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "v_query_transaction_Q2_new" &
v_first_pid=$!
v_transaction_sdl_pids+=" $v_first_pid"

wait $v_first_pid;


## transaction_query_3 loading. Replace existing
v_query_transaction_Q3_new="SELECT 
orderid,
CASE WHEN transactiontype = 1 THEN 1 ELSE 0 END credit_payment,
CASE
      WHEN paymentmode = 1 THEN 'PayU'
      WHEN paymentmode = 2 THEN 'PayTM'
      WHEN paymentmode = 3 THEN 'MobiKwik'
      WHEN paymentmode = 4 THEN 'Citrus'
      WHEN paymentmode = 5 THEN 'Citrus'
      WHEN paymentmode = 6 THEN 'FreeCharge'
      WHEN paymentmode = 7 THEN 'Airtel Money'
      WHEN paymentmode = 8 THEN 'PayTM Gateway'
      WHEN paymentmode = 0 THEN 'Credits'
    END payment_gateway,
    CASE
      WHEN failurereason = 1 THEN 'timeout'
      WHEN failurereason = 2 THEN 'failed/cancelled at Payment gateway'
    END failure_reason,
    CASE
      WHEN status = 22 THEN 'Failed'
      WHEN status = 23 THEN 'Successful transaction'
    END transaction_status 
    FROM [big-query-1233:Atom.transaction]
    WHERE transactiontype  = 1
    AND paymentflag = 2
    AND status != 21
    GROUP BY 1,2,3,4,5"
##echo -e "Query: \n $v_query_transaction_Q3_new";

tableName=transaction_credit
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_transaction_Q3_new\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "v_query_transaction_Q3_new" &
v_first_pid=$!
v_transaction_sdl_pids+=" $v_first_pid"

wait $v_first_pid;


## transaction_query_4 loading. Replace existing
v_query_transaction_Q4_new="SELECT 
orderid,
CASE WHEN transactiontype = 2 THEN 1 ELSE 0 END credit_payment,
CASE
      WHEN paymentmode = 1 THEN 'PayU'
      WHEN paymentmode = 2 THEN 'PayTM'
      WHEN paymentmode = 3 THEN 'MobiKwik'
      WHEN paymentmode = 4 THEN 'Citrus'
      WHEN paymentmode = 5 THEN 'Citrus'
      WHEN paymentmode = 6 THEN 'FreeCharge'
      WHEN paymentmode = 7 THEN 'Airtel Money'
      WHEN paymentmode = 8 THEN 'PayTM Gateway'
      WHEN paymentmode = 0 THEN 'Credits'
    END payment_gateway,
    CASE
      WHEN failurereason = 1 THEN 'timeout'
      WHEN failurereason = 2 THEN 'failed/cancelled at Payment gateway'
    END failure_reason,
    CASE
      WHEN status = 22 THEN 'Failed'
      WHEN status = 23 THEN 'Successful transaction'
    END transaction_status 
    FROM [big-query-1233:Atom.transaction]
    WHERE transactiontype  = 2
    AND paymentflag = 2
    AND status != 21
    GROUP BY 1,2,3,4,5"
 ##echo -e "Query: \n $v_query_transaction_Q4_new";
 

tableName=transaction_cash
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_transaction_Q4_new\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "v_query_transaction_Q4_new" &
v_first_pid=$!
v_transaction_sdl_pids+=" $v_first_pid"

wait $v_first_pid;



v_query_transaction_Q5_new="SELECT
  date_,
  c.orderid1 _orderid,
  user_id,
  platform,
  merchantid,
  dealid,
  ispaid,
  totalprice_,
  promocode,
  promoamount_,
  order_status,
  voucher_id,
  nb_cashback,
  merchant_cashback,
  GB,
  GR,
  cash_payment,
  credit_payment,
--   transaction_status,
--   payment_gateway,
--   failure_reason
-- ts_cash,
-- ts_credit,
CASE WHEN ts_cash = 'Failed' OR ts_credit = 'Failed' THEN 'FAILED' ELSE 'SUCCESSFUL' END transaction_status_,
payment_gateway_,
failure_reason_
FROM (
  SELECT
    date_,
    a.orderid_ orderid1,
    user_id,
    platform,
    merchantid,
    dealid,
    ispaid,
    totalprice_,
    promocode,
    promoamount_,
    order_status,
    voucher_id,
    nb_cashback,
    merchant_cashback,
    GB,
    GR,
    cash_payment,
--     transaction_status,
--     payment_gateway,
--     failure_reason
  ts_cash,
  payment_gateway_,
  failure_reason_
  FROM (
    SELECT
      *
    FROM
      [big-query-1233:project_product_metrics.transaction_headerlinebom]) a
  LEFT JOIN (
    SELECT
      orderid,
      cash_payment,
      transaction_status ts_cash,
      payment_gateway payment_gateway_,
      failure_reason failure_reason_
    FROM
      [big-query-1233:project_product_metrics.transaction_cash])b
  ON
    a.orderid_ = b.orderid
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20 )c
LEFT JOIN (
  SELECT
    orderid,
    credit_payment,
    transaction_status ts_credit
  FROM
    [big-query-1233:project_product_metrics.transaction_credit])d
ON
  c.orderid1 = d.orderid
WHERE NOT (cash_payment is null and credit_payment is null)
--   transaction_status IS NOT NULL
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  13,
  14,
  15,
  16,
  17,
  18,
  19,
  20,
  21"
  ##echo -e "Query: \n $v_query_transaction_Q4_new";
 

tableName=transaction_final
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_transaction_Q5_new\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "v_query_transaction_Q5_new" &
v_first_pid=$!
v_transaction_sdl_pids+=" $v_first_pid"

wait $v_first_pid;





if wait $v_transaction_sdl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"

if wait $v_transaction_sdl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Table refresh status of Transaction Query 1 to 5 in project_product_metrics dataset(Transaction Table):$v_table_status`date`" | mail -s "$v_table_status" saurabh.deosarkar@nearbuy

exit 0
