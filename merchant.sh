merchant.sh=$0
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi
v_dataset_name=project_product_metrics;
date

## scheduling_merchant_table_part1 loading. Replace existing
v_query_merchant_table_part1="SELECT
  c.merchantID_j1 Merchant_id,
  city,
  merchantname,
  c.DealID_2 DealID_j2,
  merchantCID,
  merchantBAID,
  cat_id,
  DealType,
  is.raffle
FROM (
  SELECT
    a.merchantID_1 merchantID_j1,
    city,
    merchantname,
    dealID_2,
    merchantCID,
    merchantBAID
  FROM (
    SELECT
      STRING(merchantId) merchantID_1,
      redemptionAddress.cityTown city,
      name merchantname
    FROM
      [big-query-1233:Atom.merchant]
    WHERE
      isPublished = TRUE) a
  LEFT JOIN (
    SELECT
      id DealID_2,
      mappings.merchant.id merchantID_2,
      mappings.chain.id merchantCID,
      mappings.businessAccount.id merchantBAID
    FROM
      FLATTEN(FLATTEN(FLATTEN([big-query-1233:Atom.mapping],mappings.merchant.id),mappings.chain.id),mappings.businessAccount.id)
    WHERE
      type = 'deal' )b
  ON
    a.merchantID_1 = b.merchantID_2)c
LEFT JOIN (
  SELECT
    STRING(a._id) DealID_3,
    CASE
      WHEN o_type = 'RAF' THEN 1'
      ELSE 0
    END is.raffle,
    cat_id,
    CASE
      WHEN deal_type = 'PAL' THEN 'POSTPAID'
      ELSE 'PREPAID'
    END DealType
  FROM (
    SELECT
      _id,
      offers.units.otype o_type
    FROM
      [big-query-1233:Atom.offer])a
  INNER JOIN (
    SELECT
      _id,
      categoryId cat_id,
      units.contractType deal_type,
    FROM
      [big-query-1233:Atom.deal])b
  ON
    a._id = b._id
  GROUP BY
    1,
    2,
    3,
    4)d
ON
  c.DealID_2 = d.DealID_3
GROUP EACH BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9
"
##echo -e "Query: \n $v_query_merchant_table_part1_new";


tableName=merchant_part1
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_merchant_table_part1\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "v_query_merchant_table_part1" &
v_first_pid=$!
v_merch_sdl_pids+=" $v_first_pid"

wait $v_first_pid;



## Rating_source_nb loading. Replace existing
v_query_rating_source_nb="SELECT
 *
FROM (
 SELECT
   date_,
   merchantid,
   dealid,
   source,
   ROUND(AVG(rating),2) avgRating,
 FROM (
   SELECT
     date_,
     a.orderid,
     merchantid,
     dealid,
     rating,
     source
   FROM (
     SELECT
       orderid,
       DATE(MSEC_TO_TIMESTAMP(createdat+19800000)) date_
     FROM
       [big-query-1233:Atom.order_header]) a
   INNER JOIN (
     SELECT
       orderid2,
       merchantid,
       rating,
       dealid,
       source
     FROM
       [big-query-1233:temp.dealratingnb])b
   ON
     a.orderid = b.orderid2)
 GROUP BY
   1,
   2,
   3,
   4)
WHERE
 avgRating IS NOT NULL
AND merchantid not in (SELECT merchantid FROM [big-query-1233:temp.dealratingZT] ) "

##echo -e "Query: \n $v_query_rating_source_nb";

tableName=rating_source_nb
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_rating_source_nb\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_rating_source_nb" &
v_first_pid=$!
v_merch_sdl_pids+=" $!"

wait $v_first_pid;



#Rating_source_zt. Replace existing
v_query_source_zt="SELECT   
       toMemberId merchantid,
       source,
       rating,
       
     FROM
       FLATTEN([big-query-1233:Atom.ratings_and_reviews],RatingContext.vouchers.orderId)
       WHERE source = 'ZOMATO' OR source = 'TRIPADVISOR'
       GROUP BY 1,2,3 "

##echo -e "Query: \n $v_query_rating_source_zt";

tableName=rating_source_zt
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_rating_source_zt\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_rating_source_zt" &
v_first_pid=$!
v_BI_sdl_pids+=" $!"

wait $v_first_pid;


#scheduling merchant_part2. Replace existing
v_query_merchant_table_part2="SELECT
  a.date_1 date,
  D_id deal_id,
  M_id,
  is.Live,
  coalesce(source_nb, source_zt) source,
  coalesce(avgRating_nb,avgRating_zt) avgRating
FROM
(SELECT
 reporting_date date_1,
 dealid_1 D_id,
 a.MerchantID_4 M_id,
 is.Live,
 source_nb,
 avgRating avgRating_nb
 FROM (SELECT
    reporting_date,
    STRING(Deal_ID) dealid_1,
    Merchant_ID MerchantID_4,
    CASE WHEN Outlet_live_status = 'Live Outlet' THEN 1 ELSE 0 END is.Live
  FROM
    [big-query-1233:nb_reports.outlets_open_with_deals]
--   WHERE
--     Outlet_live_status = 'Live Outlet'
    )a
    LEFT JOIN 
    (SELECT
    date_,
    merchantid,
    source source_nb,
    avgRating
    FROM [big-query-1233:project_product_metrics.rating_source_nb]
    )b
    ON 
     a.reporting_date = b.date_ 
     AND a.MerchantID_4 = b.merchantid) a
    LEFT JOIN
    (SELECT 
    merchantid,
    source source_zt,
    rating avgRating_zt
    FROM [big-query-1233:project_product_metrics.rating_source_zt]
    )b
    ON 
    a.M_id = b.merchantid
GROUP BY  1,2,3,4,5,6 "

##echo -e "Query: \n $v_query_merchant_table_part2_new";

tableName=merchant_part2
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_merchant_table_part2\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_merchant_table_part2" &
v_first_pid=$!
v_merch_sdl_pids+=" $!"

wait $v_first_pid;


#merchant_table_final loading. Replacing existing

v_query_merchant_final="SELECT date,
  a.Merchant_id	 merchantid,
  merchantname,
  city merchant_city,
  merchantCID merchant_chainID,
  merchantBAID merchant_BAID,
  is_Live,
  a.DealID_j2 dealid,
  cat_id deal_categoryID,
  DealType,
  is_raffle,
  avgRating,
  source
  FROM(
  SELECT
    *
  FROM
    [big-query-1233:project_product_metrics.merchant_part1])a
INNER JOIN (
  SELECT
    date, deal_id, STRING(M_id) Merchant_ID, source, avgRating, is_Live
  FROM
    [big-query-1233:project_product_metrics.merchant_part2])b
ON 
a.Merchant_id	= b.Merchant_ID	
AND 
a.DealID_j2 = b.deal_id "

##echo -e "Query: \n $v_query_merchant_final;

tableName=merchant_final
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_merchant_final\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_merchant_final" &
v_first_pid=$!
v_merch_sdl_pids+=" $!"

wait $v_first_pid;




if wait $v_merch_sdl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"

if wait $v_merch_sdl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Table refresh status of merchant_table_part1, table with rating source as nearbuy, table with rating source zt, merchant_table_part2, merchant_table_final in project_product_metrics dataset:$v_table_status`date`" | mail -s "$v_table_status" saurabh.deosarkar@nearbuy

exit 0






