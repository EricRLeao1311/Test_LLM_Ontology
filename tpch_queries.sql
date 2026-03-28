SELECT 
    l_returnflag, 
    l_linestatus, 
    SUM(l_quantity) AS sum_qty, 
    SUM(l_extendedprice) AS sum_base_price, 
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, 
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, 
    AVG(l_quantity) AS avg_qty, 
    AVG(l_extendedprice) AS avg_price, 
    AVG(l_discount) AS avg_disc, 
    COUNT(*) AS count_order
FROM 
    lineitem
WHERE 
    l_shipdate <= DATE '1995-02-13' - INTERVAL '90 days'
GROUP BY 
    l_returnflag, 
    l_linestatus
ORDER BY 
    l_returnflag, 
    l_linestatus;

SELECT 
    S_ACCTBAL, 
    S_NAME, 
    N_NAME, 
    P_PARTKEY, 
    P_MFGR, 
    S_ADDRESS, 
    S_PHONE, 
    S_COMMENT
FROM 
    PART, 
    SUPPLIER, 
    PARTSUPP, 
    NATION, 
    REGION
WHERE 
    P_PARTKEY = PS_PARTKEY 
    AND S_SUPPKEY = PS_SUPPKEY 
    AND P_SIZE = 7 
    AND P_TYPE LIKE '%ECONOMY ANODIZED STEEL' 
    AND S_NATIONKEY = N_NATIONKEY 
    AND N_REGIONKEY = R_REGIONKEY 
    AND R_NAME = 'EUROPE' 
    AND PS_SUPPLYCOST = (
        SELECT MIN(PS_SUPPLYCOST) 
        FROM PARTSUPP AS ps2
        JOIN SUPPLIER AS s2 ON s2.S_SUPPKEY = ps2.PS_SUPPKEY
        JOIN NATION AS n2 ON s2.S_NATIONKEY = n2.N_NATIONKEY
        JOIN REGION AS r2 ON n2.N_REGIONKEY = r2.R_REGIONKEY
        WHERE ps2.PS_PARTKEY = PART.P_PARTKEY
          AND r2.R_NAME = 'EUROPE'
    )
ORDER BY 
    S_ACCTBAL DESC, 
    N_NAME, 
    S_NAME, 
    P_PARTKEY
LIMIT 100;

SELECT 
    l_orderkey, 
    SUM(l_extendedprice * (1 - l_discount)) AS revenue, 
    o_orderdate, 
    o_shippriority
FROM 
    customer, 
    orders, 
    lineitem
WHERE 
    c_mktsegment = 'HOUSEHOLD' 
    AND c_custkey = o_custkey 
    AND l_orderkey = o_orderkey 
    AND o_orderdate < DATE '1992-07-30'
    AND l_shipdate > DATE '1998-05-04'
GROUP BY 
    l_orderkey, 
    o_orderdate, 
    o_shippriority
ORDER BY 
    revenue DESC, 
    o_orderdate
LIMIT 20;

SELECT 
    n_name, 
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM 
    customer, 
    orders, 
    lineitem, 
    supplier, 
    nation, 
    region
WHERE 
    c_custkey = o_custkey 
    AND l_orderkey = o_orderkey 
    AND l_suppkey = s_suppkey 
    AND c_nationkey = s_nationkey 
    AND s_nationkey = n_nationkey 
    AND n_regionkey = r_regionkey 
    AND r_name = 'ASIA' 
    AND o_orderdate >= DATE '1996-05-31'
    AND o_orderdate < DATE '1996-05-31' + INTERVAL '1 year'
GROUP BY 
    n_name
ORDER BY 
    revenue DESC;

SELECT 
    SUM(l_extendedprice * l_discount) AS revenue
FROM 
    lineitem
WHERE 
    l_shipdate >= DATE '1997-06-11'
    AND l_shipdate < DATE '1997-06-11' + INTERVAL '1 year'
    AND l_discount BETWEEN -0.95 AND 1.02
    AND l_quantity < 21;

SELECT 
    NA_N_NAME, 
    NB_N_NAME, 
    L_YEAR, 
    SUM(VOLUME) AS REVENUE
FROM 
    (
        SELECT 
            NA.N_NAME AS NA_N_NAME, 
            NB.N_NAME AS NB_N_NAME, 
            EXTRACT(YEAR FROM L_SHIPDATE) AS L_YEAR, 
            L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME
        FROM 
            SUPPLIER, 
            LINEITEM, 
            ORDERS, 
            CUSTOMER, 
            NATION NA, 
            NATION NB
        WHERE 
            S_SUPPKEY = L_SUPPKEY 
            AND O_ORDERKEY = L_ORDERKEY 
            AND C_CUSTKEY = O_CUSTKEY 
            AND S_NATIONKEY = NA.N_NATIONKEY 
            AND C_NATIONKEY = NB.N_NATIONKEY 
            AND (
                (NA.N_NAME = 'EUROPE' AND NB.N_NAME = 'UNITED STATES') 
                OR (NA.N_NAME = 'UNITED STATES' AND NB.N_NAME = 'EUROPE')
            ) 
            AND L_SHIPDATE BETWEEN DATE '1992-05-09'
                               AND DATE '1992-05-09' + INTERVAL '1 year'
    ) AS SHIPPING
GROUP BY 
    NA_N_NAME, 
    NB_N_NAME, 
    L_YEAR
ORDER BY 
    NA_N_NAME, 
    NB_N_NAME, 
    L_YEAR;

SELECT 
    O_YEAR, 
    SUM(CASE WHEN NB_N_NAME = 'BRAZIL' THEN VOLUME ELSE 0 END) / SUM(VOLUME) AS MKT_SHARE
FROM 
    (
        SELECT 
            EXTRACT(YEAR FROM O_ORDERDATE) AS O_YEAR, 
            L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME, 
            NB.N_NAME AS NB_N_NAME
        FROM 
            PART, 
            SUPPLIER, 
            LINEITEM, 
            ORDERS, 
            CUSTOMER, 
            NATION NA, 
            NATION NB, 
            REGION
        WHERE 
            P_PARTKEY = L_PARTKEY 
            AND S_SUPPKEY = L_SUPPKEY 
            AND L_ORDERKEY = O_ORDERKEY 
            AND O_CUSTKEY = C_CUSTKEY 
            AND C_NATIONKEY = NA.N_NATIONKEY 
            AND NA.N_REGIONKEY = R_REGIONKEY 
            AND R_NAME = 'AFRICA' 
            AND S_NATIONKEY = NB.N_NATIONKEY 
            AND O_ORDERDATE BETWEEN DATE '1993-12-27'
                                AND DATE '1993-12-27' + INTERVAL '1 year'
            AND P_TYPE = 'LARGE PLATED NICKEL'
    ) AS ALL_NATIONS
GROUP BY 
    O_YEAR
ORDER BY 
    O_YEAR;

SELECT 
    NATION, 
    O_YEAR, 
    SUM(AMOUNT) AS SUM_PROFIT
FROM 
    (
        SELECT 
            N_NAME AS NATION, 
            EXTRACT(YEAR FROM O_ORDERDATE) AS O_YEAR, 
            L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY AS AMOUNT
        FROM 
            PART, 
            SUPPLIER, 
            LINEITEM, 
            PARTSUPP, 
            ORDERS, 
            NATION
        WHERE 
            S_SUPPKEY = L_SUPPKEY 
            AND PS_SUPPKEY = L_SUPPKEY 
            AND PS_PARTKEY = L_PARTKEY 
            AND P_PARTKEY = L_PARTKEY 
            AND O_ORDERKEY = L_ORDERKEY 
            AND S_NATIONKEY = N_NATIONKEY 
            AND P_NAME LIKE '%pale%'
    ) AS PROFIT
GROUP BY 
    NATION, 
    O_YEAR
ORDER BY 
    NATION, 
    O_YEAR DESC;

SELECT 
    C_CUSTKEY, 
    C_NAME, 
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE, 
    C_ACCTBAL, 
    N_NAME, 
    C_ADDRESS, 
    C_PHONE, 
    C_COMMENT
FROM 
    CUSTOMER, 
    ORDERS, 
    LINEITEM, 
    NATION
WHERE 
    C_CUSTKEY = O_CUSTKEY 
    AND L_ORDERKEY = O_ORDERKEY 
    AND O_ORDERDATE >= DATE '1995-02-14'
    AND O_ORDERDATE < DATE '1995-02-14' + INTERVAL '3 months'
    AND L_RETURNFLAG = 'R' 
    AND C_NATIONKEY = N_NATIONKEY
GROUP BY 
    C_CUSTKEY, 
    C_NAME, 
    C_ACCTBAL, 
    C_PHONE, 
    N_NAME, 
    C_ADDRESS, 
    C_COMMENT
ORDER BY 
    REVENUE DESC
LIMIT 20;

SELECT 
    PS_PARTKEY, 
    SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS VALUE
FROM 
    PARTSUPP, 
    SUPPLIER, 
    NATION
WHERE 
    PS_SUPPKEY = S_SUPPKEY 
    AND S_NATIONKEY = N_NATIONKEY 
    AND N_NAME = 'INDIA'
GROUP BY 
    PS_PARTKEY
HAVING 
    SUM(PS_SUPPLYCOST * PS_AVAILQTY) > (
        SELECT 
            SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001000000
        FROM 
            PARTSUPP, 
            SUPPLIER, 
            NATION
        WHERE 
            PS_SUPPKEY = S_SUPPKEY 
            AND S_NATIONKEY = N_NATIONKEY 
            AND N_NAME = 'UNITED STATES'
    )
ORDER BY 
    VALUE DESC;

SELECT 
    l_shipmode, 
    SUM(CASE WHEN o_orderpriority = '1-urgent' OR o_orderpriority = '2-high' THEN 1 ELSE 0 END) AS high_line_count, 
    SUM(CASE WHEN o_orderpriority <> '3-medium' AND o_orderpriority <> '4-not specified' THEN 1 ELSE 0 END) AS low_line_count
FROM 
    orders, 
    lineitem
WHERE 
    o_orderkey = l_orderkey 
    AND l_shipmode IN ('SHIP', 'MAIL') 
    AND l_commitdate < l_receiptdate 
    AND l_shipdate < l_commitdate 
    AND l_receiptdate >= DATE '1997-02-03'
    AND l_receiptdate < DATE '1997-02-03' + INTERVAL '1 year'
GROUP BY 
    l_shipmode
ORDER BY 
    l_shipmode;

SELECT 
    C_COUNT, 
    COUNT(*) AS CUSTDIST
FROM 
    (
        SELECT 
            C_CUSTKEY, 
            COUNT(O_ORDERKEY) AS C_COUNT
        FROM 
            CUSTOMER 
        LEFT JOIN 
            ORDERS 
        ON 
            C_CUSTKEY = O_CUSTKEY 
            AND O_COMMENT NOT LIKE '%requests%'
        GROUP BY 
            C_CUSTKEY
    ) AS C_ORDERS
GROUP BY 
    C_COUNT
ORDER BY 
    CUSTDIST DESC, 
    C_COUNT DESC;

SELECT 
    100.00 * SUM(
        CASE 
            WHEN p_type LIKE 'PROMO%' 
                THEN l_extendedprice * (1 - l_discount) 
            ELSE 0 
        END
    ) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM 
    lineitem, 
    part
WHERE 
    l_partkey = p_partkey 
    AND l_shipdate >= DATE '1998-09-18'
    AND l_shipdate < DATE '1998-09-18' + INTERVAL '1 month';

SELECT 
    l_suppkey, 
    SUM(l_extendedprice * (1 - l_discount)) AS sum_price
FROM 
    lineitem
WHERE 
    l_shipdate >= DATE '1994-11-30'
    AND l_shipdate < DATE '1994-11-30' + INTERVAL '3 months'
GROUP BY 
    l_suppkey;

SELECT 
    p_brand, 
    p_type, 
    p_size, 
    COUNT(DISTINCT ps_suppkey) AS supp_cnt
FROM 
    partsupp, 
    part
WHERE 
    p_partkey = ps_partkey 
    AND p_brand <> 'Brand#23' 
    AND LOWER(p_type) LIKE '%small brushed brass%' 
    AND p_size IN (8, 42, 33, 21, 7, 46, 23, 3) 
    AND ps_suppkey NOT IN (
        SELECT s_suppkey 
        FROM supplier 
        WHERE LOWER(s_comment) LIKE '%carefully%'
    )
GROUP BY 
    p_brand, 
    p_type, 
    p_size
ORDER BY 
    supp_cnt DESC, 
    p_brand, 
    p_type, 
    p_size;

SELECT 
    SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
FROM 
    LINEITEM, 
    PART
WHERE 
    P_PARTKEY = L_PARTKEY 
    AND P_BRAND = 'Brand#52' 
    AND P_CONTAINER = 'WRAP PACK' 
    AND L_QUANTITY < (
        SELECT 0.2 * AVG(L_QUANTITY) 
        FROM LINEITEM 
        WHERE L_PARTKEY = PART.P_PARTKEY
    );

SELECT 
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM 
    lineitem, 
    part
WHERE 
    (
        p_partkey = l_partkey 
        AND p_brand = 'Brand#25' 
        AND p_container IN ('MED PACK', 'SM BAG', 'WRAP PKG', 'MED CAN') 
        AND l_quantity BETWEEN 20 AND 38 
        AND p_size BETWEEN 32 AND 28 
        AND l_shipmode IN ('AIR', 'AIR') 
        AND l_shipinstruct = 'TAKE BACK RETURN'
    )
    OR (
        p_partkey = l_partkey 
        AND p_brand = 'Brand#43' 
        AND p_container IN ('JUMBO BOX', 'WRAP PKG', 'LG CAN', 'LG PACK') 
        AND l_quantity BETWEEN 25 AND 53 
        AND p_size BETWEEN 21 AND 18 
        AND l_shipmode IN ('SHIP', 'MAIL') 
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR (
        p_partkey = l_partkey 
        AND p_brand = 'Brand#44' 
        AND p_container IN ('SM PKG', 'LG CAN', 'MED JAR', 'JUMBO PKG') 
        AND l_quantity BETWEEN 40 AND 44 
        AND p_size BETWEEN 8 AND 62 
        AND l_shipmode IN ('REG AIR', 'SHIP') 
        AND l_shipinstruct = 'NONE'
    );

SELECT 
    S_NAME, 
    S_ADDRESS
FROM 
    SUPPLIER, 
    NATION
WHERE 
    S_SUPPKEY IN (
        SELECT 
            PS_SUPPKEY 
        FROM 
            PARTSUPP
        WHERE 
            PS_PARTKEY IN (
                SELECT 
                    P_PARTKEY 
                FROM 
                    PART 
                WHERE 
                    P_NAME LIKE '%cornsilk%'
            ) 
            AND PS_AVAILQTY > (
                SELECT 
                    0.5 * SUM(L_QUANTITY) AS sum_quantity
                FROM 
                    LINEITEM
                WHERE 
                    L_PARTKEY = PS_PARTKEY 
                    AND L_SUPPKEY = PS_SUPPKEY 
                    AND L_SHIPDATE >= DATE '1997-12-04'
                    AND L_SHIPDATE < DATE '1997-12-04' + INTERVAL '1 year'
            )
    ) 
    AND S_NATIONKEY = N_NATIONKEY 
    AND N_NAME = 'UNITED STATES'
ORDER BY 
    S_NAME;

SELECT 
    S_NAME, 
    COUNT(*) AS NUMWAIT
FROM 
    SUPPLIER, 
    LINEITEM AS LA, 
    ORDERS, 
    NATION
WHERE 
    S_SUPPKEY = LA.L_SUPPKEY 
    AND O_ORDERKEY = LA.L_ORDERKEY 
    AND O_ORDERSTATUS = 'F' 
    AND LA.L_RECEIPTDATE > LA.L_COMMITDATE 
    AND EXISTS (
        SELECT 1
        FROM LINEITEM AS LB 
        WHERE LB.L_ORDERKEY = LA.L_ORDERKEY 
          AND LB.L_SUPPKEY <> LA.L_SUPPKEY
    ) 
    AND NOT EXISTS (
        SELECT 1
        FROM LINEITEM AS LC 
        WHERE LC.L_ORDERKEY = LA.L_ORDERKEY 
          AND LC.L_SUPPKEY <> LA.L_SUPPKEY 
          AND LC.L_RECEIPTDATE > LC.L_COMMITDATE
    ) 
    AND S_NATIONKEY = N_NATIONKEY 
    AND N_NAME = 'FRANCE'
GROUP BY 
    S_NAME
ORDER BY 
    NUMWAIT DESC, 
    S_NAME
LIMIT 100;


SELECT 
    cntrycode, 
    COUNT(*) AS numcust, 
    SUM(c_acctbal) AS totacctbal
FROM 
    (
        SELECT 
            substr(c_phone, 1, 2) AS cntrycode, 
            c_acctbal
        FROM 
            customer
        WHERE 
            substr(c_phone, 1, 2) IN ('32', '11', '29', '25', '22', '15', '12') 
            AND c_acctbal > (
                SELECT 
                    AVG(c_acctbal) 
                FROM 
                    customer
                WHERE 
                    c_acctbal > 0.00 
                    AND substr(c_phone, 1, 2) IN ('29', '26', '29', '22', '12', '28', '14')
            ) 
            AND NOT EXISTS (
                SELECT 1
                FROM orders 
                WHERE o_custkey = c_custkey
            )
    ) AS custsale
GROUP BY 
    cntrycode
ORDER BY 
    cntrycode;


SELECT 
    o_orderpriority, 
    COUNT(*) AS order_count
FROM 
    orders
WHERE 
    o_orderdate >= DATE '1993-06-17'
    AND o_orderdate < DATE '1993-06-17' + INTERVAL '3 months'
    AND EXISTS (
        SELECT 1 
        FROM lineitem 
        WHERE l_orderkey = o_orderkey 
          AND l_commitdate < l_receiptdate
    )
GROUP BY 
    o_orderpriority
ORDER BY 
    o_orderpriority;
