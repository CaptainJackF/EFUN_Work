-- 活动前
SELECT 
    "0718" as Cate, c.depth, c.sourceid, avg( c.Num) as Num
FROM 
(
    SELECT 
        a.server_id, a.user_id, a.sourceid, b.depth, sum( a.Num) as Num
    FROM
    (
        SELECT 
            server_id, user_id, sourceid, MAX( number) as Num
        FROM tb_log_resourceaccess
        WHERE CreateTime BETWEEN '2018-07-18 00:00:00' AND '2018-07-18 23:59:59'
        GROUP BY 
        	server_id, user_id, sourceid
    ) a 
    LEFT JOIN
    (
        SELECT
			server_id, user_id,
        CASE
            WHEN sum(round(cash, 2)) > 0 AND sum(round(cash, 2)) <= 10 THEN '1.小小R'
            WHEN sum(round(cash, 2)) > 10 AND sum(round(cash, 2)) <= 100 THEN '2.小R'
            WHEN sum(round(cash, 2)) > 100 AND sum(round(cash, 2)) <= 500 THEN '3.中R'
            WHEN sum(round(cash, 2)) > 500 AND sum(round(cash, 2)) <= 1000 THEN '4.大R'
            WHEN sum(round(cash, 2)) > 1000 THEN '5.超R'
        END AS depth
        FROM
            tb_log_recharge
        WHERE
            createtime BETWEEN '2018-06-18 00:00:00' AND '2018-07-17 23:59:59'
        GROUP BY
            server_id, user_id
    ) b
    ON a.server_id = b.server_id AND a.user_id = b.user_id
    GROUP BY a.server_id, a.user_id, a.sourceid, b.depth
) c
GROUP BY c.depth, c.sourceid


-- 活动前
SELECT 
    "0726" as Cate, c.depth, c.sourceid, avg( c.Num) as Num
FROM 
(
    SELECT 
        a.server_id, a.user_id, a.sourceid, b.depth, sum( a.Num) as Num
    FROM
    (
        SELECT 
            server_id, user_id, sourceid, MAX( number) as Num
        FROM tb_log_resourceaccess
        WHERE CreateTime BETWEEN '2018-07-26 00:00:00' AND '2018-07-26 23:59:59'
        GROUP BY 
        	server_id, user_id, sourceid
    ) a 
    LEFT JOIN
    (
        SELECT
			server_id, user_id,
        CASE
            WHEN sum(round(cash, 2)) > 0 AND sum(round(cash, 2)) <= 10 THEN '1.小小R'
            WHEN sum(round(cash, 2)) > 10 AND sum(round(cash, 2)) <= 100 THEN '2.小R'
            WHEN sum(round(cash, 2)) > 100 AND sum(round(cash, 2)) <= 500 THEN '3.中R'
            WHEN sum(round(cash, 2)) > 500 AND sum(round(cash, 2)) <= 1000 THEN '4.大R'
            WHEN sum(round(cash, 2)) > 1000 THEN '5.超R'
        END AS depth
        FROM
            tb_log_recharge
        WHERE
            createtime BETWEEN '2018-06-18 00:00:00' AND '2018-07-17 23:59:59'
        GROUP BY
            server_id, user_id
    ) b
    ON a.server_id = b.server_id AND a.user_id = b.user_id
    GROUP BY a.server_id, a.user_id, a.sourceid, b.depth
) c
GROUP BY c.depth, c.sourceid


-- 活动前
SELECT 
    c.depth, c.itemid, c.itemname, avg( c.Num) as Num
FROM 
(
    SELECT 
        a.server_id, a.user_id, a.itemid, a.itemname, b.depth, sum( a.Num) as Num
    FROM
    (
        SELECT 
            server_id, user_id, itemid, itemname, MAX( number) as Num
        FROM tb_log_item
        WHERE CreateTime BETWEEN '2018-07-18 00:00:00' AND '2018-07-18 23:59:59'
        AND itemid IN 
        (
        	10003, 10023, 10043, 10103, 10064, 10201, 10212, 10213, 20006, 30003, 30011, 30019, 30027, 30035, 30043, 31019, 
        	10005, 10025, 10045, 10105, 10066, 10112, 10204, 30005, 30013, 30021, 30029, 30037, 30045, 31021,
        	30014, 30030, 30038, 30046, 20007
        )
        GROUP BY 
        	server_id, user_id, itemid, itemname
    ) a 
    LEFT JOIN
    (
        SELECT
			server_id, user_id,
        CASE
            WHEN sum(round(cash, 2)) > 0 AND sum(round(cash, 2)) <= 10 THEN '1.小小R'
            WHEN sum(round(cash, 2)) > 10 AND sum(round(cash, 2)) <= 100 THEN '2.小R'
            WHEN sum(round(cash, 2)) > 100 AND sum(round(cash, 2)) <= 500 THEN '3.中R'
            WHEN sum(round(cash, 2)) > 500 AND sum(round(cash, 2)) <= 1000 THEN '4.大R'
            WHEN sum(round(cash, 2)) > 1000 THEN '5.超R'
        END AS depth
        FROM
            tb_log_recharge
        WHERE
            createtime BETWEEN '2018-06-18 00:00:00' AND '2018-07-17 23:59:59'
        GROUP BY
            server_id, user_id
    ) b
    ON a.server_id = b.server_id AND a.user_id = b.user_id
    GROUP BY a.server_id, a.user_id, a.itemid, a.itemname, b.depth
) c
GROUP BY c.depth, c.itemid, c.itemname


-- 活动前
SELECT 
    c.depth, c.itemid, c.itemname, avg( c.Num) as Num
FROM 
(
    SELECT 
        a.server_id, a.user_id, a.itemid, a.itemname, b.depth, sum( a.Num) as Num
    FROM
    (
        SELECT 
            server_id, user_id, itemid, itemname, MAX( number) as Num
        FROM tb_log_item
        WHERE CreateTime BETWEEN '2018-07-26 00:00:00' AND '2018-07-26 23:59:59'
        AND itemid IN 
        (
        	10003, 10023, 10043, 10103, 10064, 10201, 10212, 10213, 20006, 30003, 30011, 30019, 30027, 30035, 30043, 31019, 
        	10005, 10025, 10045, 10105, 10066, 10112, 10204, 30005, 30013, 30021, 30029, 30037, 30045, 31021,
        	30014, 30030, 30038, 30046, 20007
        )
        GROUP BY 
        	server_id, user_id, itemid, itemname
    ) a 
    LEFT JOIN
    (
        SELECT
			server_id, user_id,
        CASE
            WHEN sum(round(cash, 2)) > 0 AND sum(round(cash, 2)) <= 10 THEN '1.小小R'
            WHEN sum(round(cash, 2)) > 10 AND sum(round(cash, 2)) <= 100 THEN '2.小R'
            WHEN sum(round(cash, 2)) > 100 AND sum(round(cash, 2)) <= 500 THEN '3.中R'
            WHEN sum(round(cash, 2)) > 500 AND sum(round(cash, 2)) <= 1000 THEN '4.大R'
            WHEN sum(round(cash, 2)) > 1000 THEN '5.超R'
        END AS depth
        FROM
            tb_log_recharge
        WHERE
            createtime BETWEEN '2018-06-18 00:00:00' AND '2018-07-17 23:59:59'
        GROUP BY
            server_id, user_id
    ) b
    ON a.server_id = b.server_id AND a.user_id = b.user_id
    GROUP BY a.server_id, a.user_id, a.itemid, a.itemname, b.depth
) c
GROUP BY c.depth, c.itemid, c.itemname


-- 0718-0726活动期间消耗
SELECT 
    c.depth, c.itemid, c.itemname, avg( c.Num) as Num
FROM 
(
    SELECT 
        a.server_id, a.user_id, a.itemid, a.itemname, b.depth, sum( a.Num) as Num
    FROM
    (
        SELECT 
            server_id, user_id, itemid, itemname, SUM( number) as Num
        FROM tb_log_item
        WHERE CreateTime BETWEEN '2018-07-19 00:00:00' AND '2018-07-25 23:59:59'
        AND itemid IN 
        (
        	10003, 10023, 10043, 10103, 10064, 10201, 10212, 10213, 20006, 30003, 30011, 30019, 30027, 30035, 30043, 31019, 
        	10005, 10025, 10045, 10105, 10066, 10112, 10204, 30005, 30013, 30021, 30029, 30037, 30045, 31021,
        	30014, 30030, 30038, 30046, 20007
        )
        GROUP BY 
        	server_id, user_id, itemid, itemname
    ) a 
    LEFT JOIN
    (
        SELECT
			server_id, user_id,
        CASE
            WHEN sum(round(cash, 2)) > 0 AND sum(round(cash, 2)) <= 10 THEN '1.小小R'
            WHEN sum(round(cash, 2)) > 10 AND sum(round(cash, 2)) <= 100 THEN '2.小R'
            WHEN sum(round(cash, 2)) > 100 AND sum(round(cash, 2)) <= 500 THEN '3.中R'
            WHEN sum(round(cash, 2)) > 500 AND sum(round(cash, 2)) <= 1000 THEN '4.大R'
            WHEN sum(round(cash, 2)) > 1000 THEN '5.超R'
        END AS depth
        FROM
            tb_log_recharge
        WHERE
            createtime BETWEEN '2018-06-18 00:00:00' AND '2018-07-17 23:59:59'
        GROUP BY
            server_id, user_id
    ) b
    ON a.server_id = b.server_id AND a.user_id = b.user_id
    GROUP BY a.server_id, a.user_id, a.itemid, a.itemname, b.depth
) c
GROUP BY c.depth, c.itemid, c.itemname
