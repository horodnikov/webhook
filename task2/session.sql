/*
Number sessions for users (display a column next to each event indicating the session number to which the event belongs). 
An end of a session is considered when there is no user activity for more than 45 minutes (strict inequality). 
Users from different projects are treated as separate users. Start session numbering from 1
*/

/*
Source data
*/
DROP TABLE IF EXISTS table_tmp
;
CREATE TEMPORARY TABLE table_tmp (
    user_id INTEGER,
    project_id INTEGER,
    event_time TIMESTAMP
);

INSERT INTO table_tmp
VALUES (1, 1, '2019-12-01 00:00:00'),
               (1, 2, '2019-12-01 00:01:00'),
               (1, 1, '2019-12-01 00:10:41'),
               (1, 1, '2019-12-01 00:15:33'),
               (1, 2, '2019-12-01 00:46:00'),
               (1, 1, '2019-12-01 10:05:00'),
               (1, 1, '2019-12-01 10:35:00'),
               (1, 1, '2019-12-01 15:00:00'),
               (2, 1, '2019-12-01 15:00:00');

SELECT
  *,
  COUNT(session_start) OVER (w) AS session_number
FROM
  (
    SELECT
      *,
      CASE
        WHEN RANK() OVER (w) = 1 THEN 1
        ELSE
          CASE
            WHEN (EXTRACT(
                    EPOCH FROM table_tmp.event_time -
                    LAG(table_tmp.event_time) OVER (w)) / 60) > 45
            THEN 1 ELSE NULL
          END
      END AS session_start
    FROM
      table_tmp
      window w AS (PARTITION BY user_id, project_id ORDER BY event_time)
  ) AS result
  window w AS (PARTITION BY user_id, project_id ORDER BY event_time)

/*
Expected result

+-------+----------+--------------------------+-------------+--------------+
|user_id|project_id|event_time                |session_start|session_number|
+-------+----------+--------------------------+-------------+--------------+
|1      |1         |2019-12-01 00:00:00.000000|1            |1             |
|1      |1         |2019-12-01 00:10:41.000000|NULL         |1             |
|1      |1         |2019-12-01 00:15:33.000000|NULL         |1             |
|1      |1         |2019-12-01 10:05:00.000000|1            |2             |
|1      |1         |2019-12-01 10:35:00.000000|NULL         |2             |
|1      |1         |2019-12-01 15:00:00.000000|1            |3             |
|2      |1         |2019-12-01 15:00:00.000000|1            |1             |
|1      |2         |2019-12-01 00:01:00.000000|1            |1             |
|1      |2         |2019-12-01 00:46:00.000000|NULL         |1             |
+-------+----------+--------------------------+-------------+--------------+

*/