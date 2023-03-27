WITH lags AS (SELECT dt, --достаем значения предыдущего действия
                     user_id,
                     event_type,
                     event_props,
                     LAG(event_type) OVER (PARTITION BY user_id ORDER BY dt)  AS prev_event_type,
                     LAG(event_props) OVER (PARTITION BY user_id ORDER BY dt) AS prev_event_props,
                     LAG(dt) OVER (PARTITION BY user_id ORDER BY dt)          AS prev_dt
              FROM events),
     new_session AS (SELECT dt, -- размечаем признаки начала новой сессии
                            user_id,
                            CASE
                                WHEN prev_event_type = 'offline' THEN 1
                                WHEN prev_event_type = 'action' AND dt - prev_dt > INTERVAL 15 MINUTE THEN 1
                                WHEN prev_event_type = 'meeting'
                                         AND dt - prev_dt - INTERVAL prev_event_props SECOND > INTERVAL 15 MINUTE
                                    THEN 1
                                ELSE 0
                                END AS is_new_session,
                            CASE
                                WHEN event_type = 'meeting' THEN dt + INTERVAL event_props SECOND
                                ELSE dt
                                END AS prob_session_finish
                     FROM lags),
     add_session_id AS (SELECT dt, -- размечаем id сессии для всех действий
                               user_id,
                               prob_session_finish,
                               SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY dt) AS session_id
                        FROM new_session),
     sessions AS (SELECT user_id, -- находим дату начала сессии и окончания
                         MIN(dt)                  AS dt_start,
                         MAX(prob_session_finish) AS session_finish
                  FROM add_session_id
                  GROUP BY user_id, session_id)
SELECT user_id, --правим дату окончания сессии, если 15-ти минутное окно сессии не закрылось
       dt_start,
       CASE
           WHEN NOW() - session_finish < INTERVAL 15 MINUTE THEN NULL
           ELSE session_finish
           END AS dt_end
FROM sessions
