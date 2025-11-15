-- learning list:
-- range join technique
-- generate_series combining with subquery
-- subquery getting a scalar
-- date_trunc and to_char dealing with TIMESTAMP 
-- coalesce dealing with NULL


DROP TABLE IF EXISTS trades;

CREATE TABLE trades (
    id SERIAL PRIMARY KEY,
    createdtime TIMESTAMPTZ NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    volume INT NOT NULL
);

INSERT INTO trades (createdtime, price, volume) VALUES
('2025-01-01 10:03:25 UTC', 30.10, 120),
('2025-01-01 10:02:30 UTC', 31.20, 110),
('2025-01-01 10:02:00 UTC', 29.50, 100),
('2025-01-01 10:03:00 UTC', 30.30, 130),
('2025-01-01 10:04:00 UTC', 32.10, 125),
('2025-01-01 10:05:00 UTC', 28.40, 140),
('2025-01-01 10:06:00 UTC', 30.90, 150),
('2025-01-01 10:07:00 UTC', 29.80, 160),
('2025-01-01 10:08:00 UTC', 30.10, 170),
('2025-01-01 10:09:00 UTC', 31.40, 140),

('2025-01-01 10:10:00 UTC', 32.10, 155),
('2025-01-01 10:11:00 UTC', 30.20, 165),
('2025-01-01 10:12:00 UTC', 29.90, 175),
('2025-01-01 10:13:00 UTC', 31.00, 185),
('2025-01-01 10:14:00 UTC', 30.80, 195),
('2025-01-01 10:15:00 UTC', 29.70, 125),
('2025-01-01 10:16:00 UTC', 28.90, 115),
('2025-01-01 10:17:00 UTC', 30.50, 110),
('2025-01-01 10:18:00 UTC', 31.20, 105),
('2025-01-01 10:19:00 UTC', 32.40, 100),

('2025-01-01 10:20:00 UTC', 33.10, 90),
('2025-01-01 10:21:00 UTC', 31.80, 95),
('2025-01-01 10:22:00 UTC', 30.50, 100),
('2025-01-01 10:23:00 UTC', 29.20, 110),
('2025-01-01 10:24:00 UTC', 30.50, 115),
('2025-01-01 10:25:00 UTC', 31.10, 120),
('2025-01-01 10:26:00 UTC', 32.00, 130),
('2025-01-01 10:27:00 UTC', 30.40, 135),
('2025-01-01 10:28:00 UTC', 29.80, 128),
('2025-01-01 10:29:00 UTC', 31.90, 140),

('2025-01-01 10:30:00 UTC', 33.50, 150),
('2025-01-01 10:31:00 UTC', 32.20, 155),
('2025-01-01 10:32:00 UTC', 30.10, 160),
('2025-01-01 10:33:00 UTC', 29.70, 170),
('2025-01-01 10:34:00 UTC', 30.20, 165),
('2025-01-01 10:35:00 UTC', 31.10, 155),
('2025-01-01 10:36:00 UTC', 32.60, 145),
('2025-01-01 10:37:00 UTC', 33.00, 130),
('2025-01-01 10:38:00 UTC', 32.20, 150),
('2025-01-01 10:39:00 UTC', 31.40, 160),

('2025-01-01 10:40:00 UTC', 30.90, 170),
('2025-01-01 10:41:00 UTC', 29.80, 160),
('2025-01-01 10:42:00 UTC', 28.90, 150),
('2025-01-01 10:43:00 UTC', 30.20, 140),
('2025-01-01 10:44:00 UTC', 31.10, 135),
('2025-01-01 10:45:00 UTC', 32.50, 130),
('2025-01-01 10:46:00 UTC', 33.20, 125),
('2025-01-01 10:47:00 UTC', 34.10, 120),
('2025-01-01 10:48:00 UTC', 33.80, 118),
('2025-01-01 10:49:00 UTC', 32.90, 115),
('2025-01-01 10:50:00 UTC', 31.70, 110);

with time_window as(   -- cte1
    select generate_series(
        (select date_trunc('hour', min(createdtime)) from trades),
        ((select date_trunc('hour', max(createdtime)) from trades)+ interval '1 hour'),
        interval '5 minutes'
    ) as start_time
), 
core_logic as( -- cte2
    select 
    tw.start_time as start_time,
    tw.start_time + interval '15 minutes' as end_time,
    sum(t.volume) as total
from time_window tw
left join trades t
    on tw.start_time <= t.createdtime 
    and tw.start_time + interval '15 minutes' > t.createdtime
group by tw.start_time
), 
format_polished as ( -- cte3 result
    select 
        to_char(start_time, 'YYYY-MM-DD HH24:MI') as start_time,
        to_char(end_time, 'YYYY-MM-DD HH24:MI') as end_time,
        coalesce(total, 0) as total
    from core_logic
)

select *
from format_polished
