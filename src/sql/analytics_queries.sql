-- Query 1: All cities ranked by energy demand
SELECT
    city,
    date,
    temp_mean_f,
    avg_demand_mwh,
    temp_range_f
FROM weather_energy_db.weather_energy_joined
ORDER BY avg_demand_mwh DESC;

-- Query 2: Cold days and their energy impact
SELECT
    city,
    date,
    temp_mean_f,
    avg_demand_mwh,
    CASE
        WHEN temp_mean_f < 32 THEN 'Freezing'
        WHEN temp_mean_f < 50 THEN 'Cold'
        WHEN temp_mean_f < 70 THEN 'Mild'
        ELSE 'Hot'
    END AS temp_category
FROM weather_energy_db.weather_energy_joined
ORDER BY temp_mean_f ASC;

-- Query 3: Temperature vs demand correlation proxy
SELECT
    city,
    AVG(temp_mean_f)   AS avg_temp,
    AVG(avg_demand_mwh) AS avg_demand,
    MAX(avg_demand_mwh) AS peak_demand,
    MIN(temp_mean_f)    AS coldest_day
FROM weather_energy_db.weather_energy_joined
GROUP BY city
ORDER BY avg_demand DESC;

-- Query 4: Days with extreme weather
SELECT
    city,
    date,
    temp_mean_f,
    temp_range_f,
    precipitation_mm,
    windspeed_max_kmh,
    avg_demand_mwh
FROM weather_energy_db.weather_energy_joined
WHERE is_cold_day = true OR is_hot_day = true
ORDER BY date DESC;