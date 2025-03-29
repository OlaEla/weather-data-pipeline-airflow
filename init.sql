CREATE TABLE IF NOT EXISTS weather (
    id INT AUTO_INCREMENT PRIMARY KEY,
    yandex_temp FLOAT,
    open_weather_temp FLOAT,
    timestamp DATETIME
);
