import datetime
import os
import requests
import pendulum
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Загрузка переменных окружения для безопасности
load_dotenv()

# Настройки безопасности и конфигурации
os.environ["no_proxy"] = "*"

@dag(
    dag_id="weather-telegram-etl",
    schedule="@hourly",  # Почасовое выполнение
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=30)
)
def WeatherETL():
    @task(task_id='yandex_weather')
    def get_yandex_weather(**kwargs):
        """Получение погоды от Яндекс с использованием GraphQL"""
        try:
            # URL для GraphQL запроса
            url = "https://api.weather.yandex.ru/graphql/query"
            
            # Ключ API (переменная окружения)
            access_key = os.getenv('YANDEX_WEATHER_API_KEY')
            
            # Заголовок с ключом API
            headers = {
                "X-Yandex-Weather-Key": access_key,
            }
            
            # Запрос GraphQL для получения погоды по координатам
            query = """
            {
            weatherByPoint(request: { lat: 55.75396, lon: 37.620393 }) {
                now {
                temperature
                }
            }
            }
            """
            
            # Выполнение POST-запроса с GraphQL
            response = requests.post(url, headers=headers, json={'query': query})
            
            # Проверка на ошибки в запросе
            response.raise_for_status()
            
            # Извлечение температуры из ответа
            data = response.json()
            temperature = data['data']['weatherByPoint']['now']['temperature']
            
            # Отправка температуры в XCom для дальнейшего использования
            kwargs['ti'].xcom_push(key='yandex_weather', value=temperature)
            
            return temperature
        
        except Exception as e:
            print(f"Ошибка при получении погоды от Яндекс: {e}")
            return None


    @task(task_id='open_weather')
    def get_open_weather(**kwargs):
        """Получение погоды от OpenWeatherMap"""
        try:
            url = "https://api.openweathermap.org/data/2.5/weather"
            params = {
                'lat': os.getenv('LATITUDE', '55.749013596652574'),
                'lon': os.getenv('LONGITUDE', '37.61622153253021'),
                'appid': os.getenv('OPENWEATHER_API_KEY')
            }
            response = requests.get(url, params=params)
            response.raise_for_status()
            temp = round(float(response.json()['main']['temp']) - 273.15, 2)
            kwargs['ti'].xcom_push(key='open_weather', value=temp)
            return temp
        except Exception as e:
            print(f"Ошибка при получении погоды от OpenWeather: {e}")
            return None


    @task(task_id='save_weather_to_mysql')
    def save_weather_to_mysql(**kwargs):
        """Сохранение погодных данных в MySQL"""
        try:
            yandex_temp = kwargs['ti'].xcom_pull(task_ids='yandex_weather', key='yandex_weather')
            open_weather_temp = kwargs['ti'].xcom_pull(task_ids='open_weather', key='open_weather')
            
            # Создание подключения с использованием переменных окружения
            connection_string = (
                f"mysql+pymysql://{os.getenv('DB_USER', 'Airflow')}:"  # Используем переменные окружения
                f"{os.getenv('DB_PASSWORD', 'airflow_password')}@"
                f"{os.getenv('DB_HOST', 'localhost')}:"  # Локальный MySQL
                f"{os.getenv('DB_PORT', '3306')}/"
                f"{os.getenv('DB_NAME', 'airflow_db')}"  
            )
            engine = create_engine(connection_string)
            
            # Подготовка данных
            data = [[
                str(yandex_temp), 
                str(open_weather_temp), 
                datetime.datetime.now()
            ]]
            
            # Запись в базу данных
            df = pd.DataFrame(data, columns=['yandex_temp', 'open_weather_temp', 'timestamp'])
            df.to_sql('weather', engine, if_exists='append', index=False)  # Записываем данные в таблицу 'weather'
            
        except Exception as e:
            print(f"Ошибка при сохранении данных в MySQL: {e}")


    @task(task_id='telegram_conn')
    def send_telegram_notification(**kwargs):
        """Отправка уведомления в Telegram через Airflow"""
        yandex_temp = kwargs['ti'].xcom_pull(task_ids='yandex_weather', key='yandex_weather')
        open_weather_temp = kwargs['ti'].xcom_pull(task_ids='open_weather', key='open_weather')


        # Логирование данных, которые получаем из XCom
        print(f"Yandex temperature: {yandex_temp}")
        print(f"OpenWeather temperature: {open_weather_temp}")


        message = (
            f"🌤 Погода в Москве:\n"
            f"🌡 Яндекс: {yandex_temp}°C\n"
            f"🌍 OpenWeather: {open_weather_temp}°C"
        )

        # Логируем сообщение
        print(f"Отправка сообщения: {message}")
        print(f"Используется chat_id: {os.getenv('TELEGRAM_CHAT_ID')}")

        send_message = TelegramOperator(
            task_id='send_telegram_message',
            telegram_conn_id='telegram_conn',
            chat_id=os.getenv('TELEGRAM_CHAT_ID'),
            text=message
        )

        send_message.execute(context=kwargs)


    # Порядок выполнения задач
    (
        get_yandex_weather() 
        >> get_open_weather() 
        >> save_weather_to_mysql() 
        >> send_telegram_notification()
    )

dag = WeatherETL()