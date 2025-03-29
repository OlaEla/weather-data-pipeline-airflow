# ETL: автоматизация подготовки данных

## Урок 8. Специфика применения ETL в различных предметных сферах

Домашнее задание (вариант от преподавателя)

    1. Скачать s8dag с mail облака.
    2. Доработать задачу python_weather с тем чтобы полученные значения погоды писались в таблицу mysql.
    Погоду яндекса и OW записываем в отдельные столбцы. Добавляем в третий столбец метку текущего времени.
    3. попробовать запустить телеграм бот в s8dag. В случае успеха дать подробное описание, что сделали/изменили, как настроили бот.

## Решение

Проект использует Docker для развертывания среды выполнения Airflow с базой данных MySQL, где хранятся данные о погоде. Основная цель — получение данных о погоде из Яндекс.Погоды и OpenWeather, их запись в базу данных c меткой текущего времени и отправка уведомлений в Telegram.

### 1. Структура проекта

# **Описание контейнеризации проекта с Airflow, MySQL и Telegram-ботом**

## **1. Структура проекта**

```plaintext
/d8dag_weather_bot
│── dags/                     # Папка с DAG-файлами Airflow
│   ├── yandex_openweather_etl_d8dag_weather_bot.py  # Основной DAG-файл
│── logs/                     # Логи Airflow
│── plugins/                  # Плагины Airflow
│── docker-compose.yml        # Файл конфигурации Docker Compose
│── Dockerfile                # Инструкция для сборки контейнера Airflow
│── .env                      # Переменные окружения
│── init.sql                  # SQL-скрипт для инициализации БД
│── my.cnf                    # Конфигурация MySQL
│── requirements.txt          # Зависимости Python
```

### 2. Краткое описание контейнеризации

#### 2.1 docker-compose.yml

В этом файле определены все сервисы, которые используются в проекте.

- Контейнер mysql (База данных)
  Запускает MySQL 8 с паролем и пользователем.

При старте инициализируется SQL-скриптом init.sql, который создает таблицу weather.

Открывает порт 3306 для взаимодействия с другими контейнерами.

Использует файл конфигурации my.cnf, где указано default-authentication-plugin=mysql_native_password, чтобы избежать проблем с подключением.

- Контейнер airflow-init
  Подготавливает базу данных для Airflow (airflow db migrate).

Создает пользователя admin для Airflow.

- Контейнер airflow-webserver
  Запускает Airflow Web UI (порт 8080).

Берет настройки базы данных из переменных окружения (AIRFLOW**DATABASE**SQL_ALCHEMY_CONN).

Загружает API-ключи для Telegram, Yandex и OpenWeather из .env.

- Контейнер airflow-scheduler
  Запускает планировщик задач для DAG-ов.

Работает в связке с airflow-webserver, ожидая выполнения DAG-ов.

- Контейнер airflow-worker
  Используется для выполнения задач DAG.

Включает поддержку CeleryExecutor, если проект масштабируется.

## 3. Файлы проекта

#### 3.1 init.sql — инициализация базы данных

```sql
CREATE TABLE IF NOT EXISTS weather (
    id INT AUTO_INCREMENT PRIMARY KEY,
    yandex_temp FLOAT,
    open_weather_temp FLOAT,
    timestamp DATETIME
);
```

Создает таблицу weather в базе данных, куда записываются температуры.

#### 3.2 my.cnf — конфигурация MySQL

```ini
[mysqld]
default-authentication-plugin=mysql_native_password
```

Указывает старый механизм аутентификации (mysql_native_password), чтобы Airflow мог подключаться к MySQL.

#### 3.3 Dockerfile — настройка контейнера Airflow

```dockerfile
FROM apache/airflow:2.7.3-python3.8

USER root

# Устанавливаем системные зависимости

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    pkg-config \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt

USER airflow

# Обновляем pip и устанавливаем зависимости

RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    -r /requirements.txt
```

#### 3.4 .env — переменные окружения

- Файл .env содержит переменные окружения, которые используются для настройки и конфигурации различных сервисов в проекте.

- Упрощает управление конфиденциальными данными и параметрами без необходимости изменять код или файлы конфигурации.

- Позволяет хранить чувствительные данные (пароли, API-ключи) отдельно от кода, чтобы избежать их утечки в репозитории.

- Docker Compose и Airflow читают переменные из .env, упрощая переносимость.

- Один файл .env можно использовать на разных серверах с разными параметрами.

Использование .env делает проект гибким, удобным и безопасным. Это лучший способ управлять конфиденциальными данными и изменять конфигурацию без изменения кода.

#### 3.5 requirements.txt — управление зависимостями в Airflow

Файл requirements.txt используется в проектах на Python для управления зависимостями. Он содержит список всех библиотек, которые необходимо установить, чтобы проект корректно работал.

При сборке контейнера Docker запускается команда:

```bash
pip install -r requirements.txt
```

Airflow автоматически устанавливает все пакеты из списка, что упрощает развертывание.

## 4. Вывод

- Контейнеризация позволяет легко развернуть Airflow с MySQL.

- Данные о погоде записываются в БД и отправляются в Telegram.

- Можно быстро развернуть и протестировать проект на любом сервере.

## Описание изменений файла d8dag.py

1. Использование telegram_conn вместо хардкод-токена
   В исходном коде token='...' и chat_id='...' были хардкодом, что небезопасно.

Теперь мы используем telegram_conn, который задается в Airflow UI.

2. Корректное извлечение данных из XCom

В оригинальном коде использовалась конструкция:

```python
"{{ ti.xcom_pull(task_ids=['yandex_wether'],key='wether')[0] }}"
```

Это неверно, так как в Jinja-шаблонах task_ids должен быть строкой (не списком).
Исправлено:

```python
"{{ ti.xcom_pull(task_ids='yandex_weather', key='weather') }}"
```

3. Оптимизирован код работы с MySQL
   В исходном коде:

```python
df.to_sql('wether', con, schema='spark', if_exists='replace', index=False)
```

использовался replace, из-за чего вся таблица удалялась при каждом запуске.

Теперь используется if_exists='append', чтобы новые данные добавлялись, а не затирали старые.

4. Исправлен schedule.
   Был @once, но если DAG нужно запускать регулярно, то можно изменить на @hourly:

```python
schedule="@hourly"
```

## Скриншоты

### 1. Интерфейс Airflow с запущенным DAG

![Airflow DAG](img/airflow_dag.png)

### 2. Лог выполнения задачи

![Airflow Logs](img/airflow_logs.png)

### 3. Содержимое таблицы MySQL с погодными данными

![MySQL Table](img/mysql_table.png)

### 4. Сообщение в Telegram от бота

![Telegram Bot Message](img/telegram_bot.png)

### 5. Лог работы контейнеров Docker

![Docker Logs](img/docker_logs.png)
