# Отчёт по лабораторной работе №2 (BigDataSpark)

Работа выполнена студентом МАИ группы М8О-205СВ-25 Рудневым Виктором Владимировичем.

## Цель

Собрать ETL на Spark: исходные CSV через PostgreSQL переложить в модель «снежинка», затем посчитать 6 витрин и записать их в ClickHouse.

## Шаги

1. `docker-compose.yml`: PostgreSQL 16, ClickHouse 24.8, Spark 3.5.3. В образ Spark добавлены JDBC-драйверы Postgres и ClickHouse.
2. При старте Postgres в `mock_data` загружаются 10 CSV (скрипт `sql/init/00_mock_data.sql`).
3. `jobs/etl_star.py` читает `mock_data` и пишет измерения и `fact_sales` в схему `star`.
4. `jobs/etl_reports.py` читает `star` и пишет 6 таблиц в ClickHouse.
5. Проверил объёмы и несколько запросов к витринам.

Бонусные БД из задания (Cassandra, Neo4j, MongoDB, Valkey) не поднимал: в тексте они необязательные.

## Обоснование

Модель та же, что в первой работе. Факт — продажа (`sale_quantity`, `sale_total_price`) со ссылками на дату, покупателя, продавца, товар и магазин. Страна и город вынесены отдельно, у товара отдельно категория, бренд, цвет, размер, материал и поставщик.

`id` и `sale_*_id` из CSV не брал: в каждом файле они идут с 1 по 1000 и относятся к разным строкам. Покупатель, продавец, магазин и поставщик определяются по email, товар — по `product_nk` (md5 атрибутов и email поставщика). Сумму чека не пересчитывал из цены и количества: в исходнике они не сходятся.

Витрина — одна таблица на отчёт, зерно такое, чтобы пункты задания считались обычным SQL:

- `report_products` и `report_quality` — по варианту товара (строка `dim_product`);
- `report_customers` — по покупателю;
- `report_time` — по году и месяцу;
- `report_stores` — по магазину;
- `report_suppliers` — по поставщику, средняя цена взята по его товарам, не по чеку.

В этом наборе почти у каждого варианта товара одна продажа, поэтому топ по `sales_cnt` внутри варианта ничего не даёт. Имеет смысл группировать `report_products` по `product_name`. Корреляция рейтинга и числа продаж по варианту не считается: `sales_cnt` везде 1, `corr` возвращает nan.

## Конфигурация

Параметры лежат в `.env`. В git его нет, есть `.env_example`.

```bash
copy .env_example .env
```

| Переменная | Зачем |
|------------|--------|
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | доступ к Postgres |
| `POSTGRES_HOST`, `POSTGRES_INTERNAL_PORT` | адрес Postgres из контейнера Spark (`postgres` / `5432`) |
| `POSTGRES_PORT` | порт на хосте для DBeaver, в шаблоне `5433`, чтобы не задеть первую ЛР |
| `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DB` | доступ к ClickHouse |
| `CLICKHOUSE_HOST`, `CLICKHOUSE_INTERNAL_PORT` | адрес ClickHouse из Spark (`clickhouse` / `8123`) |
| `CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_NATIVE_PORT` | порты на хосте |
| `SPARK_UI_PORT` | Spark UI на хосте, пока джоба работает |

В шаблоне пароли локальные: Postgres `postgres`/`postgres`, база `pet_store`; ClickHouse пользователь `default`, пароль `clickhouse`, база `reports`.

Запуск из каталога `BigDataSpark`:

```bash
docker compose up -d --build
docker exec bdspark-spark /opt/spark/bin/spark-submit /opt/jobs/etl_star.py
docker exec bdspark-spark /opt/spark/bin/spark-submit /opt/jobs/etl_reports.py
```

Повторный прогон джоб пересоздаёт `star` и таблицы отчётов. С нуля вместе с томами: `docker compose down -v && docker compose up -d --build`, затем те же две команды `spark-submit`.

## Результат

PostgreSQL, база `pet_store`:

| Таблица | Строк |
|---------|------:|
| mock_data | 10000 |
| star.fact_sales | 10000 |
| star.dim_customer | 10000 |
| star.dim_product | 10000 |
| star.dim_store | 10000 |
| star.dim_supplier | 10000 |

ClickHouse, база `reports`:

| Таблица | Строк |
|---------|------:|
| report_products | 10000 |
| report_customers | 10000 |
| report_stores | 10000 |
| report_suppliers | 10000 |
| report_quality | 10000 |
| report_time | 12 |

Выручка по категории товара: Toy 868101.63 (3406), Cage 831117.94 (3327), Food 830632.55 (3267).

По названию товара: Bird Cage 3352 продажи / 847478.05, Dog Food 3339 / 848567.19, Cat Toy 3309 / 833806.88.

Покупатели: больше всего из China (1738), дальше Indonesia (1174), Russia (628), Philippines (555), Brazil (385).

Все продажи за 2021 год, выручка 2529852.12, средний чек 252.98. По месяцам чек держится около 246–262, максимум заказов в августе (897) и октябре (892).

Рейтинг товаров в витрине качества от 1 до 5. `corr(rating, sales_cnt)` = nan, потому что у каждого варианта ровно одна продажа.
