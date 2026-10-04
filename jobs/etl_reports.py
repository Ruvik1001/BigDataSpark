import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def exec_sql(spark, url, user, password, driver, path):
    spark._sc._jvm.java.lang.Class.forName(driver)
    conn = spark._sc._jvm.java.sql.DriverManager.getConnection(url, user, password)
    stmt = conn.createStatement()
    script = open(path, encoding="utf-8").read()
    for part in script.split(";"):
        sql = part.strip()
        if sql:
            stmt.execute(sql)
    stmt.close()
    conn.close()


def read_pg(spark, url, props, table):
    return spark.read.jdbc(url, table, properties=props)


def write_ch(df, url, props, table):
    # у ClickHouse jdbc commit транзакции Spark падает, поэтому isolationLevel NONE
    (
        df.write.mode("append")
        .option("isolationLevel", "NONE")
        .option("batchsize", "5000")
        .jdbc(url, table, properties=props)
    )


def main():
    pg_url = (
        f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:"
        f"{os.environ['POSTGRES_INTERNAL_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    pg_props = {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }
    ch_url = (
        f"jdbc:clickhouse://{os.environ['CLICKHOUSE_HOST']}:"
        f"{os.environ['CLICKHOUSE_INTERNAL_PORT']}/{os.environ['CLICKHOUSE_DB']}"
    )
    ch_props = {
        "user": os.environ["CLICKHOUSE_USER"],
        "password": os.environ["CLICKHOUSE_PASSWORD"],
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    }

    spark = SparkSession.builder.appName("lab2-reports").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    exec_sql(
        spark,
        ch_url,
        ch_props["user"],
        ch_props["password"],
        ch_props["driver"],
        "/opt/sql/clickhouse_reports.sql",
    )

    fact = read_pg(spark, pg_url, pg_props, "star.fact_sales")
    product = read_pg(spark, pg_url, pg_props, "star.dim_product")
    category = read_pg(spark, pg_url, pg_props, "star.dim_product_category")
    brand = read_pg(spark, pg_url, pg_props, "star.dim_brand")
    customer = read_pg(spark, pg_url, pg_props, "star.dim_customer")
    country = read_pg(spark, pg_url, pg_props, "star.dim_country")
    store = read_pg(spark, pg_url, pg_props, "star.dim_store")
    city = read_pg(spark, pg_url, pg_props, "star.dim_city")
    supplier = read_pg(spark, pg_url, pg_props, "star.dim_supplier")
    dates = read_pg(spark, pg_url, pg_props, "star.dim_date")

    by_product = fact.groupBy("product_id").agg(
        F.sum("sale_total_price").cast("decimal(18,2)").alias("revenue"),
        F.count("*").alias("sales_cnt"),
        F.sum("sale_quantity").cast("long").alias("qty_sum"),
    )
    product_card = (
        by_product.join(product, "product_id")
        .join(category, "category_id", "left")
        .join(brand, "brand_id", "left")
        .select(
            F.coalesce(F.col("product_name"), F.lit("")).alias("product_name"),
            F.coalesce(F.col("category_name"), F.lit("")).alias("category_name"),
            F.coalesce(F.col("brand_name"), F.lit("")).alias("brand_name"),
            "revenue",
            "sales_cnt",
            "qty_sum",
            F.coalesce(F.col("rating").cast("double"), F.lit(0.0)).alias("rating"),
            F.coalesce(F.col("reviews"), F.lit(0)).cast("int").alias("reviews"),
            "supplier_id",
            "price",
        )
    )

    report_products = product_card.select(
        "product_name",
        "category_name",
        "brand_name",
        "revenue",
        "sales_cnt",
        "qty_sum",
        "rating",
        "reviews",
    )
    report_quality = product_card.select(
        "product_name",
        "category_name",
        "brand_name",
        "rating",
        "reviews",
        "sales_cnt",
        "revenue",
    )

    report_customers = (
        fact.groupBy("customer_id")
        .agg(
            F.sum("sale_total_price").cast("decimal(18,2)").alias("total_sum"),
            F.count("*").alias("orders_cnt"),
            F.avg("sale_total_price").cast("decimal(18,2)").alias("avg_check"),
        )
        .join(customer, "customer_id")
        .join(country, "country_id", "left")
        .select(
            F.coalesce(F.col("first_name"), F.lit("")).alias("first_name"),
            F.coalesce(F.col("last_name"), F.lit("")).alias("last_name"),
            "email",
            F.coalesce(F.col("country_name"), F.lit("")).alias("country_name"),
            "total_sum",
            "orders_cnt",
            "avg_check",
        )
    )

    report_time = (
        fact.join(dates, fact.sale_date_id == dates.date_id)
        .groupBy("year_num", "month_num", "month_name")
        .agg(
            F.sum("sale_total_price").cast("decimal(18,2)").alias("revenue"),
            F.count("*").alias("orders_cnt"),
            F.avg("sale_total_price").cast("decimal(18,2)").alias("avg_check"),
        )
        .select(
            F.col("year_num").cast("int").alias("year_num"),
            F.col("month_num").cast("int").alias("month_num"),
            F.trim(F.col("month_name")).alias("month_name"),
            "revenue",
            "orders_cnt",
            "avg_check",
        )
    )

    store_geo = (
        store.join(city, "city_id", "left")
        .join(country, "country_id", "left")
        .select(
            "store_id",
            F.coalesce(F.col("store_name"), F.lit("")).alias("store_name"),
            F.coalesce(F.col("city_name"), F.lit("")).alias("city_name"),
            F.coalesce(F.col("country_name"), F.lit("")).alias("country_name"),
        )
    )
    report_stores = (
        fact.groupBy("store_id")
        .agg(
            F.sum("sale_total_price").cast("decimal(18,2)").alias("revenue"),
            F.count("*").alias("orders_cnt"),
            F.avg("sale_total_price").cast("decimal(18,2)").alias("avg_check"),
        )
        .join(store_geo, "store_id")
        .select("store_name", "city_name", "country_name", "revenue", "orders_cnt", "avg_check")
    )

    # средняя цена по товарам поставщика, не по сумме чека
    supplier_price = product.groupBy("supplier_id").agg(
        F.avg("price").cast("decimal(18,2)").alias("avg_price")
    )
    supplier_sales = (
        fact.join(product.select("product_id", "supplier_id"), "product_id")
        .groupBy("supplier_id")
        .agg(
            F.sum("sale_total_price").cast("decimal(18,2)").alias("revenue"),
            F.count("*").alias("sales_cnt"),
        )
    )
    supplier_geo = (
        supplier.join(city, "city_id", "left")
        .join(country, "country_id", "left")
        .select(
            "supplier_id",
            F.coalesce(F.col("supplier_name"), F.lit("")).alias("supplier_name"),
            F.coalesce(F.col("country_name"), F.lit("")).alias("country_name"),
        )
    )
    report_suppliers = (
        supplier_sales.join(supplier_price, "supplier_id", "left")
        .join(supplier_geo, "supplier_id")
        .select(
            "supplier_name",
            "country_name",
            "revenue",
            F.coalesce(F.col("avg_price"), F.lit(0)).cast("decimal(18,2)").alias("avg_price"),
            "sales_cnt",
        )
    )

    write_ch(report_products, ch_url, ch_props, "report_products")
    write_ch(report_customers, ch_url, ch_props, "report_customers")
    write_ch(report_time, ch_url, ch_props, "report_time")
    write_ch(report_stores, ch_url, ch_props, "report_stores")
    write_ch(report_suppliers, ch_url, ch_props, "report_suppliers")
    write_ch(report_quality, ch_url, ch_props, "report_quality")

    print("reports written")
    spark.stop()


if __name__ == "__main__":
    main()
