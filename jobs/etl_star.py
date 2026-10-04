import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# id из csv нельзя: в каждом файле они с 1 по 1000 и это разные люди/товары
# сумму продажи не пересчитываем, в исходнике она почти никогда не равна price * qty


def norm(name):
    cleaned = F.trim(F.col(name))
    return F.when(cleaned == "", None).otherwise(cleaned)


def with_id(df, id_col, order_cols):
    return df.withColumn(id_col, F.row_number().over(Window.orderBy(*order_cols)))


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


def main():
    pg_url = (
        f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:"
        f"{os.environ['POSTGRES_INTERNAL_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    props = {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
        "rewriteBatchedStatements": "true",
    }

    spark = (
        SparkSession.builder.appName("lab2-star")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    exec_sql(
        spark,
        pg_url,
        props["user"],
        props["password"],
        props["driver"],
        "/opt/sql/star_ddl.sql",
    )

    raw = spark.read.jdbc(pg_url, "mock_data", properties=props).cache()
    raw_cnt = raw.count()

    countries = with_id(
        raw.select(norm("customer_country").alias("country_name"))
        .union(raw.select(norm("seller_country")))
        .union(raw.select(norm("store_country")))
        .union(raw.select(norm("supplier_country")))
        .where(F.col("country_name").isNotNull())
        .dropDuplicates(),
        "country_id",
        ["country_name"],
    )

    pet_types = with_id(
        raw.select(norm("customer_pet_type").alias("pet_type_name"))
        .where(F.col("pet_type_name").isNotNull())
        .dropDuplicates(),
        "pet_type_id",
        ["pet_type_name"],
    )
    breeds = with_id(
        raw.select(norm("customer_pet_breed").alias("breed_name"))
        .where(F.col("breed_name").isNotNull())
        .dropDuplicates(),
        "pet_breed_id",
        ["breed_name"],
    )
    categories = with_id(
        raw.select(norm("product_category").alias("category_name"))
        .where(F.col("category_name").isNotNull())
        .dropDuplicates(),
        "category_id",
        ["category_name"],
    )
    pet_categories = with_id(
        raw.select(norm("pet_category").alias("pet_category_name"))
        .where(F.col("pet_category_name").isNotNull())
        .dropDuplicates(),
        "pet_category_id",
        ["pet_category_name"],
    )
    brands = with_id(
        raw.select(norm("product_brand").alias("brand_name"))
        .where(F.col("brand_name").isNotNull())
        .dropDuplicates(),
        "brand_id",
        ["brand_name"],
    )
    colors = with_id(
        raw.select(norm("product_color").alias("color_name"))
        .where(F.col("color_name").isNotNull())
        .dropDuplicates(),
        "color_id",
        ["color_name"],
    )
    sizes = with_id(
        raw.select(norm("product_size").alias("size_name"))
        .where(F.col("size_name").isNotNull())
        .dropDuplicates(),
        "size_id",
        ["size_name"],
    )
    materials = with_id(
        raw.select(norm("product_material").alias("material_name"))
        .where(F.col("material_name").isNotNull())
        .dropDuplicates(),
        "material_id",
        ["material_name"],
    )

    dates = with_id(
        raw.select(F.to_date(F.col("sale_date"), "M/d/yyyy").alias("full_date"))
        .where(F.col("full_date").isNotNull())
        .dropDuplicates(),
        "date_id",
        ["full_date"],
    ).select(
        "date_id",
        "full_date",
        F.year("full_date").alias("year_num"),
        F.quarter("full_date").alias("quarter_num"),
        F.month("full_date").alias("month_num"),
        F.dayofmonth("full_date").alias("day_num"),
        F.date_format("full_date", "MMMM").alias("month_name"),
    )

    cities = (
        raw.select(norm("store_city").alias("city_name"), norm("store_country").alias("country_name"))
        .union(
            raw.select(norm("supplier_city").alias("city_name"), norm("supplier_country").alias("country_name"))
        )
        .where(F.col("city_name").isNotNull() & F.col("country_name").isNotNull())
        .dropDuplicates(["city_name", "country_name"])
        .join(countries, "country_name")
    )
    cities = with_id(cities, "city_id", ["country_name", "city_name"]).select(
        "city_id", "city_name", "country_id"
    )

    suppliers = (
        raw.select(
            norm("supplier_name").alias("supplier_name"),
            norm("supplier_contact").alias("contact_name"),
            norm("supplier_email").alias("email"),
            norm("supplier_phone").alias("phone"),
            norm("supplier_address").alias("address"),
            norm("supplier_city").alias("city_name"),
            norm("supplier_country").alias("country_name"),
        )
        .where(F.col("email").isNotNull())
        .dropDuplicates(["email"])
        .join(countries, "country_name", "left")
        .join(cities, ["city_name", "country_id"], "left")
    )
    suppliers = with_id(suppliers, "supplier_id", ["email"]).select(
        "supplier_id", "supplier_name", "contact_name", "email", "phone", "address", "city_id"
    )

    customers = (
        raw.select(
            norm("customer_first_name").alias("first_name"),
            norm("customer_last_name").alias("last_name"),
            F.col("customer_age").cast("int").alias("age"),
            norm("customer_email").alias("email"),
            norm("customer_postal_code").alias("postal_code"),
            norm("customer_country").alias("country_name"),
            norm("customer_pet_type").alias("pet_type_name"),
            norm("customer_pet_breed").alias("breed_name"),
            norm("customer_pet_name").alias("pet_name"),
        )
        .where(F.col("email").isNotNull())
        .dropDuplicates(["email"])
        .join(countries, "country_name", "left")
        .join(pet_types, "pet_type_name", "left")
        .join(breeds, "breed_name", "left")
    )
    customers = with_id(customers, "customer_id", ["email"]).select(
        "customer_id",
        "first_name",
        "last_name",
        "age",
        "email",
        "postal_code",
        "country_id",
        "pet_type_id",
        "pet_breed_id",
        "pet_name",
    )

    sellers = (
        raw.select(
            norm("seller_first_name").alias("first_name"),
            norm("seller_last_name").alias("last_name"),
            norm("seller_email").alias("email"),
            norm("seller_postal_code").alias("postal_code"),
            norm("seller_country").alias("country_name"),
        )
        .where(F.col("email").isNotNull())
        .dropDuplicates(["email"])
        .join(countries, "country_name", "left")
    )
    sellers = with_id(sellers, "seller_id", ["email"]).select(
        "seller_id", "first_name", "last_name", "email", "postal_code", "country_id"
    )

    stores = (
        raw.select(
            norm("store_name").alias("store_name"),
            norm("store_location").alias("location"),
            norm("store_state").alias("state_name"),
            norm("store_phone").alias("phone"),
            norm("store_email").alias("email"),
            norm("store_city").alias("city_name"),
            norm("store_country").alias("country_name"),
        )
        .where(F.col("email").isNotNull())
        .dropDuplicates(["email"])
        .join(countries, "country_name", "left")
        .join(cities, ["city_name", "country_id"], "left")
    )
    stores = with_id(stores, "store_id", ["email"]).select(
        "store_id", "store_name", "location", "state_name", "phone", "email", "city_id"
    )

    product_nk = F.md5(
        F.concat_ws(
            "|",
            F.coalesce(norm("product_name"), F.lit("")),
            F.coalesce(norm("product_category"), F.lit("")),
            F.coalesce(norm("product_price"), F.lit("")),
            F.coalesce(norm("product_quantity"), F.lit("")),
            F.coalesce(norm("pet_category"), F.lit("")),
            F.coalesce(norm("product_weight"), F.lit("")),
            F.coalesce(norm("product_color"), F.lit("")),
            F.coalesce(norm("product_size"), F.lit("")),
            F.coalesce(norm("product_brand"), F.lit("")),
            F.coalesce(norm("product_material"), F.lit("")),
            F.coalesce(norm("product_description"), F.lit("")),
            F.coalesce(norm("product_rating"), F.lit("")),
            F.coalesce(norm("product_reviews"), F.lit("")),
            F.coalesce(norm("product_release_date"), F.lit("")),
            F.coalesce(norm("product_expiry_date"), F.lit("")),
            F.coalesce(norm("supplier_email"), F.lit("")),
        )
    )

    products = (
        raw.select(
            norm("product_name").alias("product_name"),
            F.col("product_price").cast("decimal(12,2)").alias("price"),
            F.col("product_quantity").cast("int").alias("stock_quantity"),
            F.col("product_weight").cast("decimal(12,2)").alias("weight"),
            F.col("product_rating").cast("decimal(4,2)").alias("rating"),
            F.col("product_reviews").cast("int").alias("reviews"),
            norm("product_description").alias("description"),
            F.to_date(F.col("product_release_date"), "M/d/yyyy").alias("release_date"),
            F.to_date(F.col("product_expiry_date"), "M/d/yyyy").alias("expiry_date"),
            norm("product_category").alias("category_name"),
            norm("pet_category").alias("pet_category_name"),
            norm("product_brand").alias("brand_name"),
            norm("product_color").alias("color_name"),
            norm("product_size").alias("size_name"),
            norm("product_material").alias("material_name"),
            norm("supplier_email").alias("supplier_email"),
            product_nk.alias("product_nk"),
        )
        .dropDuplicates(["product_nk"])
        .join(categories, "category_name", "left")
        .join(pet_categories, "pet_category_name", "left")
        .join(brands, "brand_name", "left")
        .join(colors, "color_name", "left")
        .join(sizes, "size_name", "left")
        .join(materials, "material_name", "left")
        .join(suppliers.select(F.col("email").alias("supplier_email"), "supplier_id"), "supplier_email", "left")
    )
    products = with_id(products, "product_id", ["product_nk"]).select(
        "product_id",
        "product_name",
        "price",
        "stock_quantity",
        "weight",
        "rating",
        "reviews",
        "description",
        "release_date",
        "expiry_date",
        "category_id",
        "pet_category_id",
        "brand_id",
        "color_id",
        "size_id",
        "material_id",
        "supplier_id",
        "product_nk",
    )

    sales = raw.select(
        F.to_date(F.col("sale_date"), "M/d/yyyy").alias("full_date"),
        norm("customer_email").alias("customer_email"),
        norm("seller_email").alias("seller_email"),
        norm("store_email").alias("store_email"),
        product_nk.alias("product_nk"),
        F.col("sale_quantity").cast("int").alias("sale_quantity"),
        F.col("sale_total_price").cast("decimal(14,2)").alias("sale_total_price"),
    )

    fact = (
        sales.join(dates.select("date_id", "full_date"), "full_date")
        .join(customers.select(F.col("email").alias("customer_email"), "customer_id"), "customer_email")
        .join(sellers.select(F.col("email").alias("seller_email"), "seller_id"), "seller_email")
        .join(stores.select(F.col("email").alias("store_email"), "store_id"), "store_email")
        .join(products.select("product_nk", "product_id"), "product_nk")
        .select(
            "date_id",
            "customer_id",
            "seller_id",
            "product_id",
            "store_id",
            "sale_quantity",
            "sale_total_price",
        )
        .withColumnRenamed("date_id", "sale_date_id")
    )

    fact_cnt = fact.count()
    if fact_cnt != raw_cnt:
        raise SystemExit(f"в факте {fact_cnt} строк, в mock_data {raw_cnt}")

    def save(df, table):
        df.write.mode("append").jdbc(pg_url, table, properties=props)

    save(countries, "star.dim_country")
    save(pet_types, "star.dim_pet_type")
    save(breeds, "star.dim_pet_breed")
    save(categories, "star.dim_product_category")
    save(pet_categories, "star.dim_pet_category")
    save(brands, "star.dim_brand")
    save(colors, "star.dim_color")
    save(sizes, "star.dim_size")
    save(materials, "star.dim_material")
    save(dates, "star.dim_date")
    save(cities, "star.dim_city")
    save(suppliers, "star.dim_supplier")
    save(customers, "star.dim_customer")
    save(sellers, "star.dim_seller")
    save(stores, "star.dim_store")
    save(products, "star.dim_product")
    save(fact, "star.fact_sales")

    print(f"fact_sales: {fact_cnt}")
    spark.stop()


if __name__ == "__main__":
    main()
