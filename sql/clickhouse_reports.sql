DROP TABLE IF EXISTS report_products;
CREATE TABLE report_products (
    product_name   String,
    category_name  String,
    brand_name     String,
    revenue        Decimal(18, 2),
    sales_cnt      UInt64,
    qty_sum        Int64,
    rating         Float64,
    reviews        Int32
) ENGINE = MergeTree
ORDER BY (category_name, product_name, brand_name);

DROP TABLE IF EXISTS report_customers;
CREATE TABLE report_customers (
    first_name   String,
    last_name    String,
    email        String,
    country_name String,
    total_sum    Decimal(18, 2),
    orders_cnt   UInt64,
    avg_check    Decimal(18, 2)
) ENGINE = MergeTree
ORDER BY (country_name, email);

DROP TABLE IF EXISTS report_time;
CREATE TABLE report_time (
    year_num   UInt16,
    month_num  UInt8,
    month_name String,
    revenue    Decimal(18, 2),
    orders_cnt UInt64,
    avg_check  Decimal(18, 2)
) ENGINE = MergeTree
ORDER BY (year_num, month_num);

DROP TABLE IF EXISTS report_stores;
CREATE TABLE report_stores (
    store_name   String,
    city_name    String,
    country_name String,
    revenue      Decimal(18, 2),
    orders_cnt   UInt64,
    avg_check    Decimal(18, 2)
) ENGINE = MergeTree
ORDER BY (country_name, city_name, store_name);

DROP TABLE IF EXISTS report_suppliers;
CREATE TABLE report_suppliers (
    supplier_name String,
    country_name  String,
    revenue       Decimal(18, 2),
    avg_price     Decimal(18, 2),
    sales_cnt     UInt64
) ENGINE = MergeTree
ORDER BY (country_name, supplier_name);

DROP TABLE IF EXISTS report_quality;
CREATE TABLE report_quality (
    product_name  String,
    category_name String,
    brand_name    String,
    rating        Float64,
    reviews       Int32,
    sales_cnt     UInt64,
    revenue       Decimal(18, 2)
) ENGINE = MergeTree
ORDER BY (rating, reviews, product_name);
