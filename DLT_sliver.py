import dlt

from pyspark.sql.functions import *
from pyspark.sql.types import *



## SILVER EVENTS


@dlt.table(
    name="stage_events"
)
def stage_events():

    df = spark.readStream.format("delta") \
        .load("/Volumes/pulsetrack/bronze/bronzevolume/events/data/")

    return df


@dlt.view(
    name="trans_events"
)
def trans_events():

    df = dlt.read_stream("stage_events")

    df = (
        df.withColumn("user_id", col("user_id").cast("string"))
          .withColumn("event_id", col("event_id").cast("string"))
          .withColumn("product_id", col("product_id").cast("string"))
          .withColumn("session_id", col("session_id").cast("string"))
          .withColumn("event_time", col("event_time").cast("timestamp"))
    )

    # Standardize event type
    df = df.withColumn(
        "event_type",
        lower(trim(col("event_type")))
    )

    # Remove rescued data
    df = df.drop("_rescued_data")

    return df


event_rules = {

    "valid_event_id":
        "event_id IS NOT NULL",

    "valid_user_id":
        "user_id IS NOT NULL",

    "valid_event_type":
        "event_type IN ('view', 'click', 'add_to_cart', 'purchase')",

    "valid_event_time":
        "event_time IS NOT NULL",

    "valid_session_id":
        "session_id IS NOT NULL",
        
    "valid_product_id":
        "product_id IS NOT NULL"
}


@dlt.table(
    name="silver_events"
)

@dlt.expect_all(event_rules)

def silver_events():

    df = dlt.read_stream("trans_events")

    return df



###################################################################################################################


## SILVER INVENTORY



# STAGE LAYER


@dlt.table(
    name="stage_inventory"
)

def stage_inventory():

    df = spark.readStream.format("delta") \
        .load("/Volumes/pulsetrack/bronze/bronzevolume/inventory/data/")

    return df



# TRANSFORMATION LAYER


@dlt.view(
    name="trans_inventory"
)

def trans_inventory():

    df = dlt.read_stream("stage_inventory")

    df = (

        df.withColumn(
            "stock_level",
            col("stock_level").cast("int")
        )

        .withColumn(
            "product_id",
            col("product_id").cast("string")
        )

        .withColumn(
            "modified_date",
            current_timestamp()
        )

        .drop("_rescued_data")

    )

    return df


# DATA QUALITY RULES


trans_rules = {

    "valid_product_id":
        "product_id IS NOT NULL",

    "valid_stock_level":
        "stock_level >= 0"
}



# SILVER LAYER


@dlt.table(
    name="silver_inventory"
)

@dlt.expect_all(trans_rules)

def silver_inventory():

    df = dlt.read_stream("trans_inventory")

    return df


# STAGE ORDERS TABLE


@dlt.table(
    name="stage_orders"
)

def stage_orders():

    df = spark.readStream.format("delta") \
        .load("/Volumes/pulsetrack/bronze/bronzevolume/orders/data/")

    return df



# TRANSFORM ORDERS


@dlt.view(
    name="trans_orders"
)

def trans_orders():

    df = dlt.read_stream("stage_orders")

    df = (

        df.withColumn(
            "order_id",
            col("order_id").cast("string")
        )

        .withColumn(
            "user_id",
            col("user_id").cast("string")
        )

        .withColumn(
            "product_id",
            col("product_id").cast("string")
        )

        .withColumn(
            "quantity",
            col("quantity").cast("int")
        )

        .withColumn(
            "price",
            col("price").cast("double")
        )

        .withColumn(
            "order_date",
            to_timestamp(col("order_date"), "MM-dd-yyyy HH:mm:ss")
        )

        .withColumn(
            "status",
            lower(trim(col("status")))
        )

        .drop("_rescued_data")

    )

    return df



# DATA QUALITY RULES


order_rules = {

    "valid_order_id":
        "order_id IS NOT NULL",

    "valid_user_id":
        "user_id IS NOT NULL",

    "valid_product_id":
        "product_id IS NOT NULL",

    "valid_quantity":
        "quantity > 0",

    "valid_order_amount":
        "price > 0",

    "valid_order_timestamp":
        "order_date IS NOT NULL",

    "valid_order_status":
        "status IN ('completed', 'cancelled', 'pending')"
}



# SILVER ORDERS TABLE


@dlt.table(
    name="silver_orders"
)

@dlt.expect_all(order_rules)

def silver_orders():

    df = dlt.read_stream("trans_orders")

    return df

###################################################################################################################

# SILVER USERS


# STAGE USERS


@dlt.table(
    name="stage_users"
)

def stage_users():

    df = spark.readStream.format("delta") \
        .load("/Volumes/pulsetrack/bronze/bronzevolume/users/data/")

    return df



# TRANSFORM USERS


@dlt.view(
    name="trans_users"
)

def trans_users():

    df = dlt.read_stream("stage_users")

    df = (

        df.withColumn(
            "user_id",
            col("user_id").cast("string")
        )

        .withColumn(
            "email",
            lower(trim(col("email")))
        )

        .withColumn(
            "signup_date",
            to_timestamp(col("signup_date"))
        )

        .withColumn(
            "city",
            upper(trim(col("city")))
        )

        .withColumn(
            "device",
            upper(trim(col("device")))
        )

        .drop("_rescued_data")

    )

    return df



# RULES


user_rules = {

    "valid_user_id":
        "user_id IS NOT NULL",

    "valid_email":
        "email IS NOT NULL",

    "valid_signup_date":
        "signup_date IS NOT NULL"
}



# SILVER USERS


@dlt.table(
    name="silver_users"
)

@dlt.expect_all(user_rules)

def silver_users():

    df = dlt.read_stream("trans_users")

    return df


###################################################################################################################

# SILVER PRODUCTS


# STAGE PRODUCTS


@dlt.table(
    name="stage_products"
)

def stage_products():

    df = spark.readStream.format("delta") \
        .load("/Volumes/pulsetrack/bronze/bronzevolume/products/data/")

    return df



# TRANSFORM PRODUCTS


@dlt.view(
    name="trans_products"
)

def trans_products():

    df = dlt.read_stream("stage_products")

    df = (

        df.withColumn(
            "product_id",
            col("product_id").cast("string")
        )

        .withColumn(
            "product_name",
            initcap(trim(col("product_name")))
        )

        .withColumn(
            "category",
            lower(trim(col("category")))
        )

        .withColumn(
            "price",
            col("price").cast("double")
        )

        .drop("_rescued_data")

    )

    return df



# RULES


product_rules = {

    "valid_product_id":
        "product_id IS NOT NULL",

    "valid_price":
        "price > 0",

    "valid_category":
        "category IS NOT NULL"
}


# SILVER PRODUCTS


@dlt.table(
    name="silver_products"
)

@dlt.expect_all(product_rules)

def silver_products():

    df = dlt.read_stream("trans_products")

    return df


###################################################################################################################

# SILVER PAYMENTS


# STAGE PAYMENTS


@dlt.table(
    name="stage_payments"
)

def stage_payments():

    df = spark.readStream.format("delta") \
        .load("/Volumes/pulsetrack/bronze/bronzevolume/payments/data/")

    return df



# TRANSFORM PAYMENTS


@dlt.view(
    name="trans_payments"
)

def trans_payments():

    df = dlt.read_stream("stage_payments")

    df = (

        df.withColumn(
            "payment_id",
            col("payment_id").cast("string")
        )

        .withColumn(
            "order_id",
            col("order_id").cast("string")
        )

        .withColumn(
            "amount",
            col("amount").cast("double")
        )

        .withColumn(
            "payment_method",
            lower(trim(col("payment_method")))
        )

        .withColumn(
            'payment_status', 
            lower(trim(col('payment_status')))
        )
        
        .drop("_rescued_data")

    )

    return df


# RULES

payments_rules = {

    "valid_payment_id":
        "payment_id IS NOT NULL",

    "valid_order_id":
        "order_id IS NOT NULL",

    "valid_payment_amount":
        "amount > 0"
        }



# SILVER PAYMENTS


@dlt.table(
    name="silver_payments"
)

@dlt.expect_all(payments_rules)

def silver_payments():

    df = dlt.read_stream("trans_payments")

    return df

###################################################################################################################

# SILVER REVIEWS


# STAGE REVIEWS


@dlt.table(
    name="stage_reviews"
)

def stage_reviews():

    df = spark.readStream.format("delta") \
        .load("/Volumes/pulsetrack/bronze/bronzevolume/reviews/data/")

    return df



# TRANSFORM REVIEWS


@dlt.view(
    name="trans_reviews"
)

def trans_reviews():

    df = dlt.read_stream("stage_reviews")

    df = (

        df.withColumn(
            "review_id",
            col("review_id").cast("string")
        )

        .withColumn(
            "user_id",
            col("user_id").cast("string")
        )

        .withColumn(
            "product_id",
            col("product_id").cast("string")
        )

        .withColumn(
            "rating",
            col("rating").cast("int")
        )

        .drop("_rescued_data")

    )

    return df



# RULES


review_rules = {

    "valid_review_id":
        "review_id IS NOT NULL",

    "valid_user_id":
        "user_id IS NOT NULL",

    "valid_product_id":
        "product_id IS NOT NULL",

    "valid_rating":
        "rating BETWEEN 1 AND 5"
}


# SILVER REVIEWS


@dlt.table(
    name="silver_reviews"
)

@dlt.expect_all(review_rules)

def silver_reviews():

    df = dlt.read_stream("trans_reviews")

    return df
