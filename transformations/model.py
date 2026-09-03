from pyspark import pipelines as dp

# Dim Booking
@dp.view
def dim_booking_view():
        df =spark.readStream.table("uber.bronze.silver_obt")
        df = df.select("ride_id","confirmation_number","dropoff_location_id","ride_status_id","pickup_city_id","dropoff_city_id","cancellation_reason_id","dropoff_address","dropoff_longitude","dropoff_latitude","booking_timestamp","dropoff_timestamp","pickup_address","pickup_longitude","pickup_latitude","pickup_city","region","state","pickup_location_id")
        df =  df.dropDuplicates (subset=["ride_id"])
        return(df)

dp.create_streaming_table("Dim_booking")

dp.create_auto_cdc_flow(
  target = "Dim_booking",
  source = "dim_booking_view",
  keys = ["ride_id"],
  sequence_by = "ride_id",
  stored_as_scd_type = 1,
)

# Dim Drivers
@dp.view
def dim_driver_view():
        df =spark.readStream.table("uber.bronze.silver_obt")
        df = df.select("driver_id","driver_license","driver_name","driver_phone","driver_rating")
        df =  df.dropDuplicates (subset=["driver_id"])
        return(df)

dp.create_streaming_table("Dim_driver")

dp.create_auto_cdc_flow(
  target = "Dim_driver",
  source = "dim_driver_view",
  keys = ["driver_id"],
  sequence_by = "driver_id",
  stored_as_scd_type = 1,
)

# Dim Passenger
@dp.view
def dim_passenger_view():
        df =spark.readStream.table("uber.bronze.silver_obt")
        df = df.select("passenger_id","passenger_name","passenger_phone","passenger_email")
        df =  df.dropDuplicates (subset=["passenger_id"])
        return(df)


dp.create_streaming_table("Dim_passenger")

dp.create_auto_cdc_flow(
  target = "Dim_passenger",
  source = "dim_passenger_view",
  keys = ["passenger_id"],
  sequence_by = "passenger_id",
  stored_as_scd_type = 1,
)

# Dim payment

@dp.view
def dim_payment_view():
        df =spark.readStream.table("uber.bronze.silver_obt")
        df = df.select("payment_method_id","payment_method","is_card","requires_auth")
        df =  df.dropDuplicates (subset=["payment_method_id"])
        return(df)


dp.create_streaming_table("Dim_payment")

dp.create_auto_cdc_flow(
  target = "Dim_payment",
  source = "dim_payment_view",
  keys = ["payment_method_id"],
  sequence_by = "payment_method_id",
  stored_as_scd_type = 1,
)

# Dim vechile

@dp.view
def dim_vehicle_view():
        df =spark.readStream.table("uber.bronze.silver_obt")
        df = df.select("vehicle_id","vehicle_type","vehicle_make","vehicle_model","vehicle_color")
        df =  df.dropDuplicates (subset=["vehicle_id"])
        return(df)


dp.create_streaming_table("Dim_vehicle")

dp.create_auto_cdc_flow(
  target = "Dim_vehicle",
  source = "dim_vehicle_view",
  keys = ["vehicle_id"],
  sequence_by = "vehicle_id",
  stored_as_scd_type = 1,
)

#Dim Location

@dp.table
def dim_location_view():
        df =spark.readStream.table("uber.bronze.silver_obt")
        df = df.select("pickup_city_id","pickup_city","region","state", "Pickup_timestamp")
        df =  df.dropDuplicates (subset=["pickup_city_id","Pickup_timestamp"])
        return(df)


dp.create_streaming_table("Dim_location")
dp.create_auto_cdc_flow(
  target = "Dim_location",
  source = "dim_location_view",
  keys = ["pickup_city_id"],
  sequence_by = "Pickup_timestamp",
  stored_as_scd_type = 2,
)

# Fact Table
@dp.view
def fact_view():
        df =spark.readStream.table("uber.bronze.silver_obt")
        df = df.select("ride_id","pickup_city_id","payment_method_id","driver_id","passenger_id","vehicle_id","distance_miles","duration_minutes","base_fare","distance_fare","time_fare","surge_multiplier","tip_amount","total_fare","rating","base_rate","per_mile","per_minute")
        df =  df.dropDuplicates (subset=["ride_id"])
        return(df)


dp.create_streaming_table("fact")
dp.create_auto_cdc_flow(
  target = "fact",
  source = "fact_view",
  keys = ["ride_id","pickup_city_id","payment_method_id","driver_id","passenger_id","vehicle_id"],
  sequence_by = "ride_id",
  stored_as_scd_type = 1,
)