def transform_orders(df):
    # pretend transformation - preserve origin
    df2 = df.withColumnRenamed("id", "order_id")
    return df2

def enrich_df(df):
    # not used in sample but shows another util
    return df
