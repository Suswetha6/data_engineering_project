def validate_order_detail(df):
    errors = df[
        (df["order_quantity"] <= 0) |
        (df["discount"] < 0) |
        (df["discount"] > 1)
    ]
    valid = df.drop(errors.index)
    return valid, errors

