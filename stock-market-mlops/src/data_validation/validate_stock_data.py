def validate(df):

    assert (
            df["close"] > 0
    ).all()

    assert (
            df["volume"] >= 0
    ).all()

    return True