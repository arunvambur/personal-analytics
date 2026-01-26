from breeze_connect import BreezeConnect

breeze = BreezeConnect(api_key="YOUR_API_KEY")

breeze.generate_session(
    api_secret="YOUR_API_SECRET",
    session_token="YOUR_SESSION_TOKEN"
)

symbols = ["TCS", "INFY", "HDFCBANK", "RELIANCE"]
all_data = []

for symbol in symbols:
    resp = breeze.get_historical_data_v2(
        interval="1day",
        from_date="2024-01-01T00:00:00.000Z",
        to_date="2024-01-31T00:00:00.000Z",
        stock_code=symbol,
        exchange_code="NSE",
        product_type="cash"
    )

    df = pd.DataFrame(resp["Success"])
    df["symbol"] = symbol
    all_data.append(df)

final_df = pd.concat(all_data)
print(final_df.head())
