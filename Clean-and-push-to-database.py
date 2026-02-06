import pandas as pd

file_path = r"C:\Users\thenm\Downloads\orders_export_1\orders_export_1-2-4-26.csv"

df = pd.read_csv(file_path)

print(df.columns.tolist())

columns_to_keep = [
    "Billing Name",
    "Name",
    "Email",
    "Phone",
    "Created at",
    "Lineitem quantity",
    "Lineitem name",
    "Lineitem price",
    "Lineitem sku",
    "Subtotal",
    "Id"
]

df_clean = df[columns_to_keep].copy()

df_clean = df_clean.rename(columns={"Id": "Order ID"})

df_clean.to_csv("shopify_orders_columns_cleaned.csv", index=False)
