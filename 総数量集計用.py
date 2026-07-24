import pandas as pd

file_path = "C:/Users/kanri5/Desktop/out_history_2026-06.xlsx"

df = pd.read_excel(file_path)

df["数量"] = pd.to_numeric(df["数量"], errors="coerce")
df["コード"] = df["コード"].astype(str)

summary = df.groupby("コード", as_index=False).agg({
    "備品名": "first",
    "数量": "sum",
    "単位": "first"
})

summary = summary[["コード", "備品名", "数量", "単位"]]

with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    summary.to_excel(writer, sheet_name="コード別集計", index=False)

print("完了")