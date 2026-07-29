import pandas as pd


file_path = r"\\dionas\管理部\備品管理\入出庫履歴\2026出庫履歴.xlsx"
sheet_name = "部門別集計"

df = pd.read_excel(file_path)
df["数量"] = pd.to_numeric(df["数量"], errors="coerce").fillna(0)
df["コード"] = df["コード"].astype(str)
df["日時"] = pd.to_datetime(df["日時"], errors="coerce")
df_valid_date = (df.dropna(subset=["日時"]).copy())
df_valid_date["年月"] = (df_valid_date["日時"].dt.strftime("%Y-%m"))


group_columns = ["納品先", "コード", "備品名", "単位"]
total_summary = (df.groupby(group_columns, as_index=False, dropna=False)["数量"]
    .sum()
    .rename(columns={"数量": "総消耗数"})
)


monthly_pivot = pd.pivot_table(
    df_valid_date,
    index=group_columns,
    columns="年月",
    values="数量",
    aggfunc="sum",
    fill_value=0
).reset_index()
monthly_pivot.columns.name = None


result = pd.merge(total_summary, monthly_pivot, on=group_columns, how="outer")

month_columns = sorted(
    [
        column
        for column in result.columns
        if column not in (group_columns + ["総消耗数"])
    ]
)


fixed_columns = ["納品先", "コード", "備品名", "総消耗数", "単位"]
result = result[fixed_columns + month_columns]
result[month_columns] = (result[month_columns].replace(0, ""))
result = result.sort_values(["納品先", "コード"],na_position="last")


with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    result.to_excel(writer, sheet_name=sheet_name, index=False)

print("完了")
