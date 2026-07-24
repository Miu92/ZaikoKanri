import pandas as pd

file_path = "C:/Users/kanri5/Desktop/out_history_2026-06.xlsx"

df = pd.read_excel(file_path)

result = (
    df.groupby(
        ["納品先", "コード", "備品名", "単位"],
        as_index=False
    )["数量"]
    .sum()
)

with pd.ExcelWriter(
    file_path,
    engine="openpyxl",
    mode="a",
    if_sheet_exists="replace"
) as writer:
    result.to_excel(
        writer,
        sheet_name="部門別集計",
        index=False
    )

print("集計完成")