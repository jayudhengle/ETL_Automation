import pandas as pd
import connectionToDatabase as conection

df_source = pd.read_csv("J:/MyData/ETLData/customer_source.csv")
print(df_source)

targetSQL = """select customer_id, concat(first_name, ' ', last_name) as full_name, lower(trim(email)) as email,
replace(phone, '+91', '-') as phone, format(dob, 'dd-MM-yyy') as dob, CASE WHEN country_code = 'IN' then 'India' end as country, 
FORMAT(created_date, 'dd-MM-yyyy HH:mm') as created_at from dim_customer"""
df_target = pd.read_sql(targetSQL, conection.engine)
print(df_target)


df_mapping = pd.read_excel("J:/MyData/ETLData/Customer_ETL_Mapping.xlsx", sheet_name="Source_to_Target")
print(df_mapping)

def test_countValidation():
    sourceCount = len(df_source)
    targetCount = len(df_target)

    print("Source Count" ,sourceCount)
    print("Target Count" ,targetCount)

    assert sourceCount==targetCount,f"Counts not matching, Source Count: {sourceCount} Target Count: {targetCount}"


def test_DataTypeValidations():

    df_target_dtypesSQL = " SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'dim_customer' ORDER BY ORDINAL_POSITION "

    df_target_dtypes1 = pd.read_sql(df_target_dtypesSQL, conection.engine)
    df_target_dtypes = df_target_dtypes1[['COLUMN_NAME', 'DATA_TYPE']]
    df_source_dtypes = df_mapping[['Target Column', 'Target Type']].rename(
        columns={
            'Target Column': 'COLUMN_NAME',
            'Target Type': 'DATA_TYPE'
        }    )
    df_source_dtypes = df_source_dtypes.astype(df_target_dtypes.dtypes.to_dict())
    df_compare = df_source_dtypes.merge(df_target_dtypes, on="COLUMN_NAME", suffixes=('_source', '_target'))
    mismatched = df_compare[df_compare['DATA_TYPE_source'] != df_compare['DATA_TYPE_target']]
    mismatched.to_excel("reports/DataType_Mismatch_Report.xlsx", index=False)
    assert mismatched.empty, f"\nData type mismatches found:\n{mismatched.to_string(index=False)}"

def test_NULLValidations():

    df_target_dtypesSQL = " SELECT COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'dim_customer' ORDER BY ORDINAL_POSITION "

    df_target_dtypes1 = pd.read_sql(df_target_dtypesSQL, conection.engine)
    df_target_dtypes = df_target_dtypes1[['COLUMN_NAME', 'IS_NULLABLE']]
    df_source_dtypes = df_mapping[['Target Column', 'Nullable']].rename(
        columns={
            'Target Column': 'COLUMN_NAME',
            'Nullable': 'IS_NULLABLE'
        }    )
    df_source_dtypes = df_source_dtypes.astype(df_target_dtypes.dtypes.to_dict())
    df_compare = df_source_dtypes.merge(df_target_dtypes, on="COLUMN_NAME", suffixes=('_source', '_target'))
    mismatched = df_compare[df_compare['IS_NULLABLE_source'] != df_compare['IS_NULLABLE_target']]
    mismatched.to_excel("reports/Nullable_Mismatch_Report.xlsx", index=False)
    assert mismatched.empty, f"\nNullable mismatches found:\n{mismatched.to_string(index=False)}"

def test_DataValidations():
    df_source['phone'] = df_source['phone'].astype(str)
    df_target['phone'] = df_target['phone'].astype(str)

    df_source.set_index("customer_id", inplace=True)
    df_target.set_index("customer_id", inplace=True)


    diff = df_source.compare(df_target)
    if not diff.empty:
        # Save mismatch to Excel
        diff.to_excel("reports/ColumnWise_Mismatch_Report.xlsx", sheet_name="Mismatch")

        with open("reports/ColumnWise_Mismatch_Report.html", "w", encoding="utf-8") as f:
            f.write(diff.to_html(border=1, justify="center", index=True))
    assert df_source.equals(df_target), f"Mismatch:\n{diff}"

