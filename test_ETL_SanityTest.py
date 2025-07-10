import pandas as pd
import connectionToDatabase as conn

df_source = pd.read_csv("J:\MyData\ETLData\customer_source.csv")
print(df_source)

targetSQL = "select * from dim_customer"
df_target = pd.read_sql(targetSQL, conn.conn)
print(df_target)

df_mapping = pd.read_excel("J:\MyData\ETLData\Customer_ETL_Mapping.xlsx", sheet_name="Source_to_Target")
print(df_mapping)

def test_countValidation():
    sourceCount = len(df_source)
    targetCount = len(df_target)

    print("Source Count" ,sourceCount)
    print("Target Count" ,targetCount)

    assert sourceCount==targetCount,f"Counts not matching, Source Count: {sourceCount} Target Count: {targetCount}"


def test_DataTypeVaidations():

    df_target_dtypesSQL = """
                        SELECT 
                            COLUMN_NAME,
                            DATA_TYPE
                        FROM 
                            INFORMATION_SCHEMA.COLUMNS
                        WHERE 
                            TABLE_NAME = 'dim_customer'
                        ORDER BY 
                            ORDINAL_POSITION """

    df_target_dtypes1 = pd.read_sql(df_target_dtypesSQL, conn.conn)
    df_target_dtypes = df_target_dtypes1[['COLUMN_NAME', 'DATA_TYPE']]

    df_source_dtypes = df_mapping[['Target Column', 'Target Type']].rename(
        columns={
            'Target Column': 'COLUMN_NAME',
            'Target Type': 'DATA_TYPE'
        }    )
    df_source_dtypes = df_source_dtypes.astype(df_target_dtypes.dtypes.to_dict())

    # Join on column name so mismatches are visible
    df_compare = df_source_dtypes.merge(df_target_dtypes, on="COLUMN_NAME", suffixes=('_source', '_target'))

    mismatched = df_compare[df_compare['DATA_TYPE_source'] != df_compare['DATA_TYPE_target']]

    mismatched.to_excel("reports/DataType_Mismatch_Report.xlsx", index=False)


    assert mismatched.empty, f"\nData type mismatches found:\n{mismatched.to_string(index=False)}"


#
# # Align dtypes
# df_source = df_source.astype(df_target.dtypes.to_dict())
#
# print("Printing", df_source.compare(df_target))
#
# assert df_source.equals(df_target), f"Mismatch:\n{df_source.compare(df_target)}"

