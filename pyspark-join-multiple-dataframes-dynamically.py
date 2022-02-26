from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("abc").getOrCreate()

lst1 = [[1, 2, 3], ["A", "B", "C"], ["aa", "bb", "cc"]]
lst2 = [[2, 3, 4], ["A", "B", "C"], ["aa", "bb", "cc"]]
lst3 = [[1, 2, 4], ["A", "B", "C"], ["aa", "bb", "cc"]]
lst4 = [[1, 2, 4], ["A", "B", "C"], ["aa", "bb", "cc"]]

R1 = Row("A1", "A2", "A3")
R2 = Row("B1", "B2", "B3")
R3 = Row("C1", "C2", "C3")
R4 = Row("D1", "D2", "D3")
df1 = spark.sparkContext.parallelize([R1(*r) for r in zip(*lst1)]).toDF().alias("df1")
df2 = spark.sparkContext.parallelize([R2(*r) for r in zip(*lst2)]).toDF().alias("df2")
df3 = spark.sparkContext.parallelize([R3(*r) for r in zip(*lst3)]).toDF().alias("df3")
df4 = spark.sparkContext.parallelize([R4(*r) for r in zip(*lst4)]).toDF().alias("df4")

list_tup = [
    (df1, df2, "df1.A1", "df2.B1"),
    (df2, df3, "df2.B1", "df3.C1"),
    (df1, df3, "df1.A1", "df3.C1"),
    (df1, df4, "df1.A1", "df4.d1"),
]

dict_df = {"df1":df1, "df2":df2, "df3":df3,"df4":df4}
list_condition = [
    ("df1", "df2", "df1.A1", "df2.B1"),
    ("df2", "df3", "df2.B1", "df3.C1"),
    ("df1", "df3", "df1.A1", "df3.C1"),
    ("df1", "df4", "df1.A1", "df4.D1")
]

def join_multiple_dfs(df_named_dict,join_condition_list):
    updated_df_list = []
    for item in join_condition_list:
        updated_df_list.append((df_named_dict.get(item[0]),df_named_dict.get(item[1]),item[2],item[3]))
    df_1 = updated_df_list[0][0]
    for x in updated_df_list:
        df_1 = x[0].join(x[1], on=F.col(x[2]) == F.col(x[3]), how="left_outer")
    return df_1

join_multiple_dfs(df_named_dict = dict_df,join_condition_list=list_condition).show()


# +---+---+---+----+----+----+
# | A1| A2| A3|  D1|  D2|  D3|
# +---+---+---+----+----+----+
# |  1|  A| aa|   1|   A|  aa|
# |  2|  B| bb|   2|   B|  bb|
# |  3|  C| cc|null|null|null|
# +---+---+---+----+----+----+
