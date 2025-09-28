-- create table my_table (
--     name varchar(255),
--     location varchar(50)
-- )
-- using parquet
-- location '/Users/andrewpurchase/Documents/electicity-demand/warehouse';

create table my_table2 (
    name varchar(255),
    location varchar(50)
)
using parquet
location '/Users/andrewpurchase/Documents/electicity-demand/data/test_data/';
-- drop table my_table2

select * from parquet.`../test_data/test_df_write`
select * from spark_catalog.default.my_table2