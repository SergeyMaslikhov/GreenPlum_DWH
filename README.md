<h1 align="center">GreenPlum Data Warehouse</h1>

## Description

Purpose of this project is building data warehouse and pipeline capable of loading data from external source, normalizing to fact\dimensions tables in [SCD1(2)](https://ru.wikipedia.org/wiki/%D0%9C%D0%B5%D0%B4%D0%BB%D0%B5%D0%BD%D0%BD%D0%BE_%D0%BC%D0%B5%D0%BD%D1%8F%D1%8E%D1%89%D0%B5%D0%B5%D1%81%D1%8F_%D0%B8%D0%B7%D0%BC%D0%B5%D1%80%D0%B5%D0%BD%D0%B8%D0%B5) format and building data mart for detecting fraud activity.

## Input data and expected result

Input data for DWH is table with denormalized transactions info ([transactions_03052020.xlsx](https://github.com/SergeyMaslikhov/GreenPlum_DWH/blob/main/data/transactions_03052020.xlsx) for example). It includes data for past three days of current month. Expected result is ETL process that allows to deliver new data to DWH from input file and cumulatively add in data mart suspected for fraud who:
- Make transactions with an expired passport
- Make transactions with an expired account
- Make transactions in different cities within 1 hour
- Attempt to select minimal amounts. Within 20 minutes, there are more than 3 operations with the following template - each subsequent one is less than the previous one, while all are rejected except last. The last operation (successful) in such a chain is considered fraudulent.

## Architecture

Data schema and role of functions:
<img src="Data_scheme.png" height='110%'>

## Usage

1) execute DDL.sql to create all necessary tables
2) execute ETL.sql to define final_proj.fn_normalize_transactions for normalizing data from final_proj.denormalized
3) execute Report.sql to define function for building data mart

### Step-by-step loading data and building data mart from scd2 format:

- to insert data for the last day from excel to denormalized table
```shell 
>>>python insert_from_excel.py -f transactions_01052020.xlsx
```
- to add data in dim tables and fact_transactions
```sql
select final_proj.fn_normalize_transactions();
```
- to add new frauds in data mart
```sql
select final_proj.fn_add_report_data('scd2');
```

### Step-by-step loading data and building data mart from scd1 format:

- to insert data for the last day from excel to denormalized table
```shell
>>>python insert_from_excel.py -f transactions_01052020.xlsx
```
- to add data in dim tables and fact_transactions
```sql
select final_proj.fn_normalize_transactions();
```
- to add new frauds in data mart
```sql
select final_proj.fn_add_report_data('scd1');
```
### To visualize data mart use:

```shell
>>>python plot_report.py
```
## Project setup

- To establish a database [GreenPlum 4.3.15.0](https://network.pivotal.io/products/vmware-tanzu-greenplum#/releases/6120/file_groups/630) for VMware was used
- Sql code was written in [DBeaver](https://dbeaver.io/)

## Python requirements

To install necessary packages in Python use:
```shell
>>>pip install -r requirements.txt
```
