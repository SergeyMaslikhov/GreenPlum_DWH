<h1 align="center">GreenPlum Data Warehouse</h1>

## Description

Purpose of this project is building data warehouse and pipeline capable for loading data from external source, normalizing to fact\dimensions tables in [SCD1(2)](https://ru.wikipedia.org/wiki/%D0%9C%D0%B5%D0%B4%D0%BB%D0%B5%D0%BD%D0%BD%D0%BE_%D0%BC%D0%B5%D0%BD%D1%8F%D1%8E%D1%89%D0%B5%D0%B5%D1%81%D1%8F_%D0%B8%D0%B7%D0%BC%D0%B5%D1%80%D0%B5%D0%BD%D0%B8%D0%B5) format and building data mart for detecting fraud activity.

## Input data and expected result

Input data for DWH is table with denormalized transactions info ([transactions_03052020.xlsx](https://github.com/SergeyMaslikhov/GreenPlum_DWH/blob/main/transactions_03052020.xlsx) for example). It includes data for past three days of current month. Expected result is built ETL process that allows to deliver new data to DWH from input file and cumulatively add in data mart suspected for fraud who:
- Make transactions with an expired passport
- Make transactions with an expired account
- Make transactions in different cities within 1 hour
- Attempt to select sums. Within 20 minutes, there are more than 3 operations with the following template - each subsequent one is less than the previous one, while all are rejected except last. The last operation (successful) in such a chain is considered fraudulent.
