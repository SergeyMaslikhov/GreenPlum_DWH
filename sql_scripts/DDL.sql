create schema final_proj authorization super;
DROP TABLE IF EXISTS final_proj.dim_terminals_hist;
DROP TABLE IF EXISTS final_proj.dim_cards_hist;
DROP TABLE IF EXISTS final_proj.dim_accounts_hist;
DROP TABLE IF EXISTS final_proj.dim_clients_hist;
DROP TABLE IF EXISTS final_proj.dim_terminals;
DROP TABLE IF EXISTS final_proj.dim_cards;
DROP TABLE IF EXISTS final_proj.dim_accounts;
DROP TABLE IF EXISTS final_proj.dim_clients;
DROP TABLE IF EXISTS final_proj.fact_transactions;
DROP TABLE IF EXISTS final_proj.report;
DROP TABLE IF EXISTS final_proj.denormalized;

--table for loading denormalized data from excel
CREATE TABLE final_proj.denormalized(	
	trans_id text,	
	trans_date timestamp,	
	card_num  text,	
	account text,	
	account_valid_to date,
	client text,	
	last_name text, 	
	first_name text,	
	patrinymic text,
	date_of_birth date, 
	passport text, 
	passport_valid_to date,	
	phone text,	
	oper_type text,
	amount numeric,	
	oper_result text,	
	terminal text,	
	terminal_type text,	
	city text,	
	address text
)
with (
	appendonly=true,
	orientation=row)
distributed by (trans_id);

--fact_transcations: AO row type (most frequent inserts in schema, 
-- almost all columns are needed in data mart selects, relatively small number of columns)
create table final_proj.fact_transactions (
	trans_id text, 
	trans_date timestamp,
	card_num text,
	oper_type text,
	amt numeric,
	oper_result text,
	terminal text
)
with (
	appendonly=true,
	orientation=row,
	compresstype=zlib,
	compresslevel=5
)
distributed by (trans_id)
partition by range (trans_date)
(
-- made for month with interval by day as 
-- an example (make more for real prod)
start (date'2020-05-01')
end (date'2020-06-01') exclusive 
every (interval '1 day')
);

--dim table are heap due to update necessity
--scd2 type
create table final_proj.dim_terminals_hist(
	terminal_id text,
	terminal_type text,
	terminal_city text,
	terminal_address text,
	start_dt timestamp,
	end_dt timestamp
)
distributed by (terminal_id);

create table final_proj.dim_cards_hist(
	card_num text,
	account_num text,
	start_dt timestamp,
	end_dt timestamp
)
distributed by (card_num);

create table final_proj.dim_accounts_hist(
	account_num text,
	valid_to date,
	client text,
	start_dt timestamp,
	end_dt timestamp
)
distributed by (account_num);

create table final_proj.dim_clients_hist(
	client_id text,
	last_name text,
	first_name text,
	patrinymic text,
	date_of_birth date,
	passport_num text,
	passport_valid_to date,
	phone text,
	start_dt timestamp,
	end_dt timestamp
)
distributed by (client_id);

--scd1
create table final_proj.dim_terminals(
	terminal_id text,
	terminal_type text,
	terminal_city text,
	terminal_address text,
	create_dt timestamp,
	update_dt timestamp
)
distributed by (terminal_id);

create table final_proj.dim_cards(
	card_num text,
	account_num text,
	create_dt timestamp,
	update_dt timestamp
)
distributed by (card_num);

create table final_proj.dim_accounts(
	account_num text,
	valid_to date,
	client text,
	create_dt timestamp,
	update_dt timestamp
)
distributed by (account_num);

create table final_proj.dim_clients(
	client_id text,
	last_name text,
	first_name text,
	patrinymic text,
	date_of_birth date,
	passport_num text,
	passport_valid_to date,
	phone text,
	create_dt timestamp,
	update_dt timestamp
)
distributed by (client_id);

--table for data mart
create table final_proj.report(
	fraud_dt timestamp,
	passport text,
	fio text,
	phone text,
	fraud_type text,
	report_dt timestamp)
with (
	appendonly=true,
	orientation=row,
	compresstype=zlib,
	compresslevel=5
)
--all columns could contain non unique values and we dont need to join this table
distributed randomly;

