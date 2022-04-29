--function for frauds with selection of amounts
CREATE OR REPLACE FUNCTION final_proj.fn_find_summ_frauds() 
RETURNS VOID
as $$
declare
	z record;
	isfound boolean :=false;
    counter integer:=0;
    zero_on_prev boolean:=false;
    cur_client text;
    sec_count integer:=0;
   --string to execute bulk load
   	ins_query text :='INSERT INTO final_proj.report (fraud_dt, passport, fio, phone, fraud_type, report_dt) values ';
   
begin
    -- client to start with
    select min(client) into cur_client from stg_denormalized_data;
   
    for z in (
    --using with to prevent unnecessary recomputing of time_diff in sec_diff
    with time_amount_diff as 
			(select 
				client, 
				trans_date as fraud_dt, 
				passport_num as passport, 
				fio, 
				phone, 
				current_timestamp as report_dt, 
				oper_result, 
				coalesce(amount - lag(amount) over(partition by client order by trans_date), -1) amount_diff,
				trans_date - lag(trans_date) over(partition by client order by trans_date) as time_diff 
			from stg_denormalized_data
			--take 20 min from previous day
			where trans_date >= (select to_timestamp(max(trans_date), 'YYYY-MM-DD 00:00:00') - interval '20 minute' from stg_denormalized_data))
			select 
				client, 
				fraud_dt, 
				passport, 
				fio, 
				phone, 
				report_dt, 
				client, 
				oper_result, 
				amount_diff, 
				coalesce(date_part('hour', time_diff)*3600 + date_part('minute', time_diff)*60 + date_part('second', time_diff),0) as sec_diff 
			from time_amount_diff
			order by client, fraud_dt) loop
	
        -- start new counter of fraud chain in case old one is finished
        -- check if this operation is suitable to be first in chain
        if zero_on_prev and z.oper_result = 'Отказ' then
            counter := 1;
            cur_client := z.client;
            zero_on_prev := false;
        -- check if this operation is not suitable to be first in chain
        elsif zero_on_prev and z.oper_result = 'Успешно' then
            sec_count := 0;
            counter := 0;
            cur_client := z.client;
            zero_on_prev := true;
        -- continue old chain
        elsif not zero_on_prev then
            sec_count := sec_count + z.sec_diff;
            -- check if we met new client
            if z.client != cur_client then
                cur_client := z.client;
                sec_count := 0;
                if z.oper_result = 'Отказ' then
                    counter := 1;
                elsif z.oper_result = 'Успешно' then
                    counter := 0;
                    zero_on_prev := true;
                end if;
            -- check if this operation is the last in fraud chain
            elsif counter >= 3 and z.oper_result = 'Успешно' and sec_count < 1200 then
            	isfound := true;
                counter := 0;
                sec_count := 0;
               	ins_query := ins_query || '(to_timestamp(' || quote_literal(z.fraud_dt) || ', ''YYYY-MM-DD HH24:MI:SS''),' || quote_literal(z.passport) || ','
               	|| quote_literal(z.fio) || ',' || quote_literal(z.phone) || ',' || quote_literal('Попытка подбора сумм') || ',to_timestamp(' 
               	|| quote_literal(z.report_dt) || ', ''YYYY-MM-DD HH24:MI:SS'')),';
                zero_on_prev := true;
            -- operation with success can't be first in new chain
            elsif z.oper_result = 'Успешно' then
                counter := 0;
                sec_count := 0;
                zero_on_prev := true;
            -- operation with wrong amoun_diff or exceeded time can be first in new chain
            elsif z.amount_diff >= 0 or sec_count >= 1200 then
                counter := 1;
                sec_count := 0;
                zero_on_prev := false;
            -- increase counter if all conditions are met
            elsif z.amount_diff < 0 and sec_count < 1200 and z.oper_result = 'Отказ' then
                counter := counter + 1;
                zero_on_prev := false;
            end if;
        end if;
    end loop;
   if isfound then
   ins_query := rtrim(ins_query, ',') || ';';
   execute ins_query;
   end if;
end;
$$
language plpgsql;

--function which adds data for all types of frauds in report
create or replace function final_proj.fn_add_report_data(scd_type text)
	returns void
	as $$
begin
	drop table if exists stg_denormalized_data;
	if scd_type = 'scd2' then
		create temporary table stg_denormalized_data 
		-- append only for faster i\o
		with (
			appendonly=true,
			orientation=row) 
		as (
			--utd stands for up-to-date
			--joining valid data to build data mart
			with utd_terminals as 
				(select 
					terminal_id, 
					terminal_city 
				from final_proj.dim_terminals_hist dth 
				where end_dt is null),
			utd_transactions as 
				(select 
					trans_id, 
					trans_date, 
					card_num, 
					amt, 
					oper_result, 
					terminal 
				from final_proj.fact_transactions ft 
				-- take the last day - hour for 3rd fraud type, as data mart is built cumulatively
				where trans_date >= (select to_timestamp(max(trans_date), 'YYYY-MM-DD 00:00:00') - interval '1 hour'  from final_proj.fact_transactions)),
			utd_cards as 
				(select 
					card_num, 
					account_num 
				from final_proj.dim_cards_hist 
				where end_dt is null),
			utd_accounts as 
				(select 
					account_num, 
					valid_to, 
					client 
				from final_proj.dim_accounts_hist 
				where end_dt is null),
			utd_clients as 
				(select 
					client_id, 
					last_name || ' ' || first_name || ' ' || patrinymic as fio, 
					phone, 
					passport_valid_to, 
					passport_num
				from final_proj.dim_clients_hist
				where end_dt is null)
			select 
				te.terminal_city, 
				tr.trans_id, 
				tr.trans_date, 
				tr.card_num, 
				tr.amt as amount, 
				tr.oper_result, 
				a.valid_to, 
				a.client, 
				cl.fio,
				cl.phone, 
				cl.passport_valid_to, 
				cl.passport_num
			from utd_terminals te 
				inner join utd_transactions tr
				on te.terminal_id = tr.terminal
					inner join utd_cards c
					on c.card_num = tr.card_num
						inner join utd_accounts a
						on a.account_num = c.account_num
							inner join utd_clients cl
							on cl.client_id = a.client
		)
		distributed by (trans_id);
	elsif scd_type = 'scd1' then
		create temporary table stg_denormalized_data 
		-- append only for faster i\o
		with (
			appendonly=true,
			orientation=row) 
		as (
			--utd stands for up-to-date
			--joining valid data to build data mart
			with utd_terminals as 
				(select 
					terminal_id, 
					terminal_city 
				from final_proj.dim_terminals dt 
				),
			utd_transactions as 
				(select 
					trans_id, 
					trans_date, 
					card_num, 
					amt, 
					oper_result, 
					terminal 
				from final_proj.fact_transactions ft 
				-- take the last day - hour for 3rd fraud type, as data mart is built cumulatively
				where trans_date >= (select to_timestamp(max(trans_date), 'YYYY-MM-DD 00:00:00') - interval '1 hour' from final_proj.fact_transactions)),
			utd_cards as 
				(select 
					card_num, 
					account_num 
				from final_proj.dim_cards 
				),
			utd_accounts as 
				(select 
					account_num, 
					valid_to, 
					client 
				from final_proj.dim_accounts
				),
			utd_clients as 
				(select 
					client_id, 
					last_name || ' ' || first_name || ' ' || patrinymic as fio, 
					phone, 
					passport_valid_to, 
					passport_num
				from final_proj.dim_clients
				)
			select 
				te.terminal_city, 
				tr.trans_id, 
				tr.trans_date, 
				tr.card_num, 
				tr.amt as amount, 
				tr.oper_result, 
				a.valid_to, 
				a.client, 
				cl.fio,
				cl.phone, 
				cl.passport_valid_to, 
				cl.passport_num
			from utd_terminals te 
				inner join utd_transactions tr
				on te.terminal_id = tr.terminal
					inner join utd_cards c
					on c.card_num = tr.card_num
						inner join utd_accounts a
						on a.account_num = c.account_num
							inner join utd_clients cl
							on cl.client_id = a.client
		)
		distributed by (trans_id);
	end if;
	
	insert into final_proj.report (fraud_dt, passport, fio, phone, fraud_type, report_dt)
	select trans_date as fraud_dt, 
		passport_num as passport, 
		fio, 
		phone, 
		case 
			when trans_date > passport_valid_to then 'Совершение операции при просроченном паспорте'
			when trans_date > valid_to then 'Совершение операции при недействующем договоре'
			end as fraud_type, 
		current_timestamp as report_dt from stg_denormalized_data
	where (trans_date > passport_valid_to or trans_date > valid_to) and
	-- take only last day
	trans_date >= (select to_timestamp(max(trans_date), 'YYYY-MM-DD 00:00:00') from stg_denormalized_data)
	union all
	select fraud_dt, 
			passport,
			fio,
			phone,
			'Совершение операции в разных городах в течение 1 часа',
			report_dt
	from 
		(select 
			trans_date as fraud_dt, 
			passport_num as passport,
			fio,
			phone,
			current_timestamp as report_dt, 
			terminal_city,
			-- city of previous transaction 
			lag(terminal_city) over(partition by client order by trans_date) prev_city,
			-- difference in hours between this and previous transactions
			date_part('hour', trans_date - lag(trans_date) over(partition by client order by trans_date)) hour_diff
		from stg_denormalized_data) trans_by_cities
	where terminal_city != prev_city and hour_diff = 0;
	
	perform final_proj.fn_find_summ_frauds();
end;
$$
language plpgsql;
