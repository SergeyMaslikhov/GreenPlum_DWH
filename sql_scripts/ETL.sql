create or replace function final_proj.fn_normalize_transactions()
	returns void
	as $$
begin
	--filling scd2 type
	drop table if exists stg_terminals;
	drop table if exists stg_cards;
	drop table if exists stg_accounts;
	drop table if exists stg_clients;
	
	create temporary table stg_terminals
	-- append only for faster i\o
	with (
		appendonly=true,
		orientation=row) 
		as(
		-- partition by terminal_id in case if dimension changes during one day more than once
			select f.*, 
			lead(start_dt) over(PARTITION BY terminal_id ORDER BY start_dt) AS end_dt
			from (select 
				terminal as terminal_id,
				terminal_type,
				city as terminal_city,
				address as terminal_address,
				min(trans_date) AS start_dt -- set start_dt not with date of etl process, but with last transaction date
			from final_proj.denormalized
			group by terminal, terminal_type, city, address) f
	)
	distributed by (terminal_id);
	
	create temporary table stg_accounts 
	with (
		appendonly=true,
		orientation=row)
		as(
			select f.*, 
			lead(start_dt) over(PARTITION BY account_num ORDER BY start_dt) AS end_dt
			from (select 
				account as account_num,
				account_valid_to as valid_to,
				client,
				min(trans_date) AS start_dt
			from final_proj.denormalized
			group by account, account_valid_to, client) f
	)
	distributed by (account_num);
	
	create temporary table stg_cards with (
		appendonly=true,
		orientation=row)
		as(
			select f.*, 
			lead(start_dt) over(PARTITION BY card_num ORDER BY start_dt) AS end_dt
			from (select
				card_num,
				account as account_num,
				min(trans_date) AS start_dt
			from final_proj.denormalized
			group by card_num, account) f
			)
	distributed by (card_num);
	
	create temporary table stg_clients 
	with (
		appendonly=true,
		orientation=row) 
		as(
			select f.*, 
			lead(start_dt) over(PARTITION BY client_id ORDER BY start_dt) AS end_dt
			from (select
				client as client_id,
				last_name,
				first_name,
				patrinymic,
				date_of_birth,
				passport as passport_num,
				passport_valid_to,
				phone,
				min(trans_date) AS start_dt
			from final_proj.denormalized
			group by client, last_name, first_name, patrinymic,
				date_of_birth, passport, passport_valid_to, phone) f
			)
	distributed by (client_id);
	
	update final_proj.dim_terminals_hist c
	set end_dt = sc.start_dt
	from (
		--filter rows that already in dim_terminals_hist
		select *
		from stg_terminals stg
		where not exists (
		select 1
		from final_proj.dim_terminals_hist c
		where c.terminal_id = stg.terminal_id and
		c.terminal_type = stg.terminal_type and
		c.terminal_city = stg.terminal_city and
		c.terminal_address = stg.terminal_address
		)
	) sc
	where sc.terminal_id = c.terminal_id and
	--update row if it has no end_dt in dim_terminals_hist
	c.end_dt is null 
	and sc.end_dt is null;
	
	insert into final_proj.dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, start_dt, end_dt) 
	select terminal_id, terminal_type, terminal_city, terminal_address, start_dt, end_dt 
	from (
		select *
		from stg_terminals stg
		where not exists (
		select 1
		from final_proj.dim_terminals_hist c
		where c.terminal_id = stg.terminal_id and
		c.terminal_type = stg.terminal_type and
		c.terminal_city = stg.terminal_city and
		c.terminal_address = stg.terminal_address
		)
	) sc;
	
	insert into final_proj.fact_transactions (trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal)
	select 
		trans_id, 
		trans_date,
		card_num,
		oper_type,
		amount as amt,
		oper_result,
		terminal
	from final_proj.denormalized;
	
	update final_proj.dim_accounts_hist c
	set end_dt = sc.start_dt
	from (
		select *
		from stg_accounts stg
		where not exists (
		select 1
		from final_proj.dim_accounts_hist c
		where c.account_num = stg.account_num and
		c.valid_to = stg.valid_to and
		c.client = stg.client
		)
	) sc
	where sc.account_num = c.account_num and
	c.end_dt is null 
	and sc.end_dt is null;
	
	insert into final_proj.dim_accounts_hist (account_num, valid_to, client, start_dt, end_dt) 
	select account_num, valid_to, client, start_dt, end_dt 
	from (
		select *
		from stg_accounts stg
		where not exists (
		select 1
		from final_proj.dim_accounts_hist c
		where c.account_num = stg.account_num and
		c.valid_to = stg.valid_to and
		c.client = stg.client
		)
	) sc;
	
	update final_proj.dim_cards_hist c
	set end_dt = sc.start_dt
	from (
		select *
		from stg_cards stg
		where not exists (
		select 1
		from final_proj.dim_cards_hist c
		where c.account_num = stg.account_num and
		c.card_num = stg.card_num
		)
	) sc
	where sc.card_num = c.card_num and
	c.end_dt is null 
	and sc.end_dt is null;
	
	insert into final_proj.dim_cards_hist (card_num, account_num, start_dt, end_dt) 
	select card_num, account_num, start_dt, end_dt
	from (
		select *
		from stg_cards stg
		where not exists (
		select 1
		from final_proj.dim_cards_hist c
		where c.account_num = stg.account_num and
		c.card_num = stg.card_num
		)
	) sc;
	
	update final_proj.dim_clients_hist c
	set end_dt = sc.start_dt
	from (
		select *
		from stg_clients stg
		where not exists (
		select 1
		from final_proj.dim_clients_hist c
		where c.client_id = stg.client_id and
		c.passport_num = stg.passport_num and
		c.last_name = stg.last_name and
		c.first_name = stg.first_name and
		c.patrinymic = stg.patrinymic and
		c.passport_valid_to = stg.passport_valid_to and
		c.passport_num = stg.passport_num and
		c.date_of_birth = stg.date_of_birth
		and c.phone = stg.phone
		)
	) sc
	where sc.client_id = c.client_id and
	c.end_dt is null 
	and sc.end_dt is null;
	
	insert into final_proj.dim_clients_hist (client_id, last_name, first_name, patrinymic,
		date_of_birth, passport_num, passport_valid_to, phone, start_dt, end_dt) 
	select client_id, last_name, first_name, patrinymic,
		date_of_birth, passport_num, passport_valid_to, phone, start_dt, end_dt
	from (
		select *
		from stg_clients stg
		where not exists (
		select 1
		from final_proj.dim_clients_hist c
		where c.client_id = stg.client_id and
		c.passport_num = stg.passport_num and
		c.last_name = stg.last_name and
		c.first_name = stg.first_name and
		c.patrinymic = stg.patrinymic and
		c.passport_valid_to = stg.passport_valid_to and
		c.passport_num = stg.passport_num and
		c.date_of_birth = stg.date_of_birth
		and c.phone = stg.phone
		)
	) sc;
	
	--filling scd1 type

	drop table if exists stg_terminals;
	drop table if exists stg_cards;
	drop table if exists stg_accounts;
	drop table if exists stg_clients;
	create temporary table stg_terminals
	-- append only for faster i\o
	with (
		appendonly=true,
		orientation=row) 
		as(
		-- partition by terminal_id in case if dimension changes during one day more than once
			select distinct 
				terminal_id,
				last_value(terminal_type) over(partition by terminal_id order by create_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as terminal_type,
				last_value(terminal_city) over(partition by terminal_id order by create_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as terminal_city,
				last_value(terminal_address) over(partition by terminal_id order by create_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as terminal_address,
				min(create_dt) over(partition by terminal_id) as create_dt,
				case 
					when max(create_dt) over(partition by terminal_id) = min(create_dt) over(partition by terminal_id) then null
					when max(create_dt) over(partition by terminal_id) != min(create_dt) over(partition by terminal_id) 
						then max(create_dt) over(partition by terminal_id)
				end as update_dt
				from (select 
					terminal as terminal_id,
					terminal_type,
					city as terminal_city,
					address as terminal_address,
					min(trans_date) AS create_dt -- set start_dt not with date of etl process, but with first transaction date
				from final_proj.denormalized
				group by terminal, terminal_type, city, address
			order by terminal_id) f
			
	)
	distributed by (terminal_id);
	
	create temporary table stg_accounts 
	with (
		appendonly=true,
		orientation=row)
		as(
			select
			account_num,
			last_value(valid_to) over(partition by account_num order by create_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as valid_to,
			last_value(client) over(partition by account_num order by create_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as client,
			min(create_dt) over(partition by account_num) as create_dt,
			case 
				when max(create_dt) over(partition by account_num) = min(create_dt) over(partition by account_num) then null
				when max(create_dt) over(partition by account_num) != min(create_dt) over(partition by account_num) 
					then max(create_dt) over(partition by account_num)
			end as update_dt
			from (select 
				account as account_num,
				account_valid_to as valid_to,
				client,
				min(trans_date) AS create_dt
			from final_proj.denormalized
			group by account, account_valid_to, client) f
	)
	distributed by (account_num);
	
	create temporary table stg_cards with (
		appendonly=true,
		orientation=row)
		as(
			select card_num, 
			last_value(account_num) over(partition by card_num order by create_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as account_num,
			min(create_dt) over(partition by card_num) as create_dt,
			case 
				when max(create_dt) over(partition by card_num) = min(create_dt) over(partition by card_num) then null
				when max(create_dt) over(partition by card_num) != min(create_dt) over(partition by card_num) 
					then max(create_dt) over(partition by card_num)
			end as update_dt
			from (select
				card_num,
				account as account_num,
				min(trans_date) AS create_dt
			from final_proj.denormalized
			group by card_num, account) f
			)
	distributed by (card_num);
	
	create temporary table stg_clients 
	with (
		appendonly=true,
		orientation=row) 
		as(
			select 
				client_id,
				last_value(last_name) over(partition by client_id order by create_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_name,
				last_value(first_name) over(partition by client_id order by create_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_name,
				last_value(patrinymic) over(partition by client_id order by create_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as patrinymic,
				last_value(date_of_birth) over(partition by client_id order by create_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_of_birth,
				last_value(passport_num) over(partition by client_id order by create_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as passport_num,
				last_value(passport_valid_to) over(partition by client_id order by create_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as passport_valid_to,
				last_value(phone) over(partition by client_id order by create_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as phone,
				min(create_dt) over(partition by client_id) as create_dt,
				case 
				when max(create_dt) over(partition by client_id) = min(create_dt) over(partition by client_id) then null
				when max(create_dt) over(partition by client_id) != min(create_dt) over(partition by client_id) 
					then max(create_dt) over(partition by client_id)
			end as update_dt
			from (select
				client as client_id,
				last_name,
				first_name,
				patrinymic,
				date_of_birth,
				passport as passport_num,
				passport_valid_to,
				phone,
				min(trans_date) AS create_dt
			from final_proj.denormalized
			group by client, last_name, first_name, patrinymic,
				date_of_birth, passport, passport_valid_to, phone) f
			)
	distributed by (client_id);

	update final_proj.dim_terminals c
	set 
		terminal_city = sc.terminal_city,
		terminal_address = sc.terminal_address,
		update_dt = coalesce(sc.update_dt, sc.create_dt)
	from stg_terminals sc
	where c.terminal_id = sc.terminal_id and (sc.terminal_city != c.terminal_city or sc.terminal_address != c.terminal_address);
	
	insert into final_proj.dim_terminals (terminal_id, terminal_type, terminal_city, terminal_address, create_dt, update_dt) 
	select terminal_id, terminal_type, terminal_city, terminal_address, create_dt, update_dt 
	from (
		select *
		from stg_terminals stg
		where not exists (
		select 1
		from final_proj.dim_terminals c
		where c.terminal_id = stg.terminal_id
		)
	) sc; 

	update final_proj.dim_accounts c
	set 
		valid_to = sc.valid_to,
		client = sc.client,
		update_dt = coalesce(sc.update_dt, sc.create_dt)
	from stg_accounts sc
	where c.account_num = sc.account_num and (sc.valid_to != c.valid_to or sc.client != c.client);
	
	insert into final_proj.dim_accounts (account_num, valid_to, client, create_dt, update_dt) 
	select account_num, valid_to, client, create_dt, update_dt
	from (
		select *
		from stg_accounts stg
		where not exists (
		select 1
		from final_proj.dim_accounts c
		where c.account_num = stg.account_num
		)
	) sc; 

	update final_proj.dim_cards c
	set 
		account_num = sc.account_num,
		update_dt = coalesce(sc.update_dt, sc.create_dt)
	from stg_cards sc
	where c.card_num = sc.card_num and sc.account_num != c.account_num ;
	
	insert into final_proj.dim_cards (card_num, account_num, create_dt, update_dt) 
	select card_num, account_num, create_dt, update_dt
	from (
		select *
		from stg_cards stg
		where not exists (
		select 1
		from final_proj.dim_cards c
		where c.card_num = stg.card_num
		)
	) sc; 

	update final_proj.dim_clients c
	set 
		last_name = sc.last_name,
		first_name = sc.first_name,
		patrinymic = sc.patrinymic,
		date_of_birth = sc.date_of_birth,
		passport_num = sc.passport_num,
		passport_valid_to = sc.passport_valid_to,
		phone = sc.phone,
		update_dt = coalesce(sc.update_dt, sc.create_dt)
	from stg_clients sc
	where c.client_id = sc.client_id and (c.last_name != sc.last_name or
		c.first_name != sc.first_name or
		c.patrinymic != sc.patrinymic or
		c.date_of_birth != sc.date_of_birth or
		c.passport_num != sc.passport_num or
		c.passport_valid_to != sc.passport_valid_to or
		c.phone != sc.phone);
	
	insert into final_proj.dim_clients (client_id, last_name, first_name, patrinymic, 
	date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt) 
	select client_id, last_name, first_name, patrinymic, 
	date_of_birth, passport_num create_dt, passport_valid_to, phone, create_dt, update_dt
	from (
		select *
		from stg_clients stg
		where not exists (
		select 1
		from final_proj.dim_clients c
		where c.client_id = stg.client_id
		)
	) sc; 
truncate final_proj.denormalized;
end;
$$
language plpgsql;