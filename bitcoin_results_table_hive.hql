CREATE EXTERNAL TABLE IF NOT EXISTS `nicky`.`bitcoin_results_final3`
	(`bitcoin_id` int, 
	`bitcoin_supply` string, 
	`market_cap_in_dollars` string, 
	`msupply` int, 
	`bitcoin_type` string, 
	`bitcoin_name_id` string, 
	`percent_change_in_one_hr` float, 
	`percent_change_in_day` float,
	`percent_change_one_week` float,
	`bitcoin_price` float, `bitcoin_price_usd` int, 
	`bitcoin_rank` string,
	`bitcoin_symbol` int
	) 
	ROW FORMAT DELIMITED 
	FIELDS TERMINATED BY ',' 
	STORED AS TEXTFILE 
    location '/bitcoin_pyspark_results';