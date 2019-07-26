CREATE TABLE public.investment_sentiment (
    time_date timestamp NOT NULL,
    bullish double precision,
    neutral double precision,
    bearish double precision,
    total double precision,
    bullish_8wk_mvg double precision,
    bull_bear_spread double precision,
    bullish_avg double precision,
    add_bullish_stddev double precision,
    subt_bullish_stddev double precision,
--     sp_500_wkly_high double precision,
--     sp_500_wkly_low double precision,
--     sp_500_wkly_close double precision,
    CONSTRAINT time_pkey PRIMARY KEY (time_date)
);

CREATE TABLE public.sp_500_composite_hist(
    time_date timestamp NOT NULL,
    sp_500_price double precision,
    dividend double precision,
    earnings double precision,
    CPI double precision,
    long_interest_rate double precision,
    real_price double precision,
    real_dividend double precision,
    real_earnings double precision,
    cyc_adj_PE_ratio double precision,
    CONSTRAINT time_pkey PRIMARY KEY (time_date)
);

CREATE TABLE public.us_stock_market_confidence_indices(
    time_date timestamp NOT NULL,
    valuation_indices double precision,
    valuation_indices_std_err double precision,
    crash_confidence double precision,
    crash_confidence_std_err double precision,
    buy_on_dips double precision,
    buy_on_dips_std_err double precision,
    CONSTRAINT time_pkey PRIMARY KEY (time_date)
);

CREATE TABLE public.hist_market_data(
    symbol varchar(256) NOT NULL,
    time_date timestamp NOT NULL,
    open double precision,
    close double precision,
    high double precision,
    low double precision,
    volume double precision,
    CONSTRAINT symbol_pkey PRIMARY KEY (time_date)
);



-- CREATE TABLE public.staging_events (
-- 	artist varchar(256),
-- 	auth varchar(256),
-- 	firstname varchar(256),
-- 	gender varchar(256),
-- 	iteminsession int4,
-- 	lastname varchar(256),
-- 	length numeric(18,0),
-- 	"level" varchar(256),
-- 	location varchar(256),
-- 	"method" varchar(256),
-- 	page varchar(256),
-- 	registration numeric(18,0),
-- 	sessionid int4,
-- 	song varchar(256),
-- 	status int4,
-- 	ts int8,
-- 	useragent varchar(256),
-- 	userid int4
-- );

-- CREATE TABLE public.staging_songs (
-- 	num_songs int4,
-- 	artist_id varchar(256),
-- 	artist_name varchar(256),
-- 	artist_latitude numeric(18,0),
-- 	artist_longitude numeric(18,0),
-- 	artist_location varchar(256),
-- 	song_id varchar(256),
-- 	title varchar(256),
-- 	duration numeric(18,0),
-- 	"year" int4
-- );

-- CREATE TABLE public."time" (
-- 	start_time timestamp NOT NULL,
-- 	"hour" int4,
-- 	"day" int4,
-- 	week int4,
-- 	"month" varchar(256),
-- 	"year" int4,
-- 	weekday varchar(256),
-- 	CONSTRAINT time_pkey PRIMARY KEY (start_time)
-- ) ;

