create table stg_stock_info_list
(   stock_ticker        varchar2(20),
    stock_name          varchar2(100),
    price_earliest_dt   date,
    price_latest_dt     date,
    nifty50             number,
    sensex              number,
    other               number);

create table stock_info_list
(   stock_ticker        varchar2(20),
    stock_name          varchar2(100),
    price_earliest_dt   date,
    price_latest_dt     date,
    nifty50             number,
    sensex              number,
    other               number);
alter table stock_info_list add CONSTRAINT stock_info_list_pk PRIMARY KEY (stock_ticker) enable;

create table stock_price_data
(   stock_ticker        varchar2(20),
    business_date       date,
    price_high          number,
    price_low           number,
    price_open          number,
    price_close         number,
    volume              number,
    adj_close           number,
    dma_200             number,
    dma_50              number,
    dma_10              number,
    dma_8               number,
    constraint stock_price_data_pk primary key (stock_ticker,business_date)
)
PARTITION BY LIST (stock_ticker) AUTOMATIC
(
  PARTITION part_maruti VALUES ('MARUTI.NS')
);


create table findings 
(   stock_ticker        varchar2(20),
    business_date       date,
    finding_type        varchar2(50),
    full_discription    varchar2(1000),
    primary key (stock_ticker,business_date,finding_type)
);


create table stg_stock_price_data as select * from stock_price_data;
select * from stg_stock_price_data;

get_stock_price_dt_range('MARUTI.BO',datetime.datetime(2009, 9, 1),datetime.datetime(2019, 03, 16))
get_stock_price_dt_range('AXISBANK.NS',datetime.datetime(2018, 9, 1),datetime.datetime(2019, 04, 01))
# get_stock_price_dt_range('INFY.NS',datetime.datetime(2018, 9, 1),datetime.datetime(2019, 03, 16))



begin
    finance_analysis.find_candle_stick_pattern;
end;
/

select * from findings;
