create table stg_stock_info_list
(   stock_ticker        varchar2(20),
    stock_name          varchar2(100),
    price_earliest_dt   date,
    price_latest_dt     date,
    nifty50             number,
    sensex              number,
    other               number);

create table stock_info_list as select * from stg_stock_info_list;
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

create table stg_stock_price_data as select * from stock_price_data;
select * from stg_stock_price_data;

