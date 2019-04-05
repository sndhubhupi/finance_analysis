create or replace package calculations
as

    procedure calc_moving_average_200;
    procedure calc_moving_average_50;
    procedure calc_moving_average_10;
    procedure calc_moving_average_8;
    procedure update_earliest_latest_dt;    

end calculations;
/

create or replace package body calculations
as
   const_smoothing_factor   number := 0.002;
   v_smoothing_value        number;
   v_row_count              number;
   v_max_date               date;
   v_yesterday_date         date;
   v_price_open             number;
   v_price_close            number;
   v_price_close_2          number;
   v_price_open_2           number;
   v_full_discription       varchar2(1000);
   v_green_percentage       number;
   v_red_percentage         number;

    procedure calc_moving_average_200
    as
    begin
        update stock_price_data set dma_200 =
            (select dma_200 from
                (select stock_ticker,business_date,
                    avg(price_close) over (partition by stock_ticker order by business_date rows 200 preceding ) as dma_200,
                    count(price_close) over (partition by stock_ticker order by business_date rows 200 preceding ) as cnt
                  from stock_price_data ) tab
        where tab.cnt > 200
        and tab.stock_ticker = stock_price_data.stock_ticker
        and trunc(tab.business_date) = trunc(stock_price_data.business_date));
        commit;
    end calc_moving_average_200;

    procedure calc_moving_average_50
    as
    begin
        update stock_price_data set dma_50 =
            (select dma_50 from
                (select stock_ticker,business_date,
                    avg(price_close) over (partition by stock_ticker order by business_date rows 50 preceding ) as dma_50,
                    count(price_close) over (partition by stock_ticker order by business_date rows 50 preceding ) as cnt
                  from stock_price_data ) tab
        where tab.cnt > 50
        and tab.stock_ticker = stock_price_data.stock_ticker
        and trunc(tab.business_date) = trunc(stock_price_data.business_date));
        commit;
    end calc_moving_average_50;

    procedure calc_moving_average_10
    as
    begin
        update stock_price_data set dma_10 =
            (select dma_10 from
                (select stock_ticker,business_date,
                    avg(price_close) over (partition by stock_ticker order by business_date rows 10 preceding ) as dma_10,
                    count(price_close) over (partition by stock_ticker order by business_date rows 10 preceding ) as cnt
                  from stock_price_data ) tab
        where tab.cnt > 10
        and tab.stock_ticker = stock_price_data.stock_ticker
        and trunc(tab.business_date) = trunc(stock_price_data.business_date));
        commit;
    end calc_moving_average_10;

    procedure calc_moving_average_8
    as
    begin
        update stock_price_data set dma_8 =
            (select dma_8 from
                (select stock_ticker,business_date,
                    avg(price_close) over (partition by stock_ticker order by business_date rows 8 preceding ) as dma_8,
                    count(price_close) over (partition by stock_ticker order by business_date rows 8 preceding ) as cnt
                  from stock_price_data ) tab
        where tab.cnt > 8
        and tab.stock_ticker = stock_price_data.stock_ticker
        and trunc(tab.business_date) = trunc(stock_price_data.business_date));
        commit;

    end calc_moving_average_8;

    procedure update_earliest_latest_dt
    as
    begin
        update stock_info_list set PRICE_EARLIEST_DT = (select min(business_date) from stock_price_data
                                                            where stock_price_data.stock_ticker = stock_info_list.stock_ticker),
                                   price_latest_dt = (select max(business_date) from stock_price_data
                                                            where stock_price_data.stock_ticker = stock_info_list.stock_ticker);


    end update_earliest_latest_dt;   
    
end calculations;
/    
