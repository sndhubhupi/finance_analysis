create or replace package calculations
as

    procedure calc_moving_average_200;
    procedure calc_moving_average_50;
    procedure calc_moving_average_10;
    procedure calc_moving_average_8;
    procedure calc_pivot_points;
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
            (select round(dma_200,3) from
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
            (select round(dma_50,3) from
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
            (select round(dma_10,3) from
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
            (select round(dma_8,3) from
                (select stock_ticker,business_date,
                    avg(price_close) over (partition by stock_ticker order by business_date rows 8 preceding ) as dma_8,
                    count(price_close) over (partition by stock_ticker order by business_date rows 8 preceding ) as cnt
                  from stock_price_data ) tab
        where tab.cnt > 8
        and tab.stock_ticker = stock_price_data.stock_ticker
        and trunc(tab.business_date) = trunc(stock_price_data.business_date));
        commit;

    end calc_moving_average_8;

    procedure calc_pivot_points
    as
        v_pivot     number;
        v_r1        number;
        v_r2        number;
        v_s1        number;
        v_s2        number;
        v_demark_h  number;
        v_demark_l  number;
        v_high  number;
        v_low   number;
        v_close number;
        v_open  number;
        v_demark_p  number;
    begin
        -- calculate  pivot point value
        for c_stock_ticker in (select distinct stock_ticker from stock_info_list where price_earliest_dt is not null)
        loop
            select max(business_date) into v_max_date from stock_price_data where stock_ticker = c_stock_ticker.stock_ticker;
            select max(price_high),max(price_low),max(price_open),max(price_close)
            into    v_high,v_low,v_open,v_close
            from stock_price_data
            where stock_ticker =  c_stock_ticker.stock_ticker
            and  business_date between last_day(add_months(v_max_date,-2)) and last_day(add_months(v_max_date,-1)) order by 2 desc ;
            v_pivot := round((v_high+v_low+v_close)/3,3);
            v_s1    := round((v_pivot *2) - v_high,3);
            v_s2    := round(v_pivot - ( v_high - v_low ),3);
            v_r1    := round((v_pivot *2 ) - v_low,3);
            v_r2    := round(v_pivot + ( v_high - v_low),3);

            merge into stock_pivot_data spd
                using (select c_stock_ticker.stock_ticker as ticker from dual) tab
                on (spd.stock_ticker = tab.ticker)
            when matched then
                update  set monthly_pivot = v_pivot,
                            monthly_s1    = v_s1,
                            monthly_s2    = v_s2,
                            monthly_r1    = v_r1,
                            monthly_r2    = v_r2
            when not matched then
                insert (stock_ticker,monthly_pivot,monthly_s1,monthly_s2,monthly_r1,monthly_r2)
                    values(tab.ticker,v_pivot,v_s1,v_s2,v_r1,v_r2);
            commit;
            -- calculate demark points
            select price_high,price_low,price_open,price_close
            into    v_high,v_low,v_open,v_close
            from stock_price_data
            where stock_ticker =  c_stock_ticker.stock_ticker and business_date =  v_max_date;
            if v_close < v_open then
                v_demark_p := v_high + v_low + v_close + v_low;
                v_demark_h := round((v_demark_p/2) - v_low,3);
                v_demark_l := round((v_demark_p/2) - v_high,3);
            elsif v_close > v_open then
                v_demark_p := v_high + v_low + v_close + v_high;
                v_demark_h := round((v_demark_p/2) - v_low,3);
                v_demark_l := round((v_demark_p/2) - v_high,3);
            else
                v_demark_p := v_high + v_low + v_close + v_close;
                v_demark_h := round((v_demark_p/2) - v_low,3);
                v_demark_l := round((v_demark_p/2) - v_high,3);
            end if;
            merge into stock_pivot_data spd
                using (select c_stock_ticker.stock_ticker as ticker from dual) tab
                on (spd.stock_ticker = tab.ticker)
            when matched then
                update  set demark_high   = v_demark_h,
                            demark_low    = v_demark_l,
                            monthly_s2    = v_s2,
                            monthly_r1    = v_r1,
                            monthly_r2    = v_r2
            when not matched then
                insert (stock_ticker,demark_high,demark_low)
                    values(tab.ticker,v_demark_h,v_demark_l);
            update stock_pivot_data set demark_high = v_demark_h,
                                        demark_low    = v_demark_l
            where stock_ticker = c_stock_ticker.stock_ticker;
            commit;
        end loop;
    end calc_pivot_points;

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
