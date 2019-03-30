create or replace package finance_analysis 
as
    type t_stock_dt_range is record ( 
        stock_ticker        varchar2(20),
        price_earliest_dt   date,
        price_latest_dt     date);
    type t_findings is record ( 
        stock_ticker        varchar2(20),
        business_date       date,
        finding_type        varchar2(50),
        full_discription    varchar2(1000));        
    type tab_stock_dt_range is table of t_stock_dt_range;
    type tab_findings is table of t_findings;
    
    function out_stock_list_dt_range return tab_stock_dt_range pipelined;
    function out_candle_stick_pattern return  tab_findings pipelined;
    
    procedure truncate_table (in_table_name     varchar2);
    procedure load_price_data_from_stg;
    procedure load_stock_list_from_stg;
    procedure calc_moving_average;
    procedure update_earliest_latest_dt;
    procedure find_candle_stick_pattern;
    procedure twizzer_bottom (  in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure twizzer_top    (  in_stock_ticker    stock_info_list.stock_ticker%type);

end finance_analysis;
/

create or replace package body finance_analysis
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


    function out_stock_list_dt_range
        return tab_stock_dt_range pipelined
    is
        cursor stock_list is
            select stock_ticker,
                   trunc(nvl(price_latest_dt,sysdate-731)) as price_earliest_dt,
                   trunc(nvl(price_latest_dt,sysdate -1)) as price_latest_dt
        from stock_info_list;
    begin
        for rec in stock_list
            loop
                if trunc(rec.price_latest_dt) = trunc(sysdate) --and rec.price_earliest_dt between sysdate and sysdate - 731
                then
                    null;
                else
                    rec.price_earliest_dt := trunc(rec.price_earliest_dt) +1;
                    rec.price_latest_dt := trunc(sysdate);
                    pipe row (rec);
                end if;
            end loop;
            return;
    end out_stock_list_dt_range;

    function out_candle_stick_pattern
        return tab_findings pipelined
    is
        cursor finding_list is
            select stock_ticker,
                   business_date,
                   finding_type,
                   full_discription
        from findings;
    begin
        for rec in finding_list
            loop
                    pipe row (rec);
            end loop;
            return;
    end out_candle_stick_pattern;


    procedure truncate_table(in_table_name     varchar2)
    as
        v_sql   varchar2(100) := 'truncate table ';
    begin
        v_sql := v_sql || in_table_name;
        execute immediate v_sql;
    end truncate_table;

    procedure load_price_data_from_stg
    as
    begin
        insert into stock_price_data
            select * from stg_stock_price_data stg
                where not exists (select 1 from stock_price_data spd where spd.stock_ticker = stg.stock_ticker
                                                                      and spd.business_date = stg.business_date);
        commit;
    end load_price_data_from_stg;

    procedure load_stock_list_from_stg
    as
    begin
        insert into stock_info_list
            select * from stg_stock_info_list stg
                where not exists (select 1 from stock_info_list sil where sil.stock_ticker = stg.stock_ticker);
        commit;
    end load_stock_list_from_stg;


    procedure calc_moving_average
    as
    begin
        update stock_price_data set dma_200 =
            (select dma_200 from
                (select stock_ticker,business_date,
                    avg(price_close) over (partition by stock_ticker order by business_date rows 199 preceding ) as dma_200,
                    count(price_close) over (partition by stock_ticker order by business_date rows 199 preceding ) as cnt
                  from stock_price_data ) tab
        where tab.cnt > 199
        and tab.stock_ticker = stock_price_data.stock_ticker
        and trunc(tab.business_date) = trunc(stock_price_data.business_date));
        commit;

        update stock_price_data set dma_50 =
            (select dma_50 from
                (select stock_ticker,business_date,
                    avg(price_close) over (partition by stock_ticker order by business_date rows 49 preceding ) as dma_50,
                    count(price_close) over (partition by stock_ticker order by business_date rows 49 preceding ) as cnt
                  from stock_price_data ) tab
        where tab.cnt > 49
        and tab.stock_ticker = stock_price_data.stock_ticker
        and trunc(tab.business_date) = trunc(stock_price_data.business_date));
        commit;

        update stock_price_data set dma_10 =
            (select dma_10 from
                (select stock_ticker,business_date,
                    avg(price_close) over (partition by stock_ticker order by business_date rows 9 preceding ) as dma_10,
                    count(price_close) over (partition by stock_ticker order by business_date rows 9 preceding ) as cnt
                  from stock_price_data ) tab
        where tab.cnt > 9
        and tab.stock_ticker = stock_price_data.stock_ticker
        and trunc(tab.business_date) = trunc(stock_price_data.business_date));
        commit;

        update stock_price_data set dma_8 =
            (select dma_8 from
                (select stock_ticker,business_date,
                    avg(price_close) over (partition by stock_ticker order by business_date rows 7 preceding ) as dma_8,
                    count(price_close) over (partition by stock_ticker order by business_date rows 7 preceding ) as cnt
                  from stock_price_data ) tab
        where tab.cnt > 7
        and tab.stock_ticker = stock_price_data.stock_ticker
        and trunc(tab.business_date) = trunc(stock_price_data.business_date));
        commit;

    end calc_moving_average;

    procedure update_earliest_latest_dt
    as
    begin
        update stock_info_list set PRICE_EARLIEST_DT = (select min(business_date) from stock_price_data
                                                            where stock_price_data.stock_ticker = stock_info_list.stock_ticker),
                                   price_latest_dt = (select max(business_date) from stock_price_data
                                                            where stock_price_data.stock_ticker = stock_info_list.stock_ticker);


    end update_earliest_latest_dt;

    procedure find_candle_stick_pattern
    as
    begin
        truncate_table('findings');
        for stock in (select distinct stock_ticker from stock_info_list)
        loop
            twizzer_bottom  (stock.stock_ticker);
            twizzer_top     (stock.stock_ticker);
        end loop;
    end find_candle_stick_pattern;

    procedure twizzer_bottom (  in_stock_ticker    stock_info_list.stock_ticker%type)
    as
        v_finding_type      varchar2(50)    := 'TWIZZER_BOTTOM';
        v_finding_counter   number default 0;
        check_equality      boolean;
    begin
          truncate_table('stg_stock_price_data');
          v_full_discription := '';

          -- load only 15 days data for particular stock in stg table
          insert into stg_stock_price_data
            select * from (select * from stock_price_data where stock_ticker = in_stock_ticker order by business_date desc) where rownum < 16;
          select max(business_date) into v_max_date from stg_stock_price_data;

          -- check 1 :- last candle must be bullish

          select price_open, price_close into v_price_open, v_price_close
            from stg_stock_price_data where business_date = v_max_date;

          if v_price_close > v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close - v_price_open)/v_price_open)*100,3);
            v_full_discription := v_full_discription || ' 1. Bullish candle formed with percentage ' || v_green_percentage;
          end if;

          -- check 2 :- previous day candle must be bearish

          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close into v_price_open_2, v_price_close_2
            from stg_stock_price_data where business_date = v_yesterday_date;

          if v_price_close_2 < v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open_2 - v_price_close_2)/v_price_open_2)*100,3);
            v_full_discription := v_full_discription || ' 2. Bearish candle formed with percentage ' || v_red_percentage;
          end if;


          -- check 3 :- find day 1 open about equal to day 2 close

         v_smoothing_value := v_price_open * const_smoothing_factor;
         check_equality := const_smoothing_factor >= abs(v_price_open - v_price_close_2);
         if check_equality then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' 3. TWIZZER BOTTOM FOUND , ' || 'Open Day 1 Price : ' || v_price_open || ' Close Day 2 Price : ' ||  v_price_close_2;
         end if;


          -- check 4 :-

          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 < v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open - v_price_close_2)/v_price_close_2)*100,3);
            v_full_discription := v_full_discription || ' 4. Downtrend confirmed with percentage ' || v_red_percentage;
          end if;

          if v_finding_counter = 4 then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
          end if;
    end twizzer_bottom;



    procedure twizzer_top    (  in_stock_ticker    stock_info_list.stock_ticker%type)
    as
        v_finding_type      varchar2(50)    := 'TWIZZER_TOP';
        v_finding_counter   number default 0;
        check_equality      boolean;
    begin
          truncate_table('stg_stock_price_data');
          v_full_discription := '';

          -- load only 15 days data for particular stock in stg table
          insert into stg_stock_price_data
            select * from (select * from stock_price_data where stock_ticker = in_stock_ticker order by business_date desc) where rownum < 16;
          select max(business_date) into v_max_date from stg_stock_price_data;

          -- check 1 :- last candle must be bearish

          select price_open, price_close into v_price_open, v_price_close
            from stg_stock_price_data where business_date = v_max_date;

          if v_price_close < v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open - v_price_close)/v_price_open)*100,3);
            v_full_discription := v_full_discription || ' 1. Bearish candle formed with percentage ' || v_red_percentage;
          end if;



          -- check 2 :- previous day candle must be bearish

          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close into v_price_open_2, v_price_close_2
            from stg_stock_price_data where business_date = v_yesterday_date;

          if v_price_close_2 > v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close_2 - v_price_open_2)/v_price_open_2)*100,3);
            v_full_discription := v_full_discription || ' 2. Bullish candle formed with percentage ' || v_green_percentage;
          end if;

         -- check 3 :- find day 1 open about equal to day 2 close

         v_smoothing_value := v_price_open * const_smoothing_factor;
         check_equality := const_smoothing_factor >= abs(v_price_open - v_price_close_2);
         if check_equality then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' 3. TWIZZER TOP FOUND , ' || 'Open Day 1 Price : ' || v_price_open || ' Close Day 2 Price : ' ||  v_price_close_2;
         end if;

          -- check 4 checking for uptrend in twizzer top:-

          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 > v_price_close then
            v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close_2 - v_price_close)/v_price_close_2)*100,3);
            v_full_discription := v_full_discription || ' 4. Uptrend confirmed with percentage ' || v_green_percentage;
          end if;

          if v_finding_counter = 4 then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
          end if;
    end twizzer_top;


end finance_analysis;
/
