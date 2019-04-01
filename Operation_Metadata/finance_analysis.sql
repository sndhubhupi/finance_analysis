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
    procedure calc_moving_average_200;
    procedure calc_moving_average_50;
    procedure calc_moving_average_10;
    procedure calc_moving_average_8;    
    procedure update_earliest_latest_dt;

    procedure find_candle_stick_pattern;

    --Bullish Reversal Patterns
    procedure bullish_englufing(  in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure bullish_harami   (  in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure morning_star     (  in_stock_ticker    stock_info_list.stock_ticker%type);

    --Bearish Reversal Patterns
    procedure bearish_englufing(  in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure bearish_harami   (  in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure evening_star     (  in_stock_ticker    stock_info_list.stock_ticker%type);

    --Single-Candle Patterns
	procedure dragonfly_doji	 (in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure gravestone_doji	 (in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure shooting_star    (  in_stock_ticker    stock_info_list.stock_ticker%type);

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


    procedure calc_moving_average_200
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
    end calc_moving_average_200;

    procedure calc_moving_average_50
    as
    begin
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
    end calc_moving_average_50;    

    procedure calc_moving_average_10
    as
    begin
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
    end calc_moving_average_10;   

    procedure calc_moving_average_8
    as
    begin
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

    end calc_moving_average_8;    
    
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
        
            truncate_table('stg_stock_price_data');
            
            -- load only 15 days data for particular stock in stg table
            insert into stg_stock_price_data
                select * from (select * from stock_price_data where stock_ticker = in_stock_ticker order by business_date desc) where rownum < 16;
            
            twizzer_bottom      (stock.stock_ticker);
            twizzer_top         (stock.stock_ticker);

            -- Bullish Reversal Patterns
            bullish_englufing(stock.stock_ticker);
            bullish_harami   (stock.stock_ticker);
            morning_star     (stock.stock_ticker);

            --Bearish Reversal Patterns
            bearish_englufing(stock.stock_ticker);
            bearish_harami   (stock.stock_ticker);
            evening_star     (stock.stock_ticker);

            --Single-Candle Patterns
	        dragonfly_doji	 (stock.stock_ticker);
            gravestone_doji	 (stock.stock_ticker);
            shooting_star    (stock.stock_ticker);

        end loop;
    end find_candle_stick_pattern;

    procedure twizzer_bottom (  in_stock_ticker    stock_info_list.stock_ticker%type)
    as
        v_finding_type      varchar2(50)    := 'TWIZZER_BOTTOM';
        v_finding_counter   number default 0;
        check_equality      boolean;
    begin
          v_full_discription := '';
          select max(business_date) into v_max_date from stg_stock_price_data;
           
          -- check 1 :- last candle must be bullish

          select price_open, price_close into v_price_open, v_price_close
            from stg_stock_price_data where business_date = v_max_date;

          if v_price_close > v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close - v_price_open)/v_price_open)*100,3);
            v_full_discription := v_full_discription || ' $$ 1. Bullish candle formed with percentage ' || v_green_percentage;
          end if;

          -- check 2 :- previous day candle must be bearish

          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close into v_price_open_2, v_price_close_2
            from stg_stock_price_data where business_date = v_yesterday_date;

          if v_price_close_2 < v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open_2 - v_price_close_2)/v_price_open_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 2. Bearish candle formed with percentage ' || v_red_percentage;
          end if;


          -- check 3 :- find day 1 open about equal to day 2 close

         v_smoothing_value := v_price_open * const_smoothing_factor;
         check_equality := v_smoothing_value/2 >= abs(v_price_open - v_price_close_2);
         if check_equality then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 3. TWIZZER BOTTOM FOUND , ' || 'Open Day 1 Price : ' || v_price_open || ' Close Day 2 Price : ' ||  v_price_close_2;
         end if;


          -- check 4 Down trend confirmed :-

          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 < v_price_open then
            --v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open - v_price_close_2)/v_price_close_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 4. Downtrend confirmed with percentage ' || v_red_percentage;
          end if;

          if v_finding_counter = 3 then
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
          v_full_discription := '';
          select max(business_date) into v_max_date from stg_stock_price_data;

          -- check 1 :- last candle must be bearish

          select price_open, price_close into v_price_open, v_price_close
            from stg_stock_price_data where business_date = v_max_date;

          if v_price_close < v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open - v_price_close)/v_price_open)*100,3);
            v_full_discription := v_full_discription || ' $$ 1. Bearish candle formed with percentage ' || v_red_percentage;
          end if;



          -- check 2 :- previous day candle must be bullish

          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close into v_price_open_2, v_price_close_2
            from stg_stock_price_data where business_date = v_yesterday_date;

          if v_price_close_2 > v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close_2 - v_price_open_2)/v_price_open_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 2. Bullish candle formed with percentage ' || v_green_percentage;
          end if;

         -- check 3 :- find day 1 open about equal to day 2 close

         v_smoothing_value := v_price_open * const_smoothing_factor;
         check_equality := v_smoothing_value/2 >= abs(v_price_open - v_price_close_2);
         if check_equality then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 3. TWIZZER TOP FOUND , ' || 'Open Day 1 Price : ' || v_price_open || ' Close Day 2 Price : ' ||  v_price_close_2;
         end if;

          -- check 4 checking for uptrend in twizzer top:-

          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 > v_price_close then
            --v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close_2 - v_price_close)/v_price_close_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 4. Uptrend confirmed with percentage ' || v_green_percentage;
          end if;

          if v_finding_counter = 3 then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
          end if;
    end twizzer_top;


    procedure bullish_englufing    (  in_stock_ticker    stock_info_list.stock_ticker%type)
    as
        v_finding_type      varchar2(50)    := 'BULLISH_ENGULFING';
        v_finding_counter   number default 0;
        check_equality      boolean;
        v_price_high_2      number;
        v_price_low_2       number;
    begin
          v_full_discription := '';
          select max(business_date) into v_max_date from stg_stock_price_data;

          -- check 1 :- last candle must be Bullish

          select price_open, price_close into v_price_open, v_price_close
            from stg_stock_price_data where business_date = v_max_date;

          if v_price_close > v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close - v_price_open)/v_price_open)*100,3);
            v_full_discription := v_full_discription || ' $$ 1. Bullish candle formed with percentage ' || v_green_percentage;
          end if;

          -- check 2 :- previous day candle must be bearish

          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close,price_high,price_low into v_price_open_2, v_price_close_2,v_price_high_2,v_price_low_2
            from stg_stock_price_data where business_date = v_yesterday_date;

          if v_price_close_2 < v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open_2 - v_price_close_2)/v_price_open_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 2. Bearish candle formed with percentage ' || v_red_percentage;
          end if;


         -- check 3 :- open of day 1 must be less than close of day 2 , Gap down
         if v_price_close_2 > v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 3. Gap Down  , ' || 'Open Price : ' || round(v_price_open,3) || ' Previous Day Close Price : ' ||  round(v_price_close_2,3);
         end if;

         -- check 4 :- Gap Down rejected and close above previous day open
         if v_price_close > v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 4. Gap Down  Rejected , ' || 'Close Price : ' || round(v_price_close,3) || ' Previous Day Open Price : ' ||  round(v_price_open_2,3);
         end if;

         -- check 5 :- checking for harami pattern, previous candle must be in latest day body
         if (v_price_close > v_price_high_2)  and (v_price_open < v_price_low_2) then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 5. BULLISH ENGLUFING PATTERN FOUND , ' ;
         end if;


          -- check 6 Down trend confirmed :-

          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 < v_price_open then
            --v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open - v_price_close_2)/v_price_close_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 6. Downtrend confirmed with percentage ' || v_red_percentage;
          end if;

          if v_finding_counter = 5 then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
          end if;
    end bullish_englufing;


    procedure bullish_harami    (  in_stock_ticker    stock_info_list.stock_ticker%type)
    as
        v_finding_type      varchar2(50)    := 'BULLISH_HARAMI';
        v_finding_counter   number default 0;
        check_equality      boolean;
        v_price_high        number;
        v_price_low         number;
    begin
          v_full_discription := '';
          select max(business_date) into v_max_date from stg_stock_price_data;

          -- check 1 :- last candle must be Bullish

          select price_open, price_close,price_high,price_low into v_price_open, v_price_close, v_price_high,v_price_low
            from stg_stock_price_data where business_date = v_max_date;

          if v_price_close > v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close - v_price_open)/v_price_open)*100,3);
            v_full_discription := v_full_discription || ' $$ 1. Bullish candle formed with percentage ' || v_green_percentage;
          end if;

          -- check 2 :- previous day candle must be bearish

          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close into v_price_open_2, v_price_close_2
            from stg_stock_price_data where business_date = v_yesterday_date;

          if v_price_close_2 < v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open_2 - v_price_close_2)/v_price_open_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 2. Bearish candle formed with percentage ' || v_red_percentage;
          end if;


         -- check 3 :- open of day must be greater than close of prevoius , Gap up
         if v_price_close_2 < v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 3. Gap up  , ' || 'Open Price : ' || round(v_price_open,3) || ' Previous Day Close Price : ' ||  round(v_price_close_2,3);
         end if;

         -- check 4 :- checking for harami pattern, new candle must be in previous day body
         if (v_price_open_2 > v_price_high)  and (v_price_close_2 < v_price_low) then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 4. HARAMI PATTERN FOUND , ' || 'Previous Open Price : ' || round(v_price_open_2,3) || ' Day High Price : ' ||  round(v_price_high,3);
            v_full_discription := v_full_discription || ' $$ 4. HARAMI PATTERN FOUND , ' || 'Previous Close Price : ' || round(v_price_close_2,3) || ' Day Low Price : ' ||  round(v_price_low,3);
         end if;


          -- check 5 Down trend confirmed :-

          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 < v_price_open then
            --v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open - v_price_close_2)/v_price_close_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 5. Downtrend confirmed with percentage ' || v_red_percentage;
          end if;

          if v_finding_counter = 4 then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
          end if;
    end bullish_harami;



    procedure bearish_englufing    (  in_stock_ticker    stock_info_list.stock_ticker%type)
    as
        v_finding_type      varchar2(50)    := 'BEARISH_ENGULFING';
        v_finding_counter   number default 0;
        check_equality      boolean;
        v_price_high_2      number;
        v_price_low_2       number;
    begin
          v_full_discription := '';
          select max(business_date) into v_max_date from stg_stock_price_data;

          -- check 1 :- lastest candle must be Bearish

          select price_open, price_close into v_price_open, v_price_close
            from stg_stock_price_data where business_date = v_max_date;

          if v_price_close < v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open - v_price_close)/v_price_open)*100,3);
            v_full_discription := v_full_discription || ' $$ 1. Bearish candle formed with percentage ' || v_red_percentage;
          end if;



          -- check 2 :- previous day candle must be bullish

          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close,price_high,price_low into v_price_open_2, v_price_close_2,v_price_high_2,v_price_low_2
            from stg_stock_price_data where business_date = v_yesterday_date;

          if v_price_close_2 > v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close_2 - v_price_open_2)/v_price_open_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 2. Bullish candle formed with percentage ' || v_green_percentage;
          end if;


         -- check 3 :- open of day 1 must be greater than close of previous day , Gap up
         if v_price_open  > v_price_close_2 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 3. Gap Up  , ' || 'Open Price : ' || round(v_price_open,3) || ' Previous Day Close Price : ' ||  round(v_price_close_2,3);
         end if;

         -- check 4 :- Gap up rejected and close below previous day open
         if v_price_open_2  > v_price_close then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 4. Gap Up  Rejected , ' || 'Close Price : ' || round(v_price_close,3) || ' Previous Day Open Price : ' ||  round(v_price_open_2,3);
         end if;

         -- check 5 :- checking for harami pattern, previous  candle must be in latest day body
         if (v_price_open > v_price_high_2)  and (v_price_close < v_price_low_2) then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 5. BEARISH ENGLUFING PATTERN FOUND , ' ;
         end if;

          -- check 6 Up trend confirmation:-

          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 > v_price_close then
            --v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close_2 - v_price_close)/v_price_close_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 6. Uptrend confirmed with percentage ' || v_green_percentage;
          end if;

          if v_finding_counter = 5 then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
          end if;
    end bearish_englufing;


    procedure bearish_harami    (  in_stock_ticker    stock_info_list.stock_ticker%type)
    as
        v_finding_type      varchar2(50)    := 'BEARISH_HARAMI';
        v_finding_counter   number default 0;
        check_equality      boolean;
        v_price_high        number;
        v_price_low         number;
    begin
          v_full_discription := '';
          select max(business_date) into v_max_date from stg_stock_price_data;

          -- check 1 :- lastest candle must be Bearish

          select price_open, price_close, price_high, price_low into v_price_open, v_price_close, v_price_high, v_price_low
            from stg_stock_price_data where business_date = v_max_date;

          if v_price_close < v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open - v_price_close)/v_price_open)*100,3);
            v_full_discription := v_full_discription || ' $$ 1. Bearish candle formed with percentage ' || v_red_percentage;
          end if;



          -- check 2 :- previous day candle must be bullish

          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close into v_price_open_2, v_price_close_2
            from stg_stock_price_data where business_date = v_yesterday_date;

          if v_price_close_2 > v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close_2 - v_price_open_2)/v_price_open_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 2. Bullish candle formed with percentage ' || v_green_percentage;
          end if;


         -- check 3 :- open of day must be lower than close of previous day , Gap down
         if v_price_open  < v_price_close_2 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 3. Gap down  , ' || 'Open Price : ' || round(v_price_open,3) || ' Previous Day Close Price : ' ||  round(v_price_close_2,3);
         end if;

         -- check 4 :- checking for harami pattern, new candle must be in previous day body
         if (v_price_open_2 < v_price_low)  and (v_price_close_2 > v_price_high) then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 4. HARAMI PATTERN FOUND , ' ;
         end if;


          -- check 5 Up trend confirmation:-

          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 > v_price_close then
            --v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close_2 - v_price_close)/v_price_close_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 5. Uptrend confirmed with percentage ' || v_green_percentage;
          end if;

          if v_finding_counter = 4 then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
          end if;
    end bearish_harami;



	procedure dragonfly_doji	 (in_stock_ticker    stock_info_list.stock_ticker%type)
	as
        v_finding_type      varchar2(50)    := 'DRAGONFLY_DOJI';
        v_finding_counter   number default 0;
        v_price_high		number;
        v_price_low         number;
        check_equality      boolean;
	begin

          v_full_discription := '';
          select max(business_date) into v_max_date from stg_stock_price_data;

          -- load days data

		  select price_open, price_close,price_high,price_low into v_price_open, v_price_close,v_price_high,v_price_low
            from stg_stock_price_data where business_date = v_max_date;

          -- check close approx equal to open

         v_smoothing_value := v_price_open * const_smoothing_factor;
         --check_equality := const_smoothing_factor >= abs(v_price_open - v_price_close);
         if v_smoothing_value >= abs(v_price_open - v_price_close) then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 1. DOJI Found , ' || 'Open Price : ' || round(v_price_open,3) || ' Close Price : ' ||  round(v_price_close,3);
         end if;

         -- check small or no upper shadow

         if v_price_open >= v_price_close then
            if v_smoothing_value*1.5 >= abs(v_price_high - v_price_open) then
                v_finding_counter := v_finding_counter + 1;
                v_full_discription := v_full_discription || ' $$ 2. Small or no upper shadow  , ' || 'Open Price : ' || round(v_price_open,3) || ' High Price : ' ||  round(v_price_high,3);
            end if;
         else
            if v_smoothing_value*1.5 >= abs(v_price_high - v_price_close) then
                v_finding_counter := v_finding_counter + 1;
                v_full_discription := v_full_discription || ' $$ 2. Small or no upper shadow  , ' || 'Close Price : ' || round(v_price_close,3) || ' High Price : ' ||  round(v_price_high,3);
            end if;
         end if;


         -- check long lower shadow

         if v_price_open <= v_price_close then
            if v_smoothing_value*5 <= abs( v_price_open - v_price_low) then
                v_finding_counter := v_finding_counter + 1;
                v_full_discription := v_full_discription || ' $$ 3. Long Lower Shadow  , ' || 'Open Price : ' || round(v_price_open,3) || ' Low Price : ' ||  round(v_price_low,3);
            end if;
         else
            if v_smoothing_value*5 <= abs(v_price_close - v_price_low) then
                v_finding_counter := v_finding_counter + 1;
                v_full_discription := v_full_discription || ' $$ 3. Long Lower Shadow   , ' || 'Close Price : ' || round(v_price_close,3) || ' Low Price : ' ||  round(v_price_low,3);
            end if;
         end if;


          -- check 4 Down trend  :-

          select price_open,price_close into v_price_open_2, v_price_close_2
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_open < v_price_close_2  then
            --v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_close_2  - v_price_open)/v_price_close_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 4. Downtrend confirmed with percentage ' || v_red_percentage;
          end if;

         if v_finding_counter = 3 then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
         end if;
	end dragonfly_doji;


	procedure gravestone_doji	 (in_stock_ticker    stock_info_list.stock_ticker%type)
	as
        v_finding_type      varchar2(50)    := 'GRAVESTONE_DOJI';
        v_finding_counter   number default 0;
        v_price_high		number;
        v_price_low         number;
        check_equality      boolean;
	begin

          v_full_discription := '';
          select max(business_date) into v_max_date from stg_stock_price_data;

          -- load days data

		  select price_open, price_close,price_high,price_low into v_price_open, v_price_close,v_price_high,v_price_low
            from stg_stock_price_data where business_date = v_max_date;

          -- check close approx equal to open

         v_smoothing_value := v_price_open * const_smoothing_factor;
         --check_equality := const_smoothing_factor >= abs(v_price_open - v_price_close);
         if v_smoothing_value >= abs(v_price_open - v_price_close) then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 1. DOJI Found , ' || 'Open Price : ' || round(v_price_open,3) || ' Close Price : ' ||  round(v_price_close,3);
         end if;

         -- check small or no lower shadow

         if v_price_open <= v_price_close then
            if v_smoothing_value*1.5 >= abs(v_price_low - v_price_open) then
                v_finding_counter := v_finding_counter + 1;
                v_full_discription := v_full_discription || ' $$ 2. Small or no lower shadow  , ' || 'Open Price : ' || round(v_price_open,3) || ' low Price : ' ||  round(v_price_low,3);
            end if;
         else
            if v_smoothing_value*1.5 >= abs(v_price_low - v_price_close) then
                v_finding_counter := v_finding_counter + 1;
                v_full_discription := v_full_discription || ' $$ 2. Small or no lower shadow  , ' || 'Close Price : ' || round(v_price_close,3) || ' low Price : ' ||  round(v_price_low,3);
            end if;
         end if;


         -- check long upper shadow

         if v_price_open >= v_price_close then
            if v_smoothing_value*5 <= abs( v_price_open - v_price_high) then
                v_finding_counter := v_finding_counter + 1;
                v_full_discription := v_full_discription || ' $$ 3. Long Upper Shadow  , ' || 'Open Price : ' || round(v_price_open,3) || ' High Price : ' ||  round(v_price_high,3);
            end if;
         else
            if v_smoothing_value*5 <= abs(v_price_close - v_price_high) then
                v_finding_counter := v_finding_counter + 1;
                v_full_discription := v_full_discription || ' $$ 3. Long Upper Shadow   , ' || 'Close Price : ' || round(v_price_close,3) || ' High Price : ' ||  round(v_price_high,3);
            end if;
         end if;


          -- check 4 up trend  :-

          select price_open,price_close into v_price_open_2, v_price_close_2
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_open_2 < v_price_close then
            --v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close - v_price_open_2)/v_price_close)*100,3);
            v_full_discription := v_full_discription || ' $$ 4. Uptrend confirmed with percentage ' || v_green_percentage;
          end if;

         if v_finding_counter = 3 then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
         end if;
	end gravestone_doji;

    procedure evening_star     (  in_stock_ticker    stock_info_list.stock_ticker%type)
    as
        v_finding_type      varchar2(50)    := 'EVENING_STAR';
        v_finding_counter   number default 0;
        v_price_high		number;
        v_price_low         number;
        v_price_open_3      number;
        v_price_close_3       number;
        check_equality      boolean;
        v_day_3_date        date;
    begin
          v_full_discription := '';
          select max(business_date) into v_max_date from stg_stock_price_data;

          -- load lastest day data
		  select price_open, price_close,price_high,price_low into v_price_open, v_price_close,v_price_high,v_price_low
            from stg_stock_price_data where business_date = v_max_date;
          -- Load previous day data
          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close into v_price_open_2, v_price_close_2
            from stg_stock_price_data where business_date = v_yesterday_date;
         -- Load day before previous i.e 3rd candle data
          select max(business_date) into v_day_3_date from stg_stock_price_data
            where business_date != v_yesterday_date and business_date != v_max_date;
          select price_open, price_close into v_price_open_3, v_price_close_3
            from stg_stock_price_data where business_date = v_day_3_date;

         -- check 1 :- lastest candle must be Bearish

          if v_price_close < v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open - v_price_close)/v_price_open)*100,3);
            v_full_discription := v_full_discription || ' $$ 1. Latest Bearish candle formed with percentage ' || v_red_percentage;
          end if;

          -- check 2 : small candle like doji

         v_smoothing_value := v_price_open_2 * const_smoothing_factor;
         --check_equality := const_smoothing_factor >= abs(v_price_open - v_price_close);
         if v_smoothing_value*3 >= abs(v_price_open_2 - v_price_close_2) then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 2. Middle Candle Doji Formation , ' || 'Open Price : ' || round(v_price_open_2,3) || ' Close Price : ' ||  round(v_price_close_2,3);
         end if;

        -- check 3 : last/3rd candle is bullish
          if v_price_close_3 > v_price_open_3 then
            v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close_3 - v_price_open_3)/v_price_open_3)*100,3);
            v_full_discription := v_full_discription || ' $$ 3. Last/3rd Bullish candle formed with percentage ' || v_green_percentage;
          end if;

        -- check 4 : second candle must be gap up, open of middle candle must be greater than close of last/3rd day
         if v_price_open_2  > v_price_close_3 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 4. Second Candle Gap Up  , ' || 'Open Price of middle candle : ' || round(v_price_open_2,3) || ' Previous Day Close Price : ' ||  round(v_price_close_3,3);
         end if;

        --check 5 : Latest candle must be gap down, Open of latest day must be lower than close of middle candle
         if v_price_open  < v_price_close_2 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 5. Latest Candle Gap down  , ' || 'Open Price : ' || round(v_price_open,3) || ' Previous Day Close Price : ' ||  round(v_price_close_2,3);
         end if;

        -- check 6 : Uptrend
          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 > v_price_close then
            --v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close_2 - v_price_close)/v_price_close_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 6. Uptrend confirmed with percentage ' || v_green_percentage;
          end if;

         if v_finding_counter = 5 then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
         end if;

    end evening_star;


    procedure morning_star     (  in_stock_ticker    stock_info_list.stock_ticker%type)
    as
        v_finding_type      varchar2(50)    := 'MORNING_STAR';
        v_finding_counter   number default 0;
        v_price_high		number;
        v_price_low         number;
        v_price_open_3      number;
        v_price_close_3       number;
        check_equality      boolean;
        v_day_3_date        date;
    begin
          v_full_discription := '';
          select max(business_date) into v_max_date from stg_stock_price_data;

          -- load lastest day data
		  select price_open, price_close,price_high,price_low into v_price_open, v_price_close,v_price_high,v_price_low
            from stg_stock_price_data where business_date = v_max_date;
          -- Load previous day data
          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close into v_price_open_2, v_price_close_2
            from stg_stock_price_data where business_date = v_yesterday_date;
         -- Load day before previous i.e 3rd candle data
          select max(business_date) into v_day_3_date from stg_stock_price_data
            where business_date != v_yesterday_date and business_date != v_max_date;
          select price_open, price_close into v_price_open_3, v_price_close_3
            from stg_stock_price_data where business_date = v_day_3_date;

         -- check 1 :- lastest candle must be Bearish

          if v_price_close > v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close - v_price_open)/v_price_open)*100,3);
            v_full_discription := v_full_discription || ' $$ 1. Lastest Bullish candle formed with percentage ' || v_green_percentage;
          end if;

          -- check 2 : small candle like doji

         v_smoothing_value := v_price_open_2 * const_smoothing_factor;
         --check_equality := const_smoothing_factor >= abs(v_price_open - v_price_close);
         if v_smoothing_value*3 >= abs(v_price_open_2 - v_price_close_2) then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 2. Middle Candle Doji Formation , ' || 'Open Price : ' || round(v_price_open_2,3) || ' Close Price : ' ||  round(v_price_close_2,3);
         end if;

        -- check 3 : last/3rd candle is bullish
          if v_price_close_3 < v_price_open_3 then
            v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open_3 - v_price_close_3)/v_price_open_3)*100,3);
            v_full_discription := v_full_discription || ' $$ 3. Last/3rd Bearish candle formed with percentage ' || v_red_percentage;
          end if;

        -- check 4 : Latest candle must be gap up, open of lastest candle must be greater than close of previous day
         if v_price_open  > v_price_close_2 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 4. Latest Candle Gap Up  , ' || 'Open Price of Latest candle : ' || round(v_price_open,3) || ' Previous Day Close Price : ' ||  round(v_price_close_2,3);
         end if;

        --check 5 : Second/Doji candle must be gap down, Open of Second/Doji day must be lower than close of last/3rd candle
         if v_price_open_2  < v_price_close_3 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 5. Second/Doji Candle Gap down  , ' || 'Open Price : ' || round(v_price_open_2,3) || ' Previous Day Close Price : ' ||  round(v_price_close_3,3);
         end if;

        -- check 6 : Downtrend
          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 < v_price_open then
            --v_finding_counter := v_finding_counter + 1;
            v_red_percentage := ROUND(((v_price_open - v_price_close_2)/v_price_close_2)*100,3);
            v_full_discription := v_full_discription || ' $$ 6. Downtrend confirmed with percentage ' || v_red_percentage;
          end if;

         if v_finding_counter = 5  then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
         end if;

    end morning_star;


	procedure shooting_star	 (in_stock_ticker    stock_info_list.stock_ticker%type)
	as
        v_finding_type      varchar2(50)    := 'BEARISH_SHOOTING_STAR';
        v_finding_counter   number default 0;
        v_price_high		number;
        v_price_low         number;
        check_equality      boolean;
	begin

          v_full_discription := '';
          select max(business_date) into v_max_date from stg_stock_price_data;

          -- load days data

		  select price_open, price_close,price_high,price_low into v_price_open, v_price_close,v_price_high,v_price_low
            from stg_stock_price_data where business_date = v_max_date;
          v_smoothing_value := v_price_open * const_smoothing_factor;

         -- check 1 :- lastest candle must be Bearish and small body
         if v_price_close > v_price_open then
            if v_smoothing_value*3 >= abs(v_price_open - v_price_close) then
                v_finding_counter := v_finding_counter + 1;
                v_full_discription := v_full_discription || ' $$ 1. Small Bearish Body Found , ' || 'Open Price : ' || round(v_price_open,3) || ' Close Price : ' ||  round(v_price_close,3);
            end if;
         end if;

         -- Almost no lower shadow


        if v_smoothing_value*.5 >= abs(v_price_low - v_price_close) then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 2. Almost no lower shadow  , ' || 'Close Price : ' || round(v_price_close,3) || ' low Price : ' ||  round(v_price_low,3);
        end if;



         -- Double long upper shadow than body


        if v_smoothing_value*6 <= abs( v_price_open - v_price_high) then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 3. More Than Double Upper Shadow  , ' || 'Open Price : ' || round(v_price_open,3) || ' High Price : ' ||  round(v_price_high,3);
        end if;



          -- check 4 up trend  :-

          select price_open,price_close into v_price_open_2, v_price_close_2
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_open_2 < v_price_close then
            v_finding_counter := v_finding_counter + 1;
            v_green_percentage := ROUND(((v_price_close - v_price_open_2)/v_price_close)*100,3);
            v_full_discription := v_full_discription || ' $$ 4. Uptrend confirmed with percentage ' || v_green_percentage;
          end if;

         if v_finding_counter = 4 then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
         end if;
	end shooting_star;


end finance_analysis;
/
