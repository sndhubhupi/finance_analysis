create or replace package candle_stick_pattern
as
    --Bullish Reversal Patterns
    procedure bullish_englufing(  in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure bullish_harami   (  in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure morning_star     (  in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure bottom_abondoned_baby (in_stock_ticker    stock_info_list.stock_ticker%type);

    --Bearish Reversal Patterns
    procedure bearish_englufing(  in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure bearish_harami   (  in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure evening_star     (  in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure top_abondoned_baby (in_stock_ticker    stock_info_list.stock_ticker%type);

    --Single-Candle Patterns
	procedure dragonfly_doji	 (in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure gravestone_doji	 (in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure shooting_star    (  in_stock_ticker    stock_info_list.stock_ticker%type);

    procedure twizzer_bottom (  in_stock_ticker    stock_info_list.stock_ticker%type);
    procedure twizzer_top    (  in_stock_ticker    stock_info_list.stock_ticker%type);

end candle_stick_pattern;
/

create or replace package body candle_stick_pattern
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

    procedure twizzer_bottom (  in_stock_ticker    stock_info_list.stock_ticker%type)
    as
        v_finding_type      varchar2(50)    := 'TWIZZER_BOTTOM';
        v_finding_counter   number default 0;
        check_equality      boolean;
        check_equality_1    boolean;
        v_price_high        number;
        v_price_low         number;
    begin
          v_full_discription := '';
          select max(business_date) into v_max_date from stg_stock_price_data;

          -- check 1 :- last candle must be bullish

          select price_open, price_close, price_low,price_high into v_price_open, v_price_close,v_price_low,v_price_high
            from stg_stock_price_data where business_date = v_max_date;

          if v_price_close > v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 1. Bullish candle formed';
          end if;

          -- check 2 :- previous day candle must be bearish

          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close into v_price_open_2, v_price_close_2
            from stg_stock_price_data where business_date = v_yesterday_date;

          if v_price_close_2 < v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 2. Bearish candle formed';
          end if;


          -- check 3 :- find day 1 open about equal to day 2 close

         v_smoothing_value := v_price_open * const_smoothing_factor;
         check_equality := v_smoothing_value/3 >= abs(v_price_open - v_price_close_2);
         if check_equality then
            -- check small tail for latest candle
            check_equality_1 := v_smoothing_value >= abs(v_price_open - v_price_low);
            if check_equality_1 then
                v_finding_counter := v_finding_counter + 1;
                v_full_discription := v_full_discription || ' $$ 3. TWIZZER BOTTOM FOUND , ' || 'Open Day 1 Price : ' || round(v_price_open,3) || ' Close Day 2 Price : ' ||  round(v_price_close_2,3);
            end if;
         end if;


          -- check 4 Down trend confirmed :-

          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 < v_price_open then
            --v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 4. Downtrend confirmed';
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
        check_equality_1    boolean;
        v_price_high        number;
        v_price_low         number;
    begin
          v_full_discription := '';
          select max(business_date) into v_max_date from stg_stock_price_data;

          -- check 1 :- last candle must be bearish

          select price_open, price_close, price_low,price_high into v_price_open, v_price_close,v_price_low,v_price_high
            from stg_stock_price_data where business_date = v_max_date;

          if v_price_close < v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 1. Bearish candle formed';
          end if;



          -- check 2 :- previous day candle must be bullish

          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close into v_price_open_2, v_price_close_2
            from stg_stock_price_data where business_date = v_yesterday_date;

          if v_price_close_2 > v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 2. Bullish candle formed';
          end if;

         -- check 3 :- find day 1 open about equal to day 2 close

         v_smoothing_value := v_price_open * const_smoothing_factor;
         check_equality := v_smoothing_value/3 >= abs(v_price_open - v_price_close_2);
         if check_equality then
            -- check small tail for latest candle
            check_equality_1 := v_smoothing_value >= abs(v_price_open - v_price_high);
            if check_equality_1 then
                v_finding_counter := v_finding_counter + 1;
                v_full_discription := v_full_discription || ' $$ 3. TWIZZER TOP FOUND , ' || 'Open Day 1 Price : ' || round(v_price_open,3) || ' Close Day 2 Price : ' ||  round(v_price_close_2,3);
            end if;
         end if;

          -- check 4 checking for uptrend in twizzer top:-

          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 > v_price_close then
            --v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 4. Uptrend confirmed';
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
            v_full_discription := v_full_discription || ' $$ 1. Bullish candle formed';
          end if;

          -- check 2 :- previous day candle must be bearish

          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close,price_high,price_low into v_price_open_2, v_price_close_2,v_price_high_2,v_price_low_2
            from stg_stock_price_data where business_date = v_yesterday_date;

          if v_price_close_2 < v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 2. Bearish candle formed';
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
            --v_finding_counter := v_finding_counter + 1
            v_full_discription := v_full_discription || ' $$ 6. Downtrend confirmed';
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
            v_full_discription := v_full_discription || ' $$ 1. Bullish candle formed';
          end if;

          -- check 2 :- previous day candle must be bearish

          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close into v_price_open_2, v_price_close_2
            from stg_stock_price_data where business_date = v_yesterday_date;

          if v_price_close_2 < v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 2. Bearish candle formed';
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
            v_full_discription := v_full_discription || ' $$ 5. Downtrend confirmed';
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
            v_full_discription := v_full_discription || ' $$ 1. Bearish candle formed';
          end if;



          -- check 2 :- previous day candle must be bullish

          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close,price_high,price_low into v_price_open_2, v_price_close_2,v_price_high_2,v_price_low_2
            from stg_stock_price_data where business_date = v_yesterday_date;

          if v_price_close_2 > v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 2. Bullish candle formed';
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
            v_full_discription := v_full_discription || ' $$ 6. Uptrend confirmed';
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
            v_full_discription := v_full_discription || ' $$ 1. Bearish candle formed';
          end if;



          -- check 2 :- previous day candle must be bullish

          select max(business_date) into v_yesterday_date from stg_stock_price_data
            where business_date != v_max_date;
          select price_open, price_close into v_price_open_2, v_price_close_2
            from stg_stock_price_data where business_date = v_yesterday_date;

          if v_price_close_2 > v_price_open_2 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 2. Bullish candle formed ';
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
            v_full_discription := v_full_discription || ' $$ 5. Uptrend confirmed';
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
            v_full_discription := v_full_discription || ' $$ 4. Downtrend confirmed .';
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
            v_full_discription := v_full_discription || ' $$ 4. Uptrend confirmed .';
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
        v_doji_value        number;
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
            v_full_discription := v_full_discription || ' $$ 1. Latest Bearish candle formed';
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
            v_full_discription := v_full_discription || ' $$ 3. Last/3rd Bullish candle formed ';
          end if;

          if v_price_open_2 > v_price_close_2 then
            v_doji_value := v_price_close_2;
          else
            v_doji_value :=v_price_open_2;
          end if;

        -- check 4 : second candle must be gap up, open of middle candle must be greater than close of last/3rd day
         if v_doji_value  > v_price_close_3 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 4. Second Candle Gap Up  , ' || 'Price of middle candle : ' || round(v_doji_value,3) || ' Previous Day Close Price : ' ||  round(v_price_close_3,3);
         end if;

        --check 5 : Latest candle must be gap down, Open of latest day must be lower than close of middle candle
         if v_price_open  < v_doji_value then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 5. Latest Candle Gap down  , ' || 'Open Price : ' || round(v_price_open,3) || ' Previous Day Price : ' ||  round(v_doji_value,3);
         end if;

        -- check 6 : Uptrend
          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 > v_price_close then
            --v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 6. Uptrend confirmed .';
          end if;

         if v_finding_counter = 5 then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
         end if;

    end evening_star;


    procedure top_abondoned_baby     (  in_stock_ticker    stock_info_list.stock_ticker%type)
    as
        v_finding_type      varchar2(50)    := 'TOP_ABONDONED_BABY';
        v_finding_counter   number default 0;
        v_price_high		number;
        v_price_low         number;
        v_price_high_2		number;
        v_price_low_2       number;
        v_price_high_3		number;
        v_price_low_3       number;
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
          select price_open, price_close,price_high,price_low into v_price_open_2, v_price_close_2 ,v_price_high_2,v_price_low_2
            from stg_stock_price_data where business_date = v_yesterday_date;
         -- Load day before previous i.e 3rd candle data
          select max(business_date) into v_day_3_date from stg_stock_price_data
            where business_date != v_yesterday_date and business_date != v_max_date;
          select price_open, price_close,price_high,price_low into v_price_open_3, v_price_close_3,v_price_high_3,v_price_low_3
            from stg_stock_price_data where business_date = v_day_3_date;
         -- check 1 :- lastest candle must be Bearish

          if v_price_close < v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 1. Latest Bearish candle formed';
          end if;

          -- check 2 : small candle like doji

         v_smoothing_value := v_price_open_2 * const_smoothing_factor;
         --check_equality := const_smoothing_factor >= abs(v_price_open - v_price_close);
         if v_smoothing_value >= abs(v_price_open_2 - v_price_close_2) then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 2. Middle Candle Doji Formation , ' || 'Open Price : ' || round(v_price_open_2,3) || ' Close Price : ' ||  round(v_price_close_2,3);
         end if;

        -- check 3 : last/3rd candle is bullish
          if v_price_close_3 > v_price_open_3 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 3. Last/3rd Bullish candle formed';
          end if;

        -- check 4 : second candle must be gap up, low of middle candle must be greater than high of last/3rd day
         if v_price_low_2  > v_price_high_3 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 4. Second Candle Gap Up  , ' || 'low Price of middle candle : ' || round(v_price_low_2,3) || ' Previous Day high Price : ' ||  round(v_price_high_3,3);
         end if;

        --check 5 : Latest candle must be gap down, high of latest day must be lower than low of middle candle
         if v_price_high  < v_price_low_2 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 5. Latest Candle Gap down  , ' || 'High Price : ' || round(v_price_high,3) || ' Previous Day low Price : ' ||  round(v_price_low_2,3);
         end if;

        -- check 6 : Uptrend
          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 > v_price_close then
            --v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 6. Uptrend confirmed.';
          end if;

         if v_finding_counter = 5 then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription|| ' $$ SELL WITH STOP LOSS :' || round(v_price_high_2,3));
            commit;
         end if;

    end top_abondoned_baby;


    procedure morning_star     (  in_stock_ticker    stock_info_list.stock_ticker%type)
    as
        v_finding_type      varchar2(50)    := 'MORNING_STAR';
        v_finding_counter   number default 0;
        v_price_high		number;
        v_price_low         number;
        v_price_open_3      number;
        v_price_close_3     number;
        v_doji_value        number;
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

         -- check 1 :- lastest candle must be bullish

          if v_price_close > v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 1. Lastest Bullish candle formed';
          end if;

          -- check 2 : small candle like doji

         v_smoothing_value := v_price_open_2 * const_smoothing_factor;
         --check_equality := const_smoothing_factor >= abs(v_price_open - v_price_close);
         if v_smoothing_value*3 >= abs(v_price_open_2 - v_price_close_2) then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 2. Middle Candle Doji Formation , ' || 'Open Price : ' || round(v_price_open_2,3) || ' Close Price : ' ||  round(v_price_close_2,3);
         end if;

        -- check 3 : last/3rd candle is BEARISH
          if v_price_close_3 < v_price_open_3 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 3. Last/3rd Bearish candle formed ';
          end if;

          if v_price_open_2 > v_price_close_2 then
            v_doji_value := v_price_open_2;
          else
            v_doji_value :=v_price_close_2;
          end if;

        -- check 4 : Latest candle must be gap up, open of lastest candle must be greater than close of previous day
         if v_price_open  > v_doji_value then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 4. Latest Candle Gap Up  , ' || 'Open Price of Latest candle : ' || round(v_price_open,3) || ' Previous Day Price : ' ||  round(v_doji_value,3);
         end if;

        --check 5 : Second/Doji candle must be gap down, Open of Second/Doji day must be lower than close of last/3rd candle
         if v_doji_value  < v_price_close_3 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 5. Second/Doji Candle Gap down  , ' || 'Price : ' || round(v_doji_value,3) || ' Previous Day Close Price : ' ||  round(v_price_close_3,3);
         end if;

        -- check 6 : Downtrend
          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 < v_price_open then
            --v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 6. Downtrend confirmed .';
          end if;

         if v_finding_counter = 5  then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
         end if;

    end morning_star;


   procedure bottom_abondoned_baby     (  in_stock_ticker    stock_info_list.stock_ticker%type)
    as
        v_finding_type      varchar2(50)    := 'BOTTOM_ABONDONED_BABY';
        v_finding_counter   number default 0;
        v_price_high		number;
        v_price_low         number;
        v_price_high_2		number;
        v_price_low_2       number;
        v_price_high_3		number;
        v_price_low_3       number;
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
          select price_open, price_close,price_high,price_low into v_price_open_2, v_price_close_2 ,v_price_high_2,v_price_low_2
            from stg_stock_price_data where business_date = v_yesterday_date;
         -- Load day before previous i.e 3rd candle data
          select max(business_date) into v_day_3_date from stg_stock_price_data
            where business_date != v_yesterday_date and business_date != v_max_date;
          select price_open, price_close,price_high,price_low into v_price_open_3, v_price_close_3,v_price_high_3,v_price_low_3
            from stg_stock_price_data where business_date = v_day_3_date;

         -- check 1 :- lastest candle must be bullish

          if v_price_close > v_price_open then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 1. Lastest Bullish candle formed' ;
          end if;

          -- check 2 : small candle like doji

         v_smoothing_value := v_price_open_2 * const_smoothing_factor;
         if v_smoothing_value >= abs(v_price_open_2 - v_price_close_2) then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 2. Middle Candle Doji Formation , ' || 'Open Price : ' || round(v_price_open_2,3) || ' Close Price : ' ||  round(v_price_close_2,3);
         end if;

        -- check 3 : last/3rd candle is bearish
          if v_price_close_3 < v_price_open_3 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 3. Last/3rd Bearish candle formed';
          end if;

        -- check 4 : Latest candle must be gap up, low of lastest candle must be greater than high of previous day
         if v_price_low  > v_price_high_2 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 4. Latest Candle Gap Up  , ' || 'Low Price of Latest candle : ' || round(v_price_low,3) || ' Previous Day high Price : ' ||  round(v_price_high_2,3);
         end if;

        --check 5 : Second/Doji candle must be gap down, Open of Second/Doji day must be lower than close of last/3rd candle
         if v_price_high_2  < v_price_low_3 then
            v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 5. Second/Doji Candle Gap down  , ' || 'High Price : ' || round(v_price_high_2,3) || ' Previous Day low Price : ' ||  round(v_price_low_3,3);
         end if;

        -- check 6 : Downtrend
          select price_open,price_close into v_price_open, v_price_close
                 from stg_stock_price_data where business_date = (select min(business_date) from stg_stock_price_data);
          if v_price_close_2 < v_price_open then
            --v_finding_counter := v_finding_counter + 1;
            v_full_discription := v_full_discription || ' $$ 6. Downtrend confirmed ';
          end if;

         if v_finding_counter = 5  then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription || ' $$ BUY WITH STOPLOSS' || v_price_low_2);
            commit;
         end if;

    end bottom_abondoned_baby;


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
            v_full_discription := v_full_discription || ' $$ 4. Uptrend confirmed ';
          end if;

         if v_finding_counter = 4 then
            insert into findings values (in_stock_ticker,v_max_date,v_finding_type,v_full_discription);
            commit;
         end if;
	end shooting_star;


end candle_stick_pattern;
/
