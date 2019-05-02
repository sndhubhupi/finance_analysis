create or replace package finance_analysis 
as
    type t_stock_list is record ( 
        stock_ticker        varchar2(20));
    type t_findings is record ( 
        stock_ticker        varchar2(20),
        business_date       date,
        finding_type        varchar2(50),
        full_discription    varchar2(1000));        
    type tab_stock_list is table of t_stock_list;
    type tab_findings is table of t_findings;
    
    function out_stock_list return tab_stock_list pipelined;
    function out_candle_stick_pattern return  tab_findings pipelined;
    
    procedure truncate_table (in_table_name     varchar2);
    procedure load_price_data_from_stg;
    procedure load_stock_list_from_stg;

    procedure find_candle_stick_pattern(in_date     varchar2);

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


    function out_stock_list
        return tab_stock_list pipelined
    is
        cursor stock_list is
            select stock_ticker
                from stock_info_list where trunc(price_latest_dt) <>  trunc(sysdate) or price_latest_dt is null;
    begin
        for rec in stock_list
            loop
                    pipe row (rec);

            end loop;
            return;
    end out_stock_list;

    function out_candle_stick_pattern
        return tab_findings pipelined
    is
        cursor finding_list is select * from findings order by finding_type;
           /* select * from (
                select distinct
                    t.stock_ticker, t.business_date,t.finding_type,
                    trim(regexp_substr(t.full_discription, '[^$$]+', 1, levels.column_value))  as Full_Disc
                from
                    findings t,
                    table(cast(multiset(select level from dual connect by  level <= length (regexp_replace(t.full_discription, '[^$$]+'))  + 1) as sys.OdciNumberList)) levels
                order by stock_ticker)
            where full_disc is not null;*/
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
        delete from stock_price_data spd
         where exists (select 1 from stg_stock_price_data stg where spd.stock_ticker = stg.stock_ticker
                                                                      and spd.business_date = stg.business_date);
        insert into stock_price_data
            select * from stg_stock_price_data;
        commit;
        -- delete invalid data from main table
        delete from stock_price_data where price_high = 0;
        commit;
        delete from stock_price_data where volume = 0 and stock_ticker not in ('^NSEI','^NSEBANK');
        commit;
        delete from stock_price_data where price_high = 0;
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

    procedure find_candle_stick_pattern(in_date     varchar2)
    as
        v_in_date   date;
    begin
        truncate_table('findings');

        for stock in (select distinct stock_ticker from stock_info_list)
        loop

            truncate_table('stg_stock_price_data');

            -- load only 15 days data for particular stock in stg table
            if in_date is not null
            then
                v_in_date := to_date(in_date,'DD-MM-YYYY');
                insert into stg_stock_price_data
                    select * from (select * from stock_price_data where stock_ticker = stock.stock_ticker
                                                                   and business_date <= v_in_date
                        order by business_date desc) where rownum < 16;
            else
                insert into stg_stock_price_data
                    select * from (select * from stock_price_data where stock_ticker = stock.stock_ticker order by business_date desc) where rownum < 16;
            end if;

            candle_stick_pattern.twizzer_bottom      (stock.stock_ticker);
            candle_stick_pattern.twizzer_top         (stock.stock_ticker);
            candle_stick_pattern.in_out_in           (stock.stock_ticker);

            -- Bullish Reversal Patterns
            candle_stick_pattern.bullish_englufing(stock.stock_ticker);
            candle_stick_pattern.bullish_harami   (stock.stock_ticker);
            candle_stick_pattern.morning_star     (stock.stock_ticker);
            candle_stick_pattern.bottom_abondoned_baby(stock.stock_ticker);

            --Bearish Reversal Patterns
            candle_stick_pattern.bearish_englufing(stock.stock_ticker);
            candle_stick_pattern.bearish_harami   (stock.stock_ticker);
            candle_stick_pattern.evening_star     (stock.stock_ticker);
            candle_stick_pattern.top_abondoned_baby(stock.stock_ticker);

            --Single-Candle Patterns
	        candle_stick_pattern.dragonfly_doji	 (stock.stock_ticker);
            candle_stick_pattern.gravestone_doji	 (stock.stock_ticker);
            candle_stick_pattern.shooting_star    (stock.stock_ticker);

            -- four candlestick pattern
            candle_stick_pattern.bearish_three_line_strike (stock.stock_ticker);
            candle_stick_pattern.bullish_three_line_strike (stock.stock_ticker);
        end loop;
    end find_candle_stick_pattern;

end finance_analysis;
/
