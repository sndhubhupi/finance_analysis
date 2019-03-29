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
    procedure calc_moving_average;
    procedure update_earliest_latest_dt;
    procedure find_candle_stick_pattern;
    procedure twizzer_bottom (  in_stock_ticker    stock_info_list.stock_ticker%type);

end finance_analysis;
/

create or replace package body finance_analysis
as
   const_smoothing_factor   number := 0.1;
   v_row_count              number;
    
    
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
        for stock in (select distinct stock_ticker from stock_info_list)
        loop    
            null;
        end loop;
    end find_candle_stick_pattern;

    procedure twizzer_bottom (  in_stock_ticker    stock_info_list.stock_ticker%type)
    as
        cursor analysis_data is
            select * from (select * from stock_price_data where stock_ticker = in_stock_ticker order by business_date desc) where rownum < 21;
        type t_stock_price_data is table of stock_price_data%rowtype index by binary_integer;
        tab_stock_price_data t_stock_price_data;
        v_max_date date;
    begin
        open analysis_data;
        fetch analysis_data bulk collect into tab_stock_price_data;
        close analysis_data;
        -- check valid 20 days data is there to do calculation 
        if tab_stock_price_data.count < 20
            then 
                return;
        end if;
        -- check condition 4 bullish candle
          select max(business_date) into v_max_date from table(tab_stock_price_data);
    
    end twizzer_bottom;
end finance_analysis;
/
