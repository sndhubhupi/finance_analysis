create or replace package finance_analysis
as
    type t_stock_dt_range is record (
        stock_ticker        varchar2(20),
        price_earliest_dt   date,
        price_latest_dt     date);
    type tab_stock_dt_range is table of t_stock_dt_range;
    function out_stock_list_dt_range return tab_stock_dt_range pipelined;

end finance_analysis;
/

create or replace package body finance_analysis
as
    function out_stock_list_dt_range
        return tab_stock_dt_range pipelined
    is
        cursor stock_list is
            select stock_ticker,
                case
                    when nvl(price_earliest_dt,sysdate-731) > (sysdate -729) then trunc(nvl(price_earliest_dt,sysdate-731))
                    when nvl(price_earliest_dt,sysdate-731) < (sysdate -729) then trunc(sysdate - 731)
                else trunc (nvl(price_earliest_dt,sysdate-731))
                end as price_earliest_dt,
                trunc(nvl(price_earliest_dt,sysdate -1)) as price_latest_dt
        from stock_info_list;
    begin
        for rec in stock_list
            loop
                if rec.price_latest_dt = sysdate and rec.price_earliest_dt between sysdate and sysdate - 731
                then
                    null;
                else
                    rec.price_latest_dt := trunc(sysdate);
                    pipe row (rec);
                end if;
            end loop;
            return;
    end out_stock_list_dt_range;


end finance_analysis;
/



