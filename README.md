 Create table category(
id int null,
category_name varchar(250) null,
parent_id int null,
position int null);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/crowdfunding_Category_Csv.csv'
INTO TABLE category
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@id, @category_name, @parent_id, @position)
SET
id = NULLIF(@id,''),
category_name = NULLIF(@category_name,''),
parent_id = NULLIF(@parent_id,''),
position = NULLIF(@position,'');

create table creator(
id int null,
creator_name varchar(250) null,
chosen_currency varchar(200) null
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/crowdfunding_creator_csv.csv'
into table creator
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows
(@id, @creator_name, @chosen_currency)
set
id = nullif(@id,''),
creator_name = nullif(@creator_name,''),
chosen_currency = nullif(@chosen_currency,'');

create table location(
id int NULL, displayable_name varchar(30) NULL, type varchar(10) NULL, city varchar(30) NULL, state varchar(5) NULL,
short_name varchar(35) NULL, is_root varchar(10) NULL, country varchar(5) NULL, localized_name int NULL);

alter table location modify localized_name varchar(255) null;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/crowdfunding_Location_Csv.csv'
INTO TABLE location
FIELDS terminated by ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
ignore 1 rows 
(@id, @displayable_name, @type, @city, @state, @short_name,
@is_root, @country, @localized_name)
set
id = nullif(@id,''),
displayable_name = nullif(@displayable_name,''),
type = nullif(@type,''),
city = nullif(@city,''),
state = nullif(@state,''),
short_name = nullif(@short_name,''),
is_root = nullif(@is_root,''),
country = nullif(@country,''),
localized_name = nullif(@localized_name,'');

create table main(
id int, state varchar(255), name varchar(500), country varchar(10), creator_id int, location_id int, category_id int,
created_at int, deadline int, updated_at int, state_changed_at int, successful_at int, launched_at int, goal int, pledged int, currency varchar(20),
currency_symbol varchar(10), usd_pledged int, static_usd_rate int, backers_count int, spotlight varchar(20), staff_pick varchar(20), blurb varchar(500),
currency_trailing_code varchar(20),disable_communcation varchar(20));

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/crowdfunding_projects_csv.csv'
INTO TABLE main
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@id, @state, @name, @country, @creator_id, @location_id, @category_id,
 @created_at, @deadline, @updated_at, @state_changed_at, @successful_at,
 @launched_at, @goal, @pledged, @currency, @currency_symbol,
 @usd_pledged, @static_usd_rate, @backers_count, @spotlight,
 @staff_pick, @blurb, @currency_trailing_code, @disable_communcation)
SET
id = nullif(@id,''), state = nullif(@state,''), name = nullif(@name,''), country = nullif(@country,''), creator_id = nullif(@creator_id,''), 
location_id = nullif(@location_id,''), category_id = nullif(@category_id,''), created_at = nullif(@created_at,''), deadline = nullif(@deadline,''),
updated_at = nullif(@updated_at,''), state_changed_at = nullif(@state_changed_at,''), successful_at = nullif(@successful_at,''),
launched_at = nullif(@launched_at,''), goal = nullif(@goal,''), pledged = nullif(@pledged,''), currency = nullif(@currency,''), 
currency_symbol = nullif(@currency_symbol,''), usd_pledged = nullif(@usd_pledged,''), static_usd_rate = nullif(@static_usd_rate,''), 
backers_count = nullif(@backers_count,''), spotlight = nullif(@spotlight,''), staff_pick = nullif(@staff_pick,''), blurb = nullif(@blurb,''),
currency_trailing_code = nullif(@currency_trailing_code,''), disable_communcation = nullif(@disable_communcation,'');

alter table main 
add created_at_date date after created_at;

update main set 
created_at_date = date(from_unixtime(created_at));

alter table main 
add deadline_date date after deadline;

update main set 
deadline_date = date(from_unixtime(created_at));

ALTER TABLE main
DROP COLUMN created_at_time,
DROP COLUMN deadline;

alter table main 
add updated_date date after updated_at;

update main set
updated_date = date(from_unixtime(updated_at));

alter table main 
drop column updated_at;

alter table main
add state_changed_date date after state_changed_at;

update main set
state_changed_date = date(from_unixtime(state_changed_at));

alter table main
drop column state_changed_at;

alter table main
add successful_date date after successful_at;

update main set
successful_date = date(from_unixtime(successful_at));

alter table main
drop column successful_at;

alter table main
add launched_date date after launched_at;

update main set
launched_date = date(from_unixtime(launched_at));

alter table main
drop column launched_at;

select min(created_at_date) as min, max(created_at_date) as max from main;

# Create Date Table 

SET SESSION cte_max_recursion_depth = 5000;

CREATE TABLE date_table AS
WITH RECURSIVE date_series AS (
	SELECT MIN(created_at_date) AS dt
    FROM main
    
    UNION ALL
    
    SELECT DATE_ADD(dt, INTERVAL 1 DAY)
    FROM date_series
    WHERE dt < (SELECT MAX(created_at_date) FROM main) 
)
SELECT 
    dt AS full_date,
    YEAR(dt) AS year,
    MONTH(dt) AS month_number,
    MONTHNAME(dt) AS month_name,
    QUARTER(dt) AS quarter,
    WEEK(dt) AS week_number,
    DAY(dt) AS day,
    DAYNAME(dt) AS day_name
FROM date_series;

# Total Successful Projects

select concat(round(count(distinct(id))/1000,2),"k") as Total_project_by_outcome from main
where state = "successful";

# Total Projects by City

select l.city,round(count(distinct(c.id)),2) as Total_project_by_city from main c
join location l on c.location_id = l.id group by city
order by Total_project_by_city desc limit 10;

# Total Projects by Category

select c.category_name, round(count(distinct(m.id)),2) as Total_Projects_by_Categories 
from main m join category c on m.category_id = c.id
group by  category_name order by Total_Projects_by_Categories desc limit 10;

# Total Projects by Year/Quarter/Month

select d.year, d.quarter, d.month_number,d.month_name,  round(count(distinct(m.id)),2) as Project_trend_oveer_time
from main m join date_table d on m.created_at_date = d.full_date group by d.year,d.quarter,d.month_number,d.month_name;

# Amount Raised

select concat(round(sum(usd_pledged)/1000000000,2),"B") as Amount_Raised from main 
where state = "Successful";

# Total Backers for Successful Projects

select concat(round(sum(backers_count)/1000000,2),"M") as  Total_Backers_for_successful_Projects
from main where state = "Successful";
 
 # Average Days for Successful Projects
 
select round(avg(datediff(successful_date,launched_date)),2) as Average_Campign_Duration from main;

# Percentage of Successful Projects 

# 1. Overall Percentage 

select concat(round((Total_Successful_Project/Total_Project)*100,2),"%") as Overall_Percentage from 
(select count(id) as Total_Successful_Project from main where state = "Successful")t,
(select  count(id) as Total_Project from main )t2; 

#2. Percentage By Category 

select category_name,
concat(Category_Wise_Percentage,"%") as Category_Wise
from (select c.category_name,round((sum(case when m.state = "Successful" then 1 else 0 end)/count(m.id))*100,2)
as category_wise_percentage from
main m join category c on m.category_id = c.id group by category_name)t order by Category_Wise_percentage
desc limit 10;

#3. Percentage By Year/Month 

select Year,month_name,concat(Year_Month_Wise,"%") as Year_Month_Wise_percentage
from (select d.year, d.month_number, d.Month_name, round((sum( case
when m.state = "Successful" then 1 else 0 end)/ count(m.id))*100,2) as Year_Month_Wise 
from main m join date_table d on m.created_at_date = d.full_date group by d.year, d.month_number,d.month_name order by month_number)t order by Year; 

#4. Percentage By Goal Range

alter table main 
add goal_range varchar(10) after goal;

update  main
set goal_range =  case when goal <= 10000 then "0-10K"
when goal <= 50000 then "10k-50k"
when goal <= 100000 then "50K-100K"
when goal <= 500000 then "100k-500K"
when goal <= 1000000 then "500k-1M"
when goal <= 5000000 then "1M-5M"
when goal <= 10000000 then "5M-10M"
else "10M+"
end; 

alter table main
drop column goal_range;

select goal_range,concat(goal_range_wise,"%") as goal_range_wise_success_rate from (select goal_range,
round((sum(case when state = "successful" then 1 else  0 end)/count(id))*100,2) as goal_range_wise from main group  by goal_range)t;
