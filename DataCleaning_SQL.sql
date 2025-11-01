-- Data cleaning in SQL
use world_layoffs;

-- Layoffs dataset :  https://www.kaggle.com/datasets/swaptr/layoffs-2022


select * from layoffs;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary 



-- duplicate remove 

create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert into layoffs_staging
select * from layoffs;
-- lets try to find duplicates on the columns like company, industry, total_laid & date.

with duplicate_cte as 
(
select *,
row_number() over(partition by company, industry,total_laid_off, `date`) as row_num
 from layoffs_staging
 )
 select * from duplicate_cte 
 where row_num > 1;
 
-- lets verify whether the above results are really duplicate, lets verify for 'Oda' & others

select * from layoffs_staging where company ='Oda';
select * from layoffs_staging where company ='Terminus';

-- it seems that they are not really duplicate, we have to find out the duplicates based on the all the columns,
-- lets do it with below CTE
with duplicate_cte as 
(
select *,
row_number() over(partition by company,location, industry,total_laid_off,percentage_laid_off,
 `date`,stage,country,funds_raised_millions) as row_num
 from layoffs_staging
 )
 select * from duplicate_cte 
 where row_num > 1;

-- the above query giving proper duplicates, lets reconfirm with below queries.

select * from layoffs_staging where company ='Hibob';

select * from layoffs_staging where company ='Wildlife Studios';

-- now we can delete these duplicates, we are trying to delete the duplicates with below CTE,
-- but it is not working, as CTE is not updateable.

with duplicate_cte as 
(
select *,
row_number() over(partition by company,location, industry,total_laid_off,percentage_laid_off,
 `date`,stage,country,funds_raised_millions) as row_num
 from layoffs_staging
 )
 delete from duplicate_cte 
 where row_num > 1;
 
 -- for this we will create another table which is copy of existing table layoffs_staging but with additional
 -- row_num column which will be indicator for our duplicate records. lets create it with below query:
 
 CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- notice the extra row_num column added above, the table created will be empty, we need to fill the data in it

select * from layoffs_staging2;

-- add the data in it with below query

 insert into layoffs_staging2
 select *, 
 row_number() over(partition by company,location, industry,total_laid_off,percentage_laid_off,
 `date`,stage,country,funds_raised_millions) as row_num
 from layoffs_staging;

-- now we should be good to delete the records which are duplicates
select * from layoffs_staging2
where row_num > 1;

-- below query will delete duplicate records

delete from layoffs_staging2
where row_num > 1;


select * from layoffs_staging2;


-- 2. Standardize data 
--  lets find null records in industry
select * from layoffs_staging2
where industry is null or industry ='';

-- if you look at Airbnb record we can see that one row in industry is blank. so we can update it considering others
select * from layoffs_staging2
where company ='Airbnb';

-- below query just updates '' records with null so that it is easy to filter the table.
update layoffs_staging2
set industry = null
where industry ='';


select * from layoffs_staging2
where company like 'Bally%';

select * from layoffs_staging2
where company ='Carvana';


-- lets try to select industry with self join table wherever it is null industry
select * from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
where t1.industry is null  
and t2.industry is not null;

-- now this is going to update the empty industry of the company by looking other values of industry. 
-- like for Airbnb, it will update 'Travel' in industry.

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry= t2.industry
where t1.industry is null  
and t2.industry is not null;

-- verify if it is updated or not.
select * from layoffs_staging2
where company ='Airbnb';

-- lets standardize some data 
select distinct industry from layoffs_staging2
order by 1;
-- in industry, there are multiple records with crypto related, 
select * from layoffs_staging2
where industry like 'Crypto%'
order by 1;
-- to standardize, update to all 'Crypto'
update layoffs_staging2
set industry ='Crypto'
where industry like 'Crypto%';

select distinct industry from layoffs_staging2
order by 1;

-- now correct country column
select distinct country from layoffs_staging2
order by 1;

-- this removes trailing . from country name
select distinct country, trim(trailing '.' from country) 
from layoffs_staging2
order by 1;

-- update all countrys removing all trailing .
update layoffs_staging2
set country = trim(trailing '.' from country) ;


select distinct country from layoffs_staging2 order by 1;

-- now change the data type of date column
select `date`, 
str_to_date(`date`,'%m/%d/%Y' )
 from layoffs_staging2;

-- update the values of dates to date type with str_to_date function as below.
update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y' );

-- update the column 'date' to date type 
alter table layoffs_staging2
modify column `date` date ;


-- 3. find null values  and delete the records

select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


delete from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


select * from layoffs_staging2;
-- 4. remove unncessary columns
--  this will remove row_num column which is not needed for us now.


alter table layoffs_staging2
drop column row_num;


select * from layoffs_staging2;