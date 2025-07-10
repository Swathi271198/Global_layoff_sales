select * from layoffs;

create table layoffs_working
like layoffs;

insert into layoffs_working
select *
from layoffs;

select * from layoffs_working;

-- 1.Removing duplicates

select * ,
row_number() over (partition by 
company, location, total_laid_off, `date`, percentage_laid_off,
industry, stage, funds_raised, country) AS row_num
from layoffs_working;

select * 
from layoffs_working
where company = 'Casper';

with duplicate_cte as 
(select * ,
row_number() over (partition by 
company, location, total_laid_off, `date`, percentage_laid_off,
industry, stage, funds_raised, country) AS row_num
from layoffs_working)
select *
from duplicate_cte
where row_num > 1;

CREATE TABLE `layoffs_new` (
  `company` text,
  `location` text,
  `total_laid_off` text,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `stage` text,
  `funds_raised` text,
  `country` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_new;

insert into layoffs_new
select * ,
row_number() over (partition by 
company, location, total_laid_off, `date`, percentage_laid_off,
industry, stage, funds_raised, country) AS row_num
from layoffs_working;

delete from layoffs_new
where row_num >1;

-- 2.Standarization

select *
from layoffs_new;

select company, trim(company)
from layoffs_new;

select distinct industry
from layoffs_new
order by 1;

select distinct country
from layoffs_new
order by 1;

update layoffs_new
set country = 'UAE'
where country = 'United Arab Emirates'; 

select `date` from layoffs_new;

select `date`,
str_to_date (`date`, '%m/%d/%Y') as Date_format
from layoffs_new;

update layoffs_new
set `date` = str_to_date (`date`, '%m/%d/%Y');

alter table layoffs_new
modify column `date` date;

select *
from layoffs_new
where total_laid_off = ''
and percentage_laid_off = '';

delete from layoffs_new
where total_laid_off = ''
and percentage_laid_off = '';

alter table layoffs_new
drop column row_num;

-- EDA

select * 
from layoffs_new;

select max(total_laid_off),max(percentage_laid_off)
from layoffs_new;

select *
from layoffs_new
where percentage_laid_off = 1
order by funds_raised desc ;

select company, sum(total_laid_off)
from layoffs_new
group by company
order by 2 desc;

select min(`date`), max(`date`)
from layoffs_new;

select industry, sum(total_laid_off)
from layoffs_new
group by industry
order by 2 desc;

select country, sum(total_laid_off)
from layoffs_new
group by country
order by 2 desc;

select Year(`date`), sum(total_laid_off)
from layoffs_new
group by Year(`date`)
order by 2 desc;

select stage, sum(total_laid_off)
from layoffs_new
group by stage
order by 2 desc;


with rolling_total as
(
select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_new
where substring(`date`,1,7) is not null
group by `month`
order by 1 ASC
)
select `month`,total_off, sum(total_off) over (order by `month`) as rolling_total
from rolling_total;

select company,year(`date`), sum(total_laid_off)
from layoffs_new
group by company,year(`date`)
order by 3 desc;

with company_year (company,years,total_laid_off) as 
(
select company,year(`date`), sum(total_laid_off)
from layoffs_new
group by company,year(`date`)
), company_year_rank as
(
select *, dense_rank() over (partition by years order by total_laid_off DESC) AS Ranking
from company_year
)
select *
from company_year_rank
where Ranking <= 5
;







 