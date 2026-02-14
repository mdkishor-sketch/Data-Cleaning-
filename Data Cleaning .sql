-- Data Cleaning

SELECT * 
FROM layoffs;

-- Creating duplicate tables to avoid errors in raw data
CREATE TABLE layoffs_stagging
LIKE layoffs;

INSERT layoffs_stagging
SELECT *
FROM layoffs;

-- Removing Duplicate
WITH duplicate_cte as(
		select *,
		ROW_NUMBER() OVER(
		PARTITION BY company,location,industry,
                     total_laid_off,percentage_laid_off,`date`,
                     stage,country,funds_raised_millions) as row_num
		FROM layoffs_stagging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

-- Creating another table to delete the duplicate
CREATE TABLE `layoffs_stagging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

Select * 
from layoffs_stagging2
where row_num > 1;

INSERT INTO layoffs_stagging2
select *,
		ROW_NUMBER() OVER(
		PARTITION BY company,location,industry,
                     total_laid_off,percentage_laid_off,`date`,
                     stage,country,funds_raised_millions) as row_num
		FROM layoffs_stagging;
        
DELETE 
FROM layoffs_stagging2
WHERE row_num > 1;

-- Standardizing data
UPDATE layoffs_stagging2
set company = trim(company);

update layoffs_stagging2
set industry = 'Cryto'
where industry like 'Cryto%';

select distinct country, trim(trailing '.' from country)
from layoffs_stagging2 
order by 1;

update layoffs_stagging2
set country = trim(trailing '.' from country)
where country like 'United States%';

UPDATE layoffs_stagging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_stagging2
MODIFY column `date` date;

-- NULL values or blank values
update layoffs_stagging2
set industry = null
where industry = '';

select t1.industry,t2.industry
from layoffs_stagging2 t1
join layoffs_stagging2 t2
    on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

UPDATE layoffs_stagging2 t1
join layoffs_stagging2 t2
   on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

-- Remove any column
ALTER TABLE layoffs_stagging2
DROP column row_num