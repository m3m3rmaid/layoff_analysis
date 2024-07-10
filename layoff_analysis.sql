select * from world_layoff.layoffs;

-- Prepare dataset
-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null Values or blank values
-- 4. Remove any columns

USE world_layoff;
CREATE TABLE layoffs_staging 
LIKE world_layoff.layoffs;

SELECT * FROM world_layoff.layoffs_staging;


INSERT world_layoff.layoffs_staging
SELECT *
FROM world_layoff.layoffs;



WITH duplicate_cte AS
(
SELECT *, 
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country,
funds_raised_millions) AS 	row_num
FROM world_layoff.layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM world_layoff.layoffs_staging2
WHERE row_num >1;

INSERT INTO world_layoff.layoffs_staging2
SELECT *, 
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country,
funds_raised_millions) AS 	row_num
FROM world_layoff.layoffs_staging;

SET SQL_SAFE_UPDATES = 0;
DELETE FROM world_layoff.layoffs_staging2
WHERE row_num >1 ;

SELECT * FROM world_layoff.layoffs_staging2;


-- Standardization

SELECT company,  TRIM(company)
FROM world_layoff.layoffs_staging2;

UPDATE world_layoff.layoffs_staging2
SET company = TRIM(company);


SELECT *
FROM world_layoff.layoffs_staging2
where industry LIKE "Crypto%";

UPDATE world_layoff.layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM world_layoff.layoffs_staging2
ORDER BY 1;

UPDATE world_layoff.layoffs_staging2
SET country = "United States"
WHERE country LIKE "United States%";

UPDATE world_layoff.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE "United States%";

SELECT *
FROM world_layoff.layoffs_staging2;

SELECT `date`,
STR_TO_DATE (`date`, '%m/%d/%y')
FROM world_layoff.layoffs_staging2;

UPDATE world_layoff.layoffs_staging2
SET `date` = STR_TO_DATE (`date`, '%m/%d/%Y');

ALTER TABLE world_layoff.layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM world_layoff.layoffs_staging2
WHERE total_laid_off is NULL
AND percentage_laid_off is NULL;

UPDATE world_layoff.layoffs_staging2
SET industry = NULL
WHERE industry = '';


SELECT *
FROM world_layoff.layoffs_staging2
WHERE industry is NULL
OR industry = "";

SELECT *
FROM world_layoff.layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM world_layoff.layoffs_staging2 t1
JOIN world_layoff.layoffs_staging2 t2
	ON t1.company= t2.company
    AND t1.location = t2.location
WHERE (t1.industry is null or t1.industry = '')
AND t2.industry is not null;

UPDATE world_layoff.layoffs_staging2 t1
JOIN world_layoff.layoffs_staging2 t2
	ON t1.company= t2.company
SET t1.industry = t2.industry
WHERE t1.industry is null
AND t2.industry is not null;


SELECT *
FROM world_layoff.layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;


SELECT COUNT(*) AS total
FROM world_layoff.layoffs_staging2;

DELETE
FROM world_layoff.layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM world_layoff.layoffs_staging2;

ALTER TABLE world_layoff.layoffs_staging2
DROP COLUMN row_num;

SELECT MIN(`date`), MAX(`date`)
FROM world_layoff.layoffs_staging2;

SELECT company, sum(total_laid_off)
FROM world_layoff.layoffs_staging2
GROUP BY company
ORDER by 2 DESC;

SELECT industry, sum(total_laid_off)
FROM world_layoff.layoffs_staging2
GROUP BY industry
ORDER by 2 DESC;


SELECT country, sum(total_laid_off)
FROM world_layoff.layoffs_staging2
GROUP BY country
ORDER by 2 DESC;

SELECT YEAR(`date`), sum(total_laid_off)
FROM world_layoff.layoffs_staging2
GROUP BY YEAR(`date`)
ORDER by 1 DESC;


SELECT stage, sum(total_laid_off)
FROM world_layoff.layoffs_staging2
GROUP BY stage
ORDER by 2 DESC;

SELECT substring(`date`,1,7) AS `MONTH`,sum(total_laid_off)
FROM world_layoff.layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `MONTH` 
ORDER BY 1 ASC
;


-- How do layoffs trend over time? Are there any particular months or periods with higher layoff rates?
WITH Rolling_Total AS
(
SELECT substring(`date`,1,7) AS `MONTH`,sum(total_laid_off) AS total_off
FROM world_layoff.layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `MONTH` 
ORDER BY 1 ASC
)
SELECT `MONTH` , total_off, SUM(total_off) OVER (ORDER BY`MONTH` ) As rolling_total
FROM Rolling_Total;

SELECT company, YEAR(`date`) AS `YEAR`, sum(total_laid_off) AS Total_off
FROM world_layoff.layoffs_staging2
GROUP BY company,`YEAR`
ORDER BY 3 DESC;



-- Which companies rank top 5the highest number of layoffs in each year ? 

WITH Company_Year (company,years,total_laid_off)AS
(
SELECT company, YEAR(`date`) , sum(total_laid_off) AS Total_off
FROM world_layoff.layoffs_staging2
GROUP BY company,YEAR(`date`)
), Company_Year_Rank AS
(SELECT * ,
dense_rank() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL)
SELECT * 
FROM Company_Year_Rank
WHERE Ranking <= 5
;


SELECT * 
FROM world_layoff.layoffs_staging2;

-- Which industries had the highest number of layoffs in total? 

SELECT industry , sum(total_laid_off) AS Total_off , count(company) AS Number_Company
FROM world_layoff.layoffs_staging2
WHERE industry OR total_laid_off IS NOT NULL
GROUP BY industry
ORDER BY Total_off DESC;



-- Which countries experienced the most layoffs, and how does this correlate with the total funds raised by companies in those countries?

WITH  Company_Total_off AS
(
SELECT country,  sum(total_laid_off) AS Total_off , sum(funds_raised_millions) as Total_funds_raised_million
FROM world_layoff.layoffs_staging2
where total_laid_off IS NOT NULL AND funds_raised_millions IS NOT NULL
GROUP BY country), Company_rank AS
(SELECT * ,
DENSE_RANK() OVER (ORDER BY Total_off DESC) AS Ranking_by_total_off,
DENSE_RANK() OVER (ORDER BY Total_funds_raised_million DESC) AS Ranking_by_fund_raised
FROM Company_Total_off)
SELECT 
    country,
    Total_off,
    Total_funds_raised_million,
    Ranking_by_total_off,
    Ranking_by_fund_raised
FROM 
    Company_rank;
    
-- For companies that have gone public (Post-IPO), how does their layoff percentage compare to private companies?

SELECT
    CASE 
        WHEN stage = 'Post-IPO' THEN 'Public'
        ELSE 'Private'
    END AS company_status,
    COUNT(*) AS company_count,
    SUM(total_laid_off) AS total_laid_off,
    (SUM(total_laid_off) * 100.0 / (SELECT SUM(total_laid_off) FROM world_layoff.layoffs_staging2)) AS percentage_laid_off
FROM 
    world_layoff.layoffs_staging2
GROUP BY 
    CASE 
        WHEN stage = 'Post-IPO' THEN 'Public'
        ELSE 'Private'
    END;

