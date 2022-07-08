/* 
Data Exploration of COVID-19 data in Scandinavia. 
Skilled used: Aggregate Functions, Casting Data types, Creating Views, Joins, Self-Joins, 
Subqueries, Temp Tables, Window Functions.
*/

-- Looking at Total Cases vs Total Deaths

SELECT Location, date, total_cases, total_deaths, ROUND(((total_deaths/total_cases::FLOAT)::NUMERIC*100),2 ) AS percentage_of_deaths
FROM public.cases_data
ORDER BY 1,2

-- Compare highest percentage at any given time total_deaths/total_cases by Country for any day 
SELECT Location, MAX(ROUND(((total_deaths/total_cases::FLOAT)::NUMERIC*100),2)) AS percentage_of_deaths
FROM public.cases_data
GROUP BY 1

-- Find percentage of population that have tested positive for covid by date
SELECT Location, date, ROUND((total_cases/population::FLOAT )::NUMERIC* 100,2) AS percentage_of_cases_by_pop
FROM public.cases_data
ORDER BY 2, 1

-- Day of first death recorded death by covid-19 by Country
SELECT location, date AS first_covid_death
FROM(
	SELECT Location, RANK() OVER (PARTITION BY Location ORDER BY date ASC) AS day_of_death, date
	FROM public.cases_data
	WHERE total_deaths IS NOT NULL) a
WHERE day_of_death = 1
ORDER BY 2 ASC

-- Maximum number of new cases in any day
SELECT a.location, max_new_cases, b.date
FROM(
	SELECT location, MAX(new_cases) AS max_new_cases
	FROM public.cases_data
	GROUP BY 1
) a
LEFT JOIN public.cases_data b
ON a.max_new_cases = b.new_cases AND a.location = b.location

-- Maxmimum number of new deaths in a day
SELECT a.location, max_new_deaths, b.date
FROM(
	SELECT location, MAX(new_deaths) AS max_new_deaths
	FROM public.cases_data
	GROUP BY 1
) a
LEFT JOIN public.cases_data b
ON a.max_new_deaths = b.new_deaths AND a.location = b.location

-- Number of vaccinations vs total populations
WITH rolling_vacc_data AS (
	SELECT c.location, c.date, c.population, v.new_vaccinations, 
			SUM(v.new_vaccinations) OVER (PARTITION BY c.location ORDER BY c.location, c.date) AS rolling_vaccinations
	FROM public.cases_data c
	JOIN public.vacc_data v
	ON c.location = v.location AND c.date = v.date
	WHERE v.new_vaccinations IS NOT NULL
	ORDER BY 1,2)
SELECT location, date, rolling_vaccinations/population::FLOAT * 100 AS percentage_population_vaccinated
FROM rolling_vacc_data
ORDER BY 2

-- Temp table of the above query
CREATE TEMP TABLE  percent_population_vaccinated AS(
SELECT c.location, c.date, c.population, v.new_vaccinations, 
		SUM(v.new_vaccinations) OVER (PARTITION BY c.location ORDER BY c.location, c.date) AS rolling_vaccinations
FROM public.cases_data c
JOIN public.vacc_data v
ON c.location = v.location AND c.date = v.date
WHERE v.new_vaccinations IS NOT NULL
)


SELECT location, date, rolling_vaccinations/population::FLOAT * 100 AS percentage_population_vaccinated
FROM percent_population_vaccinated
ORDER BY 2

-- Percentage of population vaccinated and percentage of population fully vaccinated
DROP TABLE IF EXISTS vacc_status_by_population

CREATE TEMP TABLE vacc_status_by_population AS(
SELECT 
	v.location, v.date, people_vaccinated/population::FLOAT * 100 AS percentage_population_vaccinated, 
	people_fully_vaccinated/population::FLOAT * 100 AS percentage_population_fully_vaccinated,
	total_boosters/population::FLOAT * 100 AS percentage_population_boosted
FROM public.vacc_data v 
JOIN public.cases_data c
ON v.location = c.location AND v.date = c.date
WHERE people_vaccinated IS NOT NULL
ORDER BY 2 )

-- Return the whole vacc_status_by_population temp table
SELECT * 
FROM vacc_status_by_population

-- Return the vacc_status_by_population for the most recent day
WITH data_recorded AS (
	SELECT *, RANK() OVER (PARTITION BY location ORDER BY date DESC) AS rank_most_recent
	FROM vacc_status_by_population
)
SELECT location, date, percentage_population_vaccinated, percentage_population_fully_vaccinated, percentage_population_boosted
FROM data_recorded
WHERE rank_most_recent = 1

-- Create Views for future visualizations 

-- Percentage of population that has tested positive in each country by day
CREATE VIEW percentage_of_population_tested_positive AS (
SELECT Location, date, ROUND((total_cases/population::FLOAT )::NUMERIC* 100,2) AS percentage_of_cases_by_pop
FROM public.cases_data
ORDER BY 2, 1 )

-- Vaccination status by population by day
CREATE VIEW vacc_status_by_population
AS(
SELECT 
	v.location, v.date, people_vaccinated/population::FLOAT * 100 AS percentage_population_vaccinated, 
	people_fully_vaccinated/population::FLOAT * 100 AS percentage_population_fully_vaccinated,
	total_boosters/population::FLOAT * 100 AS percentage_population_boosted
FROM public.vacc_data v 
JOIN public.cases_data c
ON v.location = c.location AND v.date = c.date
WHERE people_vaccinated IS NOT NULL
ORDER BY 2 )
