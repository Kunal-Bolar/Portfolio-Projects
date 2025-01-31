/*
Covid 19 Data Exploration 

Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types

*/

# importing data using load data statement
create DATABASE project_portfolio;

SET GLOBAL local_infile = 1; # The local_infile system variable determines whether the MySQL server allows clients to use the LOAD DATA LOCAL INFILE statement.

CREATE TABLE covid_deaths (
  iso_code text,
  continent text,
  location text,
  `date` datetime,
  population bigint DEFAULT NULL,
  total_cases int DEFAULT NULL,
  new_cases int DEFAULT NULL,
  new_cases_smoothed double DEFAULT NULL,
  total_deaths int DEFAULT NULL,
  new_deaths int DEFAULT NULL,
  new_deaths_smoothed double DEFAULT NULL,
  total_cases_per_million double DEFAULT NULL,
  new_cases_per_million double DEFAULT NULL,
  new_cases_smoothed_per_million double DEFAULT NULL,
  total_deaths_per_million double DEFAULT NULL,
  new_deaths_per_million double DEFAULT NULL,
  new_deaths_smoothed_per_million double DEFAULT NULL,
  reproduction_rate double DEFAULT NULL,
  icu_patients int DEFAULT NULL,
  icu_patients_per_million double DEFAULT NULL,
  hosp_patients int DEFAULT NULL,
  hosp_patients_per_million double DEFAULT NULL,
  weekly_icu_admissions double DEFAULT NULL,
  weekly_icu_admissions_per_million double DEFAULT NULL,
  weekly_hosp_admissions double DEFAULT NULL,
  weekly_hosp_admissions_per_million double DEFAULT NULL
);


# load data statement
LOAD DATA INFILE 'CovidDeaths.csv' into table covid_deaths
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

create table covid_vaccinations(
iso_code text,
continent text,
location text,
`date` datetime,
new_tests int default null,
total_tests int default null,
total_tests_per_thousand double DEFAULT NULL,
new_tests_per_thousand double DEFAULT NULL,
new_tests_smoothed int default null,
new_tests_smoothed_per_thousand double DEFAULT NULL,
positive_rate double DEFAULT NULL,
tests_per_case double DEFAULT NULL,
tests_units text,
total_vaccinations int default null,
people_vaccinated int default null,
people_fully_vaccinated int default null,
new_vaccinations int default null,
new_vaccinations_smoothed int default null,
total_vaccinations_per_hundred double DEFAULT NULL,
people_vaccinated_per_hundred double DEFAULT NULL,
people_fully_vaccinated_per_hundred double DEFAULT NULL,
new_vaccinations_smoothed_per_million int DEFAULT NULL,
stringency_index double DEFAULT NULL,
population_density double DEFAULT NULL,
median_age double DEFAULT NULL,
aged_65_older double DEFAULT NULL,
aged_70_older double DEFAULT NULL,
gdp_per_capita double DEFAULT NULL,
extreme_poverty double DEFAULT NULL,
cardiovasc_death_rate double DEFAULT NULL,
diabetes_prevalence double DEFAULT NULL,
female_smokers double DEFAULT NULL,
male_smokers double DEFAULT NULL,
handwashing_facilities double DEFAULT NULL,
hospital_beds_per_thousand double DEFAULT NULL,
life_expectancy double DEFAULT NULL,
human_development_index double DEFAULT NULL
);

LOAD DATA INFILE 'CovidVaccinations.csv' into table covid_vaccinations
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


#select * from covid_deaths;
#select * from covid_vaccinations;

-- shows likelihood of dying if you contract covid in india
-- total cases vs total death 
select location, `date`, total_cases, new_cases, total_deaths, population, round((total_deaths/total_cases)*100,2) as death_percent
from covid_deaths
where location like '%india%'
order by 1, 2;

-- looking at total cases vs population
-- show what percentage of population got covid
select location, `date`,population, total_cases, round((total_cases/population)*100,4) as infected_percentage
from covid_deaths
where location = 'India'
order by 1, 2;

-- looking for countries with highest infection rate compared to popluation
select location,population, max(total_cases) as highest_infection_count, max((total_cases)/population)* 100 as infected_population_percentage
from covid_deaths
group by location, population
order by infected_population_percentage desc;

-- using cte to rank country with highest infection rate by population
WITH ranked_data AS (
    SELECT 
        location,
        population,
        MAX(total_cases) AS highest_infection_count,
        MAX(total_cases / population) * 100 AS infected_population_percentage
    FROM 
        covid_deaths
    GROUP BY 
        location, population
)
SELECT 
	ROW_NUMBER() OVER (ORDER BY infected_population_percentage DESC) AS `rank`,
    location,
    population,
    highest_infection_count,
    infected_population_percentage
FROM 
    ranked_data
ORDER BY 
    infected_population_percentage DESC;

-- showing countries with the highest death count per population

select 
	location, 
    max(total_deaths) as total_death_count
	#max(total_deaths)/population*100  as population_death_percentage
from covid_deaths
where location not in ('Europe','oceania','North America','South America','Asia','Africa','International','World','European Union')
group by location
order by total_death_count desc;

-- death count by continents
 
select continent, sum(new_cases) as total_death_count from covid_deaths
where continent is not null
group by continent
order by total_death_count desc;

-- global death percentage
select 
	#`date`,
    sum(new_cases) as total_cases,
    sum(new_deaths) as total_deaths,
    (sum(new_deaths)/sum(new_cases))*100 as death_percentage
from covid_deaths
where continent is not null;
#group by `date`;

-- how many people got vaccinated # vaccination vs populations
# in order to to remove percentage of people vaccinated in a country we got to use CTE
with vaccinated_percentage_Cte(continent, location, population, new_vaccinations, rolling_count_vaccinated) 
as (
select 
	d.continent, 
    d.location, 
    d.population, 
    v.new_vaccinations,
    sum(v.new_vaccinations) over (partition by d.location order by d.location, d.date) as rolling_count_vaccinated
from covid_deaths d
join covid_vaccinations v
on d.date = v.date and d.location = v.location
where d.continent is not null
order by d.location
)

select location, population,max(rolling_count_vaccinated) as total_vacinated, max(rolling_count_vaccinated)/population*100 as percentage_vacinated
from vaccinated_percentage_Cte
where continent is not null and location not in ('Europe','North America','South America','Asia','Africa','Oceania')
group by location, continent, population
order by percentage_vacinated desc;

# since vaccinated_percentage > population that means new_vaccination column is faulty data, so we will use people_fully_vaccinated columns instead

# vaccinated percentage of each country by end of 2021
select d.continent,d.location,max(v.people_fully_vaccinated) as count_of_fully_vaccinated,d.population, round((max(v.people_fully_vaccinated)/d.population)*100,2) as vaccinated_percentage
from covid_deaths as d
join covid_vaccinations as v
on d.date = v.date and d.location = v.location
where d.continent is not null and d.location not in('North America','South America','Asia','Africa','Europe','Oceania')
group by d.continent,d.location,d.population
order by vaccinated_percentage desc;

-- Using Temp Table to perform  previous query
drop temporary table if exists percentage_vaccinated_population;
create temporary table percentage_vaccinated_population
(
continent char(255),
location char(255),
count_of_fully_vaccinated int default null,
population bigint default null,
vaccinated_percentage double default null
);

insert into percentage_vaccinated_population
select d.continent,d.location,max(v.people_fully_vaccinated) as count_of_fully_vaccinated,d.population, round((max(v.people_fully_vaccinated)/d.population)*100,2) as vaccinated_percentage
from covid_deaths as d
join covid_vaccinations as v
on d.date = v.date and d.location = v.location
where d.continent is not null and d.location not in('North America','South America','Asia','Africa','Europe','Oceania')
group by d.continent,d.location,d.population
order by vaccinated_percentage desc;

select * from percentage_vaccinated_population;


-- Creating View to store data for later visualizations
#1
create view india_death_percent as
select location, `date`, total_cases, new_cases, total_deaths, population, round((total_deaths/total_cases)*100,2) as death_percent
from covid_deaths
where location like '%india%'
order by 1, 2;
#2
create view india_infected_percent as
select location, `date`,population, total_cases, round((total_cases/population)*100,4) as infected_percentage
from covid_deaths
where location = 'India'
order by 1, 2;
#3
create view countries_highest_infected as
select location,population, max(total_cases) as highest_infection_count, max((total_cases)/population)* 100 as infected_population_percentage
from covid_deaths
group by location, population
order by infected_population_percentage desc;
#4
create view countries_highest_death_count as
select 
	location, 
    max(total_deaths) as total_death_count
	#max(total_deaths)/population*100  as population_death_percentage
from covid_deaths
where location not in ('Europe','oceania','North America','South America','Asia','Africa','International','World','European Union')
group by location
order by total_death_count desc;
#5
create view continents_death_count as 
select continent, sum(new_cases) as total_death_count from covid_deaths
where continent is not null
group by continent
order by total_death_count desc;
#6
create view global_cases as 
select 
	#`date`,
    sum(new_cases) as total_cases,
    sum(new_deaths) as total_deaths,
    (sum(new_deaths)/sum(new_cases))*100 as death_percentage
from covid_deaths
where continent is not null;
#group by `date`;
#7
create view countries_vaccinated_percentage as
select d.continent,d.location,max(v.people_fully_vaccinated) as count_of_fully_vaccinated,d.population, round((max(v.people_fully_vaccinated)/d.population)*100,2) as vaccinated_percentage
from covid_deaths as d
join covid_vaccinations as v
on d.date = v.date and d.location = v.location
where d.continent is not null and d.location not in('North America','South America','Asia','Africa','Europe','Oceania')
group by d.continent,d.location,d.population
order by vaccinated_percentage desc;



