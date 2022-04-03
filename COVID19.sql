use PortfolioProject;
create table CovidVaccinations
(
iso_code varchar(5),
continent varchar(10),
location varchar(30),
date date,
total_tests int,
new_tests int,
total_tests_per_thousand float(10,5),
new_tests_per_thousand float(10,5),
new_tests_smoothed int,
new_tests_smoothed_per_thousand float(10,5),
positive_rate float(10,7),
tests_per_case float(10,5),
tests_units varchar(20),
total_vaccinations int,
people_vaccinated int,
people_fully_vaccinated int,
total_boosters int,
new_vaccinations int,
new_vaccinations_smoothed int,
total_vaccinations_per_hundred float(10,5),
people_vaccinated_per_hundred float(10,5),
people_fully_vaccinated_per_hundred float(10,5),
total_boosters_per_hundred float(10,5),
new_vaccinations_smoothed_per_million int,
new_people_vaccinated_smoothed int,
new_people_vaccinated_smoothed_per_hundred float(10,7),
stringency_index float(10,5),
population_density float(10,5),
median_age float(10,5),
aged_65_older float(10,5),
aged_70_older float(10,5),
gdp_per_capita float(15,5),
extreme_poverty float(10,5),
cardiovasc_death_rate float(10,5),
diabetes_prevalence float(10,5),
female_smokers float(10,5),
male_smokers float(10,5),
handwashing_facilities float(10,5),
hospital_beds_per_thousand float(10,5),
life_expectancy float(10,5),
human_development_index float(10,5),
excess_mortality_cumulative_absolute float(10,5),
excess_mortality_cumulative float(10,5),
excess_mortality float(10,5),
excess_mortality_cumulative_per_million float(25,20)
);
create table CovidDeaths
(
iso_code varchar(5),
continent varchar(10),
location varchar(30),
date date,
population int,
total_cases int,
new_cases int,
new_cases_smoothed float(10,5),
total_deaths int,
new_deaths int,
new_deaths_smoothed float(10,5),
total_cases_per_million float(10,5),
new_cases_per_million float(10,5),
new_cases_smoothed_per_million float(10,5),
total_deaths_per_million float(10,5),
new_deaths_per_million float(10,5),
new_deaths_smoothed_per_million float(10,5),
reproduction_rate float(10,5),
icu_patients int,
icu_patients_per_million float(10,5),
hosp_patients int,
hosp_patients_per_million float(10,5),
weekly_icu_admissions int,
weekly_icu_admissions_per_million float(10,5),
weekly_hosp_admissions int,
weekly_hosp_admissions_per_million float(10,5)
);

load data local infile '/Users/huseinjauhari/Downloads/CovidVaccinations.csv'
into table CovidVaccinations
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

load data local infile '/Users/huseinjauhari/Downloads/CovidDeaths.csv'
into table CovidDeaths
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

-- Select Data that we are going to be using
select location, date, total_cases, new_cases, total_deaths, population
from CovidDeaths
order by 1,2;

-- Looking at total cases vs total deaths
-- Shows the likelihood of dying if you contract COVID in your country
select location, date, total_cases, new_cases, total_deaths, population, (total_deaths/total_cases)*100 as DeathPercentage
from CovidDeaths
where location like '%states%'
order by 1,2;

-- Looking at total cases vs population
-- Shows what % of population got COVID
select location, date, population, total_cases, (total_cases/population)*100 as PercentPopulationInfected
from CovidDeaths
where location like '%states%'
order by 1,2;

-- Looking at countries with highest infection rate copmared to population
select location, population, max(total_cases) as HighestInfectionCount, max((total_cases/population))*100 as PercentPopulationInfected
from CovidDeaths
group by location, population
order by PercentPopulationInfected desc;

-- Looking at countries with the highest death count per population
select location, max(total_deaths) as TotalDeathCount
from CovidDeaths
where iso_code!='OWID_'
group by location
order by TotalDeathCount desc;

-- Showing the continents with the highest death count per population
select location, max(total_deaths) as TotalDeathCount
from CovidDeaths
where location='Europe' or location='North America' or location='South America' or location='Asia' or location='Africa' or location='Oceania' 
group by location
order by TotalDeathCount desc;

-- GLOBAL NUMBERS
select date, sum(new_cases) as TotalCases, sum(new_deaths) as TotalDeaths, (sum(new_deaths)/sum(new_cases))*100 as DeathPercentage
from CovidDeaths
where iso_code!='OWID_'
group by date
order by 1,2;

-- Finding out global total cases and deaths to date and the death percentage
select sum(new_cases) as TotalCases, sum(new_deaths) as TotalDeaths, (sum(new_deaths)/sum(new_cases))*100 as DeathPercentage
from CovidDeaths
where iso_code!='OWID_'
order by 1,2;


-- Looking at total population vs total vaccinations with a rolling counter of vaccinations
select d.continent, d.location, d.date, d.population, v.new_vaccinations, sum(v.new_vaccinations) over (partition by d.location order by d.location, d.date) as RollingPeopleVaccinated
from CovidDeaths d, CovidVaccinations v
where d.iso_code=v.iso_code and d.continent=v.continent and d.location=v.location and d.date=v.date and d.iso_code!='OWID_'
order by 2,3;

-- Using CTE Expression to get rolling percentage of people vaccinated
with PopvsVac (continent, location, date, population, new_vaccinations, RollingPeopleVaccinated)
as(
select d.continent, d.location, d.date, d.population, v.new_vaccinations, sum(v.new_vaccinations) over (partition by d.location order by d.location, d.date) as RollingPeopleVaccinated
from CovidDeaths d, CovidVaccinations v
where d.iso_code=v.iso_code and d.continent=v.continent and d.location=v.location and d.date=v.date and d.iso_code!='OWID_'
)
select *, (RollingPeopleVaccinated/population)*100 as PercentPeopleVaccinated
from PopvsVac;

-- Using Temp Tables to get rolling percentage of people vaccinated
drop table if exists PercentPopulationVaccinated;
create table PercentPopulationVaccinated(
continent varchar(10),
location varchar(30),
date date,
population int,
new_vaccinations int,
RollingPeopleVaccinated numeric
);

insert into PercentPopulationVaccinated
select d.continent, d.location, d.date, d.population, v.new_vaccinations, sum(v.new_vaccinations) over (partition by d.location order by d.location, d.date) as RollingPeopleVaccinated
from CovidDeaths d, CovidVaccinations v
where d.iso_code=v.iso_code and d.continent=v.continent and d.location=v.location and d.date=v.date and d.iso_code!='OWID_';

select *, (RollingPeopleVaccinated/population)*100 as PPV
from PercentPopulationVaccinated;

-- Creating view to store data for later visualizations
create view PercentPopulationVaccinated as
select d.continent, d.location, d.date, d.population, v.new_vaccinations, sum(v.new_vaccinations) over (partition by d.location order by d.location, d.date) as RollingPeopleVaccinated
from CovidDeaths d, CovidVaccinations v
where d.iso_code=v.iso_code and d.continent=v.continent and d.location=v.location and d.date=v.date and d.iso_code!='OWID_';




/*

Queries used for Tableau Project

*/

-- 1.
-- Finding out global total cases and deaths to date and the death percentage
select sum(new_cases) as TotalCases, sum(new_deaths) as TotalDeaths, (sum(new_deaths)/sum(new_cases))*100 as DeathPercentage
from CovidDeaths
where iso_code!='OWID_'
order by 1,2;


-- 2.
-- Showing the continents with the highest death count per population
select location, sum(new_deaths) as TotalDeathCount
from CovidDeaths
where location='Europe' or location='North America' or location='South America' or location='Asia' or location='Africa' or location='Oceania' 
group by location
order by TotalDeathCount desc;

-- 3.
-- Looking at countries with highest infection rate copmared to population
select location, population, max(total_cases) as HighestInfectionCount, max((total_cases/population))*100 as PercentPopulationInfected
from CovidDeaths
where iso_code!='OWID_'
group by location, population
order by PercentPopulationInfected desc;

-- 4.
-- Looking at countries with highest infection rate copmared to population, grouped by date this time
select location, population, date, max(total_cases) as HighestInfectionCount, max((total_cases/population))*100 as PercentPopulationInfected
from CovidDeaths
where iso_code!='OWID_'
group by location, population, date
order by PercentPopulationInfected desc;






