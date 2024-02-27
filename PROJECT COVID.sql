----DATA EXPLORATION
SELECT * 
FROM CovidProject..COVID_Deaths
ORDER BY 3, 4

SELECT location, date, total_cases, new_cases, total_deaths, population
FROM CovidProject..COVID_Deaths
ORDER BY Location, date

----DATA CLEANSING
/* After a thorough review of the data, it has come to my attention that the location column contains several entries that do not align 
with the project's objectives. As such, these entries are deemed unnecessary and will be removed from the dataset.*/
--Data Clensing
SELECT location,COUNT(DISTINCT location) AS NotUsefulEntries
FROM CovidProject..COVID_Deaths
WHERE continent IS NULL AND location LIKE '%income%' 
GROUP BY location

DELETE FROM CovidProject..COVID_Deaths
WHERE continent IS NULL AND location LIKE '%income%'

SELECT location,COUNT(DISTINCT location) AS NotUsefulEntries
FROM CovidProject..COVID_Deaths
WHERE continent IS NULL AND location LIKE '%income%' 
GROUP BY location

/* As well, , it has been observed that the location column contains instances where the geographical region of Europe is divided into 
two separate entries: "Europe" and "European Union". This duplication is not aligned with the project's objectives and may potentially 
skew the data analysis. Therefore, it has been decided to merge these two entries into a single category, "Europe", 
to maintain consistency and accuracy in the dataset.*/

SELECT location, COUNT(location) AS ContinentOccurrences
FROM CovidProject..COVID_Deaths
WHERE continent IS NULL AND location <> 'World'
GROUP BY location


SELECT location
FROM CovidProject..COVID_Deaths
WHERE continent IS NULL AND location <> 'World'
UPDATE CovidProject..COVID_Deaths
SET location = 'Europe'
WHERE location = 'European Union'

SELECT location, COUNT(location) AS NewContinentOccurrences
FROM CovidProject..COVID_Deaths
WHERE continent IS NULL AND location <> 'World'
GROUP BY location 


----TOTAL CASES vs TOTAL DEATHS
-- Displays the percentage of deaths caused by COVID in the United States by date
SELECT location, date, total_cases, total_deaths, (CAST(total_deaths AS DECIMAL) / total_cases)*100 AS death_percentage
FROM CovidProject..COVID_Deaths
WHERE location LIKE 'United States'
ORDER BY date DESC, death_percentage DESC

----TOTAL CASES vs POPULATION
-- Displays the percentage of the COVID cases with respect to population the United States by date 
SELECT Location, date, total_cases, population, (total_cases / population)*100 AS InfectionRate
FROM CovidProject..COVID_Deaths
WHERE LOCATION LIKE 'United States'
ORDER BY Location, date

----INFECTION RATE vs POPULATION
--Displays the Countries wit the highest infection rate compared to population
SELECT Location, MAX(total_cases) AS HighestInfectionCount, population, (MAX(total_cases) / population)*100 AS InfectionRate
FROM CovidProject..COVID_Deaths
WHERE continent IS NOT NULL AND location <> 'World'
GROUP BY Location, population
ORDER BY InfectionRate DESC

----LOCATION vs HIGHEST DEATH COUNT
--Displays the highest death counts by Country
SELECT Location, MAX(CAST(total_deaths AS INT)) AS HighestDeathCount
FROM CovidProject..COVID_Deaths
WHERE continent IS NOT NULL AND location <> 'World'
GROUP BY Location
ORDER BY HighestDeathCount DESC

--Displays the highest death counts by Continent
SELECT location, MAX(CAST(total_deaths AS INT)) AS HighestDeathCount
FROM CovidProject..COVID_Deaths
WHERE continent IS NULL AND location <> 'World'
GROUP BY location
ORDER BY HighestDeathCount DESC

---- GLOBAL NUMBERS
SELECT SUM(new_cases) AS GlobalCases, SUM(CAST(new_deaths AS int)) AS GlobalDeaths,
SUM(CAST(new_deaths as DECIMAL))/SUM(CAST(new_cases as DECIMAL))*100 as GlobalDeathPercentage
FROM CovidProject..COVID_Deaths
WHERE continent IS NOT NULL 
ORDER BY 1, 2

----POPULATION vs VACCIONATIONS PARTITIONED BY LOCATION
SELECT dth.continent, dth.location, dth.date, dth.population, vax.new_vaccinations, SUM(CONVERT(decimal, vax.new_vaccinations)) 
OVER (PARTITION BY dth.location ORDER BY dth.location, dth.date) AS VaxByCountryAndDate
FROM CovidProject..COVID_Deaths AS dth
JOIN CovidProject..COVID_Vaccinations AS vax
	ON dth.location = vax.location AND dth.date = vax.date
WHERE dth.continent IS NOT NULL
order by 2, 3



---- VACCINATION PERCENTAGE BY COUNTRY AND DATE
WITH TotalVax (continent, location, date, population, new_vaccinations, VaxByCountryAndDate) AS(
SELECT dth.continent, dth.location, dth.date, dth.population, vax.new_vaccinations, SUM(CONVERT(decimal, vax.new_vaccinations)) 
OVER (PARTITION BY dth.location ORDER BY dth.location, dth.date) AS VaxByCountryAndDate
FROM CovidProject..COVID_Deaths AS dth
JOIN CovidProject..COVID_Vaccinations AS vax
	ON dth.location = vax.location AND dth.date = vax.date
WHERE dth.continent IS NOT NULL)
SELECT *, (VaxByCountryAndDate/population)*100 as VaxPercentage
FROM TotalVax
ORDER BY location, date

--TEMP TABLE
DROP TABLE IF EXISTS #DeathsVAX2;

CREATE TABLE #DeathsVAX2
(
    continent nvarchar(20),
    location nvarchar(50),
    date datetime,
    population numeric,
    new_cases numeric,
    new_deaths numeric,
    new_vaccination numeric,
    DeathBeforeVaxPctg numeric,
    DeathAfterVaxPctg numeric
);

INSERT INTO #DeathsVAX2
SELECT 
    dth.continent, 
    dth.location, 
    dth.date, 
    dth.population, 
    dth.new_cases, 
    dth.new_deaths, 
    vax.new_vaccinations,
    CASE WHEN vax.new_vaccinations < 0 THEN
        SUM(CONVERT(numeric, vax.new_vaccinations)) OVER (PARTITION BY dth.location ORDER BY dth.location, dth.date) 
    ELSE
        0
    END AS cumulative_vaccinations,
    CASE WHEN vax.new_vaccinations > 0 THEN
        SUM(dth.new_deaths) OVER (PARTITION BY dth.location ORDER BY dth.location, dth.date) 
    ELSE
        0
    END 
FROM 
    CovidProject..COVID_Deaths AS dth
JOIN 
    CovidProject..COVID_Vaccinations AS vax ON dth.location = vax.location AND dth.date = vax.date
WHERE 
    dth.continent IS NOT NULL;

SELECT DeathBeforeVaxPctg, DeathAfterVaxPctg 
FROM #DeathsVAX2;


