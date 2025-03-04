/*

Cleaning Data in SQL Queries

*/

/*
SET GLOBAL – This sets the variable for the entire MySQL server, meaning it applies to all sessions.
local_infile – This system variable controls whether the LOAD DATA LOCAL INFILE statement is allowed.
= 1 – This enables (1 for ON) the feature.
*/
set global local_infile  = 1;


-- importing a staging table to insert messy data 
-- Clean and transform data, then insert into the final table
CREATE TABLE nashville_housing_staging (
    UniqueID VARCHAR(255),
    ParcelID VARCHAR(255),
    LandUse VARCHAR(255),
    PropertyAddress VARCHAR(255),
    SaleDate VARCHAR(255),
    SalePrice VARCHAR(255),
    LegalReference VARCHAR(255),
    SoldAsVacant VARCHAR(3),
    OwnerName VARCHAR(255),
    OwnerAddress VARCHAR(255),
    Acreage VARCHAR(255),
    TaxDistrict VARCHAR(255),
    LandValue VARCHAR(255),
    BuildingValue VARCHAR(255),
    TotalValue VARCHAR(255),
    YearBuilt VARCHAR(255),
    Bedrooms VARCHAR(255),
    FullBath VARCHAR(255),
    HalfBath VARCHAR(255)
);


-- dynamically get set values instead of manually typing it
SELECT CONCAT(
    'SET ', 
    GROUP_CONCAT(CONCAT(column_name, ' = NULLIF(NULLIF(', column_name, ", ''), 'null')") SEPARATOR ', '), 
    ';'
) 
FROM information_schema.columns 
WHERE table_schema = 'project_portfolio' AND table_name = 'nashville_housing_staging';



-- load data statement for faster importing
-- load data local infile '{file_name}' into table tb_name    #  Used to load data from a file on your local computer.
-- load data infile '{file_name}' into table tb_name          # Used to load data from a file on the database server. 
LOAD DATA LOCAL INFILE 'D:/portfolio projects alex/project3_data_cleaning_in_sql/Nashville Housing Data for Data Cleaning.csv'
INTO TABLE nashville_housing_staging
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES  
# pasting the set values after load data statment 
(UniqueID, ParcelID, LandUse, PropertyAddress, SaleDate, SalePrice, LegalReference, SoldAsVacant, OwnerName, OwnerAddress, Acreage, TaxDistrict, LandValue, BuildingValue, TotalValue, YearBuilt, Bedrooms, FullBath, HalfBath)
SET UniqueID = NULLIF(NULLIF(UniqueID, ''), 'null'), # this is to ensure that csv file with "blanks" and "null strings" are converted into null values
	ParcelID = NULLIF(NULLIF(ParcelID, ''), 'null'),
    LandUse = NULLIF(NULLIF(LandUse, ''), 'null'),
    PropertyAddress = NULLIF(NULLIF(PropertyAddress, ''), 'null'),
    SaleDate = NULLIF(NULLIF(SaleDate, ''), 'null'), 
    SalePrice = NULLIF(NULLIF(SalePrice, ''), 'null'),
    LegalReference = NULLIF(NULLIF(LegalReference, ''), 'null'),
    SoldAsVacant = NULLIF(NULLIF(SoldAsVacant, ''), 'null'),
    OwnerAddress = NULLIF(NULLIF(OwnerAddress, ''), 'null'), 
    OwnerName = NULLIF(NULLIF(OwnerName, ''), 'null'), 
    Acreage = NULLIF(NULLIF(Acreage, ''), 'null'),
    TaxDistrict = NULLIF(NULLIF(TaxDistrict, ''), 'null'),
	LandValue = NULLIF(NULLIF(LandValue, ''), 'null'),
    BuildingValue = NULLIF(NULLIF(BuildingValue, ''), 'null'),
    TotalValue = NULLIF(NULLIF(TotalValue, ''), 'null'), 
	Bedrooms = NULLIF(NULLIF(Bedrooms, ''), 'null'), 
	YearBuilt = NULLIF(NULLIF(YearBuilt, ''), 'null'),
    FullBath = NULLIF(NULLIF(FullBath, ''), 'null'), 
    HalfBath = NULLIF(NULLIF(HalfBath, ''), 'null');
     
     
select * from project_portfolio.nashville_housing_staging;

-- Standartize date format

UPDATE nashville_housing_staging
SET SaleDate = STR_TO_DATE(SaleDate, '%M %d, %Y') -- april 9 2013 --> This identifies rows that MySQL cannot convert.
WHERE SaleDate IS NOT NULL;                       -- and converts it into yyyy-mm-dd 2013-04-09 dateTime

ALTER TABLE nashville_housing_staging 
MODIFY COLUMN SaleDate DATE;  # converts it into date datatype


-- populate Property Address data
select * from nashville_housing_staging
where PropertyAddress is null;


SELECT a.UniqueID,a.ParcelID, a.PropertyAddress, b.UniqueID, b.ParcelID, b.PropertyAddress, ifnull(a.PropertyAddress, b.PropertyAddress) as filler
from nashville_housing_staging a 
join nashville_housing_staging b
	on a.ParcelID = b.ParcelID 
    and a.UniqueID <> b.UniqueID
where a.PropertyAddress is null;

# filling addresses
UPDATE nashville_housing_staging a
JOIN nashville_housing_staging b
    ON a.ParcelID = b.ParcelID 
    AND a.UniqueID <> b.UniqueID
SET a.PropertyAddress = b.PropertyAddress
WHERE a.PropertyAddress IS NULL;


select * from nashville_housing_staging 
where PropertyAddress is null;


-- Breaking out Address into Individual Columns (Address, City, State)

select PropertyAddress
	,substring_index(PropertyAddress,",",1) as Property_address
    ,trim(substring_index(PropertyAddress,",",-1)) as Property_city
from nashville_housing_staging;

ALTER TABLE nashville_housing_staging
add column Property_Address varchar(255),
add column Property_City varchar(255);

Update nashville_housing_staging
SET Property_Address = substring_index(PropertyAddress,",",1),
	Property_City = trim(substring_index(PropertyAddress,",",-1));

-- for owneraddress
select OwnerAddress
	,substring_index(OwnerAddress,',',1) as Owner_address
    ,substring_index(substring_index(OwnerAddress,',',2),',',-1) as Owner_City
    ,substring_index(OwnerAddress,',',-1) as Owner_State
from nashville_housing_staging;

ALTER TABLE nashville_housing_staging
add column Owner_Address varchar(255),
add column Owner_City varchar(255),
add column Owner_State varchar(255);


update nashville_housing_staging
set Owner_Address = substring_index(OwnerAddress,',',1),
	Owner_City = substring_index(substring_index(OwnerAddress,',',2),',',-1),
    Owner_State = substring_index(OwnerAddress,',',-1);


-- converting 'Y' & 'N' to 'Yes' and 'No' in SoldAsVacant column
select SoldAsVacant,
CASE SoldAsVacant
	WHEN 'Y' then 'Yes'
    WHEN 'N' then  'No'
    Else SoldAsVacant
END as SoldAsVacant_new
from nashville_housing_staging;

update nashville_housing_staging
set SoldAsVacant = CASE SoldAsVacant
						WHEN 'Y' then 'Yes'
						WHEN 'N' then  'No'
						Else SoldAsVacant
				   END;

-- verification of changes
select distinct(SoldAsVacant) 
from nashville_housing_staging;

-- removing duplicates
start transaction;
with Duplicate_cte as (
	select 
			*,
			row_number() over(partition by ParcelID, Property_Address, SaleDate, SalePrice, LegalReference order by UniqueID) as row_num
	from nashville_housing_staging
)
#delete from nashville_housing_staging
#where UniqueID in (select UniqueID from Duplicate_cte where row_num > 1);
select * from Duplicate_cte where row_num > 1 ;


rollback;
commit;


-- delete unused columns

start transaction;
alter table nashville_housing_staging
drop  PropertyAddress, 
drop OwnerAddress, 
drop TaxDistrict;


-- rearranging columns
select * from nashville_housing_staging;

alter table nashville_housing_staging
modify column Property_Address varchar(255) after ParcelID,
modify column Property_City varchar(255) after Property_Address,
modify column Owner_Address varchar(255) after OwnerName,
modify column Owner_City varchar(255) after Owner_Address,
modify column Owner_State varchar(255) after Owner_City;

