CREATE DATABASE IF NOT EXISTS data_cleaning_project;
USE data_cleaning_project;

-----------------------------------------------------------------------------------------------------------------

DROP TABLE Nashville_Housing;

CREATE TABLE Nashville_Housing
(
   UniqueID INT,
   ParcelID VARCHAR(100) NULL,
   LandUse VARCHAR(100) NULL,
   PropertyAddress VARCHAR(500) NULL,
   SaleDate VARCHAR(100) NULL,
   SalePrice VARCHAR(20) NULL,
   LegalReference VARCHAR(100) NULL,
   SoldAsVacant VARCHAR(10) NULL,
   OwnerName VARCHAR(100) NULL,
   OwnerAddress VARCHAR(100) NULL,
   Acreage VARCHAR(20) NULL,
   TaxDistrict VARCHAR(100) NULL,
   LandValue VARCHAR(20) NULL,
   BuildingValue VARCHAR(20) NULL,
   TotalValue VARCHAR(20) NULL,
   YearBuilt VARCHAR(20) NULL,
   Bedrooms VARCHAR(20) NULL,
   FullBath VARCHAR(20) NULL,
   HalfBath VARCHAR(2) NULL
);
   

-----------------------------------------------------------------------------------------------------------------------------------


SHOW VARIABLES LIKE "secure_file_priv";

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Nashville Housing Data for Data Cleaning.csv'
INTO TABLE Nashville_Housing
CHARACTER SET 'utf8mb4'
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS
SET LandUse = NULLIF(LandUse, ' '),
    PropertyAddress = NULLIF(PropertyAddress, ' '),
    OwnerName = NULLIF(OwnerName, ' '),
    OwnerAddress = NULLIF(OwnerAddress, ' '),
    Acreage = NULLIF(Acreage, ' '),
    TaxDistrict = NULLIF(TaxDistrict, ' '),
    LandValue = NULLIF(LandValue, ' '),
    BuildingValue = NULLIF(BuildingValue, ' '),
    TotalValue = NULLIF(TotalValue, ' '),
    YearBuilt = NULLIF(YearBuilt, ' '),
    Bedrooms = NULLIF(Bedrooms, ' '),
    FullBath = NULLIF(FullBath, ' '),
    HalfBath = NULLIF(HalfBath, ' ');


SELECT * FROM nashville_housing;

DESCRIBE nashville_housing;


ALTER TABLE nashville_housing MODIFY SalePrice INT NULL;
ALTER TABLE nashville_housing MODIFY Acreage DOUBLE NULL;
ALTER TABLE nashville_housing MODIFY LandValue INT NULL;
ALTER TABLE nashville_housing MODIFY BuildingValue INT NULL;
ALTER TABLE nashville_housing MODIFY YearBuilt INT NULL;
ALTER TABLE nashville_housing MODIFY TotalValue INT NULL;
ALTER TABLE nashville_housing MODIFY Bedrooms INT NULL;
ALTER TABLE nashville_housing MODIFY FullBath INT NULL;

------------------------------------------------------------------------------------------------------------------------------------

-- DATA CLEANING

SELECT * FROM nashville_housing;

------------------------------------------------------------------------------------------------------------------------------------

-- Standardized Date Format:

SELECT SaleDate, STR_TO_DATE(SaleDate, '%M %e, %Y') FROM nashville_housing;

ALTER TABLE Nashville_Housing
ADD COLUMN SaleDateConverted DATE;

UPDATE Nashville_Housing
SET SaleDateConverted = STR_TO_DATE(SaleDate, '%M %e, %Y');


---------------------------------------------------------------------------------------------------------------------------------------


-- Populate Property Address Data for NULL Values

SELECT * FROM Nashville_Housing
ORDER BY ParcelID;

SELECT a.UniqueID, a.ParcelID, a.PropertyAddress, b.UniqueID, b.ParcelID, b.PropertyAddress 
FROM Nashville_Housing a
JOIN Nashville_Housing b ON a.ParcelID = b.ParcelID AND a.UniqueID <> b.UniqueID
WHERE a.PropertyAddress IS NULL;


SELECT a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress, COALESCE(a.PropertyAddress, b.PropertyAddress)
FROM Nashville_Housing a
JOIN Nashville_Housing b ON a.ParcelID = b.ParcelID AND a.UniqueID <> b.UniqueID
WHERE a.PropertyAddress IS NULL;


UPDATE Nashville_Housing a
JOIN Nashville_Housing b ON a.ParcelID = b.ParcelID AND a.UniqueID <> b.UniqueID
SET a.PropertyAddress = COALESCE(a.PropertyAddress, b.PropertyAddress)
WHERE a.PropertyAddress IS NULL;

SELECT * FROM Nashville_Housing
WHERE PropertyAddress IS NULL;


--------------------------------------------------------------------------------------------------------------------------------


-- Breaking out Property & Owner Addresses into Individual Columns (Address, City, State)
-- Beaking out Property Address

SELECT PropertyAddress FROM Nashville_Housing;

SELECT 
  SUBSTRING_INDEX(PropertyAddress, ',', 1) AS Address,
  TRIM(SUBSTRING_INDEX(PropertyAddress, ',', -1)) AS City
FROM Nashville_Housing;

ALTER TABLE Nashville_Housing
ADD COLUMN PropertySplitAddress VARCHAR(50);

UPDATE Nashville_Housing
SET PropertySplitAddress = SUBSTRING_INDEX(PropertyAddress, ',', 1);

ALTER TABLE Nashville_Housing
ADD COLUMN PropertySplitCity VARCHAR(50);

UPDATE Nashville_Housing
SET PropertySplitCity = TRIM(SUBSTRING_INDEX(PropertyAddress, ',', -1));

SELECT * FROM Nashville_Housing;


--------------------------------------------------------------------------------------------------------------------------------


-- Breaking out Owner Address

SELECT OwnerAddress FROM Nashville_Housing;

SELECT
SUBSTRING_INDEX(OwnerAddress, ',', 1) AS Address,
TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', -2), ',', 1)) AS City,
TRIM(SUBSTRING_INDEX(OwnerAddress, ',',-1)) AS State
FROM Nashville_Housing;

ALTER TABLE Nashville_Housing
ADD COLUMN OwnerSplitAddress VARCHAR(50);

UPDATE Nashville_Housing
SET OwnerSplitAddress = SUBSTRING_INDEX(OwnerAddress, ',', 1);

ALTER TABLE Nashville_Housing
ADD COLUMN OwnerSplitCity VARCHAR(30);

UPDATE Nashville_Housing
SET OwnerSplitCity = TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', -2), ',', 1));

ALTER TABLE Nashville_Housing
ADD COLUMN OwnerSplitState VARCHAR(10);

UPDATE Nashville_Housing
SET OwnerSplitState = TRIM(SUBSTRING_INDEX(OwnerAddress, ',',-1));

SELECT * FROM Nashville_Housing;


--------------------------------------------------------------------------------------------------------------------------------


-- Change Y and N to Yes and No in "Sold as Vacant" field

SELECT DISTINCT(SoldAsVacant), COUNT(SoldAsVacant) FROM Nashville_Housing
GROUP BY SoldAsVacant;


SELECT SoldAsVacant,
CASE WHEN SoldAsVacant = "N" THEN "No"
     WHEN SoldAsVacant = "Y" THEN "Yes"
     ELSE SoldAsVacant
END AS SoldAsVacant
FROM Nashville_Housing;

UPDATE Nashville_Housing
SET SoldAsVacant = 
CASE WHEN SoldAsVacant = "N" THEN "No"
	 WHEN SoldAsVacant = "Y" THEN "Yes"
	 ELSE SoldAsVacant
END;


-------------------------------------------------------------------------------------------------------------------------------


-- Removing Duplicates

SELECT * FROM Nashville_Housing;

WITH Row_Num_CTE AS
(
SELECT *, ROW_NUMBER() OVER 
(PARTITION BY ParcelID,
              PropertyAddress,
              SaleDate,
              SalePrice,
              LegalReference,
              OwnerName
ORDER BY UniqueID) AS Row_num
FROM Nashville_Housing
)
SELECT * FROM Row_Num_CTE
WHERE Row_num > 1
ORDER BY PropertyAddress;


--------------------------------------------------------------------------------------------------------------------------------------


-- To deleting the duplicates

DELETE FROM Nashville_Housing
WHERE UniqueID IN (
    SELECT UniqueID
    FROM (
        SELECT UniqueID,
            ROW_NUMBER() OVER (
                PARTITION BY ParcelID, PropertyAddress, SaleDate, SalePrice, LegalReference, OwnerName
                ORDER BY UniqueID
            ) AS Row_num
        FROM Nashville_Housing
    ) AS subquery
    WHERE Row_num > 1
);
    
    
-----------------------------------------------------------------------------------------------------------------------------------------

    
 -- Delete Unused Columns
 
 SELECT * FROM Nashville_Housing;
 
 ALTER TABLE Nashville_Housing
 DROP COLUMN PropertyAddress, 
 DROP COLUMN OwnerAddress, 
 DROP COLUMN SaleDate,
 DROP COLUMN TaxDistrict;


