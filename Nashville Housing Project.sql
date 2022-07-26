SELECT * FROM dbo.NashvilleHousing
---------------------------------------------------------------------------
--Standardize Date Format 

SELECT SaleDate, CONVERT(Date, SaleDate)
FROM dbo.NashvilleHousing

ALTER TABLE  NashvilleHousing
ADD SalesDate Date;

UPDATE NashvilleHousing
SET SalesDate = CONVERT(Date, SaleDate)

--Dropping Old date Column
ALTER TABLE NashvilleHousing 
DROP COLUMN SaleDate  

---------------------------------------------------------------------------
--Populate Property Address Data To fill in Null Values 

SELECT a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress, ISNULL(a.PropertyAddress,b.PropertyAddress)
FROM NashvilleHousing a
JOIN NashvilleHousing b
ON a.ParcelID = b.ParcelID
AND a.[UniqueID ] <> b.[UniqueID ]
WHERE a.PropertyAddress IS NULL

UPDATE a
SET PropertyAddress = ISNULL(a.PropertyAddress,b.PropertyAddress)
FROM NashvilleHousing a
JOIN NashvilleHousing b
ON a.ParcelID = b.ParcelID
AND a.[UniqueID ] <> b.[UniqueID ]
WHERE a.PropertyAddress IS NULL

---------------------------------------------------------------------------
--Breaking out 'PropertyAddress' Into Individual Columns(Address, City)

SELECT PropertyAddress
FROM NashvilleHousing

SELECT
SUBSTRING(PropertyAddress,1, CHARINDEX(',', propertyAddress)-1) AS Address 
, SUBSTRING(PropertyAddress, CHARINDEX(',', propertyAddress)+1, LEN(propertyAddress)) AS Address 
FROM NashvilleHousing

ALTER TABLE NashvilleHousing
ADD PropertyNAddress Nvarchar(255);

UPDATE NashvilleHousing
SET PropertyNAddress = SUBSTRING(PropertyAddress,1, CHARINDEX(',', propertyAddress)-1)

ALTER TABLE NashvilleHousing
ADD PropertyCity Nvarchar(255);

UPDATE NashvilleHousing
SET PropertyCity = SUBSTRING(PropertyAddress, CHARINDEX(',', propertyAddress)+1, LEN(propertyAddress))

---------------------------------------------------------------------------
--Breaking out 'OwnerAddress' Into Individual Columns(Address, City, State)

SELECT OwnerAddress FROM NashvilleHousing

SELECT PARSENAME(REPLACE(OwnerAddress, ',', '.'), 3)
,PARSENAME(REPLACE(OwnerAddress, ',', '.'), 2)
,PARSENAME(REPLACE(OwnerAddress, ',', '.'), 1)
FROM NashvilleHousing

ALTER TABLE NashvilleHousing
ADD OwnerNaddress Nvarchar(255);

UPDATE NashvilleHousing
SET OwnerNaddress = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 3)

-----------------
ALTER TABLE NashvilleHousing
ADD OwnerCity Nvarchar(255);

UPDATE NashvilleHousing
SET OwnerCity = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 2)

-----------------
ALTER TABLE NashvilleHousing
ADD OwnerState Nvarchar(255);

UPDATE NashvilleHousing
SET OwnerState = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 1)

-------------------------------------------------------------------
--Replacing Y and N in column 'SoldAsVacant' to Yes and No
SELECT DISTINCT(SoldAsVacant), COUNT(SoldAsVacant)
FROM NashvilleHousing 
GROUP BY SoldAsVacant
ORDER BY 2

SELECT SoldAsVacant
, CASE WHEN SoldAsVacant = 'Y'  THEN 'Yes'
	   WHEN SoldAsVacant = 'N' THEN 'No'
	   ELSE SoldAsVacant
	   END 
FROM NashvilleHousing 

UPDATE NashvilleHousing
SET SoldAsVacant = CASE WHEN SoldAsVacant = 'Y'  THEN 'Yes'
	   WHEN SoldAsVacant = 'N' THEN 'No'
	   ELSE SoldAsVacant
	   END 
FROM NashvilleHousing 

--------------------------------------------------------------------------
--Remove Duplicate columns

SELECT * FROM NashvilleHousing

ALTER TABLE NashvilleHousing
DROP COLUMN OwnerAddress, TaxDistrict, PropertyAddress, SalesDate
