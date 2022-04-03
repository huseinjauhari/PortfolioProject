use PortfolioProject;
create table NashvilleHousing
(
UniqueID int,
ParcelID varchar(255),
LandUse varchar(255),
PropertyAddress varchar(255),
SaleDate datetime,
SalePrice int,
LegalReference varchar(255),
SoldAsVacant varchar(255),
OwnerName varchar(255),
OwnerAddress varchar(255),
Acreage float,
TaxDistrict varchar(255),
LandValue int,
BuildingValue int,
TotalValue int,
YearBuilt int,
Bedrooms int,
FullBath int,
HalfBath int,
SaleDateConverted date
);
/*
load data local infile '/Users/huseinjauhari/Downloads/NashvilleHousing.csv'
into table NashvilleHousing
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;
*/

-----------------------------------------------------------------------------------------------------------
-- Standardize date format
alter table NashvilleHousing
add SaleDateConverted date;

update NashvilleHousing
set SaleDateConverted = convert(SaleDate,date);

-----------------------------------------------------------------------------------------------------------
-- Populate property addres
select *
from NashvilleHousing
where PropertyAddress is null
order by ParcelID;

select n.ParcelID, n.PropertyAddress, h.ParcelID, h.PropertyAddress , isnull(n.PropertyAddress,h.PropertyAddress)
from NashvilleHousing n
join NashvilleHousing h
on n.ParcelID=h.ParcelID and n.UniqueID!=h.UniqueID
where n.PropertyAddress is null;

update n
set PropertyAddress = isnull(n.PropertyAddress,h.PropertyAddress)
from NashvilleHousing n
join NashvilleHousing h
on n.ParcelID=h.ParcelID and n.UniqueID!=h.UniqueID
where n.PropertyAddress is null;

-----------------------------------------------------------------------------------------------------------
-- Breaking out address into individual columns (address, city, state)
select PropertyAddress
from NashvilleHousing;

select 
substring(PropertyAddress, 1, locate(',', PropertyAddress)-1) as address1,
substring(PropertyAddress, locate(',', PropertyAddress)+1, length(PropertyAddress)) as address2
from NashvilleHousing;

alter table NashvilleHousing
add PropertySplitAddress nvarchar(255);

update NashvilleHousing
set PropertySplitAddress = substring(PropertyAddress, 1, locate(',', PropertyAddress)-1);

alter table NashvilleHousing
add PropertySplitCity nvarchar(255);

update NashvilleHousing
set PropertySplitCity = substring(PropertyAddress, locate(',', PropertyAddress)+1, length(PropertyAddress));

select *
from NashvilleHousing;

-- simpler way to do the above for owner address column
select OwnerAddress
from NashvilleHousing;

alter table NashvilleHousing
add OwnerSplitAddress nvarchar(255);

update NashvilleHousing
set OwnerSplitAddress = substring_index(OwnerAddress,',',1);

alter table NashvilleHousing
add OwnerSplitCity nvarchar(255);

update NashvilleHousing
set OwnerSplitCity = substring_index(substring_index(OwnerAddress,',',2),',',-1);

alter table NashvilleHousing
add OwnerSplitState nvarchar(255);

update NashvilleHousing
set OwnerSplitState = substring_index(OwnerAddress,',',-1);


-----------------------------------------------------------------------------------------------------------
-- Change Y and N to yes and no in "Sold as Vacant" field
select distinct(SoldAsVacant), count(SoldAsVacant)
from NashvilleHousing
group by SoldAsVacant
order by 2;

select SoldAsVacant, 
case when SoldAsVacant='Y' then 'Yes' 
when SoldAsVacant='N' then 'No' 
else SoldAsVacant 
end
from NashvilleHousing;

update NashvilleHousing
set SoldAsVacant = case when SoldAsVacant='Y' then 'Yes' 
when SoldAsVacant='N' then 'No' 
else SoldAsVacant 
end;


-----------------------------------------------------------------------------------------------------------
-- Removing duplicates
with RowNumCTE as(
select *, 
	row_number() over(
	partition by ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference
	order by UniqueID) row_num
from NashvilleHousing
order by ParcelID)

delete
from RowNumCTE
where row_num>1;


-----------------------------------------------------------------------------------------------------------
-- delete unused columns
alter table NashvilleHousing
drop column OwnerAddress;























