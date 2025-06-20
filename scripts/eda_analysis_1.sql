SELECT
	*
FROM household_data

/*----------------
EDA
------------------*/
-- Checking unique values of required dimentions
SELECT
	 COUNT(DISTINCT sector) AS unique_values
FROM household_data;

SELECT
	 COUNT(DISTINCT state_code) AS unique_values
FROM household_data;

SELECT
	 COUNT(DISTINCT religion) AS unique_values
FROM household_data;

SELECT
	 COUNT(DISTINCT social_group) AS unique_values
FROM household_data;

SELECT
	 COUNT(DISTINCT religion) AS unique_values
FROM household_data;

SELECT
	 COUNT(DISTINCT whether_the_household_has_access_to_broadband_internet_facility_within_the_house) AS unique_values
FROM household_data;

-- rural urban household estimation

SELECT
	sector,
	--hh,
	--SUM(hh) OVER() AS total_hh,
	ROUND((hh/SUM(hh) OVER())*100,1) AS hh_percentage
FROM
	(SELECT
		sector,
		SUM(final_weight) AS hh
	FROM household_data
	GROUP BY sector) t;

-- social group estimation

SELECT
	social_group,
	--hh,
	--SUM(hh) OVER() AS total_hh,
	ROUND((hh/SUM(hh) OVER())*100,1) AS hh_percentage
FROM
	(SELECT
		social_group,
		SUM(final_weight) AS hh
	FROM household_data
	GROUP BY social_group) t
ORDER BY hh_percentage DESC;

-- religion estimation

SELECT
	religion,
	--hh,
	--SUM(hh) OVER() AS total_hh,
	ROUND((hh/SUM(hh) OVER())*100,1) AS hh_percentage
FROM
	(SELECT
		religion,
		SUM(final_weight) AS hh
	FROM household_data
	GROUP BY religion) t
ORDER BY hh_percentage DESC;

-- broadband connectivity
SELECT
	 CASE WHEN broadband_connectivity = 1 THEN 'Yes'
		 ELSE 'No'
	END AS broadband_connectivity,
	--hh,
	--SUM(hh) OVER() AS total_hh,
	ROUND((hh/SUM(hh) OVER())*100,1) AS hh_percentage
FROM
	(SELECT
		whether_the_household_has_access_to_broadband_internet_facility_within_the_house AS broadband_connectivity,
		SUM(final_weight) AS hh
	FROM household_data
	GROUP BY whether_the_household_has_access_to_broadband_internet_facility_within_the_house) t
ORDER BY hh_percentage DESC;

-- broadband connectivity by rural/urban
SELECT
	sector,
	broadband_connectivity,
	hh,
	SUM(hh) OVER(PARTITION BY sector) AS total_hh,
	ROUND((hh/SUM(hh) OVER(PARTITION BY sector))*100,1) AS hh_percentage
FROM
(SELECT
	    sector,
		whether_the_household_has_access_to_broadband_internet_facility_within_the_house AS broadband_connectivity,
		SUM(final_weight) AS hh
	FROM household_data
	GROUP BY 
			sector,
			whether_the_household_has_access_to_broadband_internet_facility_within_the_house) t;

-- broadband connectivity by social group

WITH base AS (
	SELECT
		CASE WHEN social_group = 1 THEN 'ST'
			 WHEN social_group = 2 THEN 'SC'
			 WHEN social_group = 3 THEN 'OBC'
			 WHEN social_group = 9 THEN 'GEN'
		END AS social_group,
		CASE WHEN broadband_connectivity = 1 THEN 'Yes'
			 ELSE 'No'
		END AS broadband_connectivity,
		hh,
		SUM(hh) OVER(PARTITION BY social_group) AS total_hh,
		ROUND((hh/SUM(hh) OVER(PARTITION BY social_group))*100,1) AS hh_percentage
	FROM (
		SELECT
			social_group,
			whether_the_household_has_access_to_broadband_internet_facility_within_the_house AS broadband_connectivity,
			SUM(final_weight) AS hh
		FROM household_data
		GROUP BY 
			social_group,
			whether_the_household_has_access_to_broadband_internet_facility_within_the_house
	) t
)
SELECT 
	social_group,
	hh_percentage
FROM base
WHERE broadband_connectivity = 'Yes'
ORDER BY hh_percentage DESC;

-- broadband connectivity by religion

WITH cleaned_data AS (
	SELECT 
		CASE 
			WHEN religion = 1 THEN 'Hindu'
			WHEN religion = 2 THEN 'Muslim'
			WHEN religion = 3 THEN 'Christian'
			ELSE 'Others'
		END AS religion,
		whether_the_household_has_access_to_broadband_internet_facility_within_the_house AS broadband_connectivity,
		final_weight
	FROM household_data
),
grouped_data AS (
	SELECT
		religion,
		broadband_connectivity,
		SUM(final_weight) AS hh
	FROM cleaned_data
	GROUP BY religion, broadband_connectivity
),
final AS (
	SELECT
		religion,
		CASE WHEN broadband_connectivity = 1 THEN 'Yes' ELSE 'No' END AS broadband_connectivity,
		hh,
		SUM(hh) OVER(PARTITION BY religion) AS total_hh,
		ROUND((hh * 100.0) / SUM(hh) OVER(PARTITION BY religion), 1) AS hh_percentage
	FROM grouped_data
)
SELECT religion, hh_percentage
FROM final
WHERE broadband_connectivity = 'Yes'
ORDER BY hh_percentage DESC;

-- State wise broadband connectivity
	WITH base_state AS
	(SELECT
		state_code,
		broadband_connectivity,
		hh,
		SUM(hh) OVER(PARTITION BY state_code) AS total_hh,
		ROUND((hh/SUM(hh) OVER(PARTITION BY state_code))*100,1) AS hh_percentage
	FROM
	(SELECT
			state_code,
			whether_the_household_has_access_to_broadband_internet_facility_within_the_house AS broadband_connectivity,
			SUM(final_weight) AS hh
		FROM household_data
		GROUP BY 
				state_code,
				whether_the_household_has_access_to_broadband_internet_facility_within_the_house) t
	)
	, state_rank AS (SELECT	
		state_code,
		hh_percentage,
		ROW_NUMBER() OVER(ORDER BY hh_percentage DESC) AS state_rank
	FROM base_state
	WHERE broadband_connectivity = 1) 
SELECT
	state_code,
	hh_percentage
FROM state_rank
WHERE (hh_percentage >= 90) OR (hh_percentage < 70)
ORDER BY hh_percentage DESC;

/*......................................................
	state level data summary
........................................................
*/

CREATE VIEW vw_state_level_hh_digital_summary AS
WITH state_level_summary AS
	(SELECT
		state_code,
		sector,
		religion,
		social_group,
		whether_the_household_has_access_to_broadband_internet_facility_within_the_house,
		[number_of_items_possessed_:_desktop_pc],
		[number_of_items_possessed_:_laptop],
		[number_of_items_possessed_:_telephone_including_landline],
		[number_of_items_possessed_:_mobile_phone_including_smart_phone],
		[number_of_items_possessed_:_tablet_palmtop],
		[number_of_items_possessed_:_television],
		ROUND(SUM(final_weight),0) AS house_hold
	FROM dbo.household_data
	GROUP BY state_code,
		sector,
		religion,
		social_group,
		whether_the_household_has_access_to_broadband_internet_facility_within_the_house,
		[number_of_items_possessed_:_desktop_pc],
		[number_of_items_possessed_:_laptop],
		[number_of_items_possessed_:_telephone_including_landline],
		[number_of_items_possessed_:_mobile_phone_including_smart_phone],
		[number_of_items_possessed_:_tablet_palmtop],
		[number_of_items_possessed_:_television])
	, state_level_summary_code AS
	(SELECT
		state_code,
		sector,
		whether_the_household_has_access_to_broadband_internet_facility_within_the_house,
		social_group,
		religion,
		[number_of_items_possessed_:_desktop_pc],
			[number_of_items_possessed_:_laptop],
			[number_of_items_possessed_:_telephone_including_landline],
			[number_of_items_possessed_:_mobile_phone_including_smart_phone],
			[number_of_items_possessed_:_tablet_palmtop],
			[number_of_items_possessed_:_television],
		SUM(house_hold) AS house_hold_number
	FROM state_level_summary
	GROUP BY state_code,sector,social_group,religion,
			 whether_the_household_has_access_to_broadband_internet_facility_within_the_house,
			 [number_of_items_possessed_:_desktop_pc],
			[number_of_items_possessed_:_laptop],
			[number_of_items_possessed_:_telephone_including_landline],
			[number_of_items_possessed_:_mobile_phone_including_smart_phone],
			[number_of_items_possessed_:_tablet_palmtop],
			[number_of_items_possessed_:_television])
SELECT
	state_code,
	CASE
		WHEN sector = 1 THEN 'rural'
		WHEN sector = 2 THEN 'urban'
	END AS rural_urban,
	CASE
		WHEN social_group = 1 THEN 'ST'
		WHEN social_group = 2 THEN 'SC'
		WHEN social_group = 3 THEN 'OBC'
		WHEN social_group = 9 THEN 'GN'
	END AS social_group,
	CASE
		WHEN religion = 1 THEN 'Hindu'
		WHEN religion = 2 THEN 'Muslim'
		WHEN religion = 3 THEN 'Chrishtian'
		ELSE 'Others'
	END AS religion,
	CASE
		WHEN whether_the_household_has_access_to_broadband_internet_facility_within_the_house = 1 THEN 'yes'
		WHEN whether_the_household_has_access_to_broadband_internet_facility_within_the_house = 2 THEN 'no'
	END AS household_broadband_connectivity_within_house_premises,
	[number_of_items_possessed_:_desktop_pc] AS number_of_desktop,
	[number_of_items_possessed_:_laptop] AS number_of_laptop,
	[number_of_items_possessed_:_mobile_phone_including_smart_phone] AS number_of_mob_including_smart_phone,
	[number_of_items_possessed_:_tablet_palmtop] AS number_of_tablet_palmtop,
	[number_of_items_possessed_:_television] AS number_of_tv,
	house_hold_number
FROM state_level_summary_code;

-- Checking the view
SELECT
	*
FROM vw_state_level_hh_digital_summary;




