
/* -------------------------Report----------------------------------
	Broadband Connectivity of Households by Monthly Expenditure Decile
---------------------------------------------------------------------*/

-- Checking household ids in the datasets are unique or not
SELECT
	COUNT(*) AS total_rows,
	COUNT(DISTINCT hhid) AS distinct_id
FROM household_data;

-- required columns
SELECT
	hhid,
	usual_monthly_consumer_expenditure,
	final_weight,
	whether_the_household_has_access_to_broadband_internet_facility_within_the_house
FROM household_data;

-- by expenditure decile
WITH expenditure_group AS -- step 1
(SELECT
        usual_monthly_consumer_expenditure,
        SUM(final_weight) AS weight_at_level
    FROM dbo.household_data
    GROUP BY usual_monthly_consumer_expenditure)
,ranked_level AS -- step 2
	(SELECT
		*,
		SUM(weight_at_level) OVER(
		ORDER BY usual_monthly_consumer_expenditure)*1.0/SUM(weight_at_level) OVER() AS cumulative_share
	FROM expenditure_group) 
,expenditure_decile_map AS -- step 3
	(SELECT *,
        CASE 
            WHEN cumulative_share <= 0.1 THEN 1
            WHEN cumulative_share <= 0.2 THEN 2
            WHEN cumulative_share <= 0.3 THEN 3
            WHEN cumulative_share <= 0.4 THEN 4
            WHEN cumulative_share <= 0.5 THEN 5
            WHEN cumulative_share <= 0.6 THEN 6
            WHEN cumulative_share <= 0.7 THEN 7
            WHEN cumulative_share <= 0.8 THEN 8
            WHEN cumulative_share <= 0.9 THEN 9
            ELSE 10
        END AS decile
    FROM ranked_level) 
, decile_data AS -- step 4
	(SELECT
		h.usual_monthly_consumer_expenditure,
		h.final_weight,
		h.whether_the_household_has_access_to_broadband_internet_facility_within_the_house AS broadband_access,
		e.decile
	FROM household_data h
	JOIN expenditure_decile_map e
	ON h.usual_monthly_consumer_expenditure = e.usual_monthly_consumer_expenditure) 
, decile_with_broadband AS -- step 5
	(SELECT
		decile,
		SUM(final_weight*usual_monthly_consumer_expenditure)/SUM(final_weight) AS avg_monthly_exp,
		SUM(final_weight) AS total_households,
		broadband_access
	from decile_data
	GROUP BY decile, broadband_access) 

SELECT -- Final
	*,
	SUM(total_households) OVER(PARTITION BY decile) AS households_by_decile,
	ROUND((total_households/SUM(total_households) OVER(PARTITION BY decile))*100,1) AS percentage
FROM decile_with_broadband
ORDER BY decile;

