-- GROUP PROJECT; RIHAB ATWI - ZAHRAA DOKMAK

-- CREATE SCHEMA:

CREATE SCHEMA reporting_schema;


-- CREATE DAILY REPORT TABLE:

CREATE TABLE reporting_schema.zahraa_rihab_agg_daily(
	daily_date DATE, 
	distinct_customer INTEGER, 
	total_rentals INTEGER, 
	avg_rental_duration NUMERIC, 
	late_returns INTEGER, 
	late_return_percentage NUMERIC, 
	current_day INTEGER, 
	previous_day INTEGER, 
	rental_difference INTEGER
);
INSERT INTO reporting_schema.zahraa_rihab_agg_daily (daily_date, distinct_customer, total_rentals, avg_rental_duration, 
													 late_returns, late_return_percentage, current_day, previous_day, 
													 rental_difference)												

WITH CTE_DAILY_RENTAL_STATS AS
(
    SELECT 
        CAST(rental_date AS DATE) AS daily_date,
        COUNT(DISTINCT se_rental.customer_id) AS distinct_customer,
        COUNT(se_rental.rental_id) AS total_rentals, 
        ROUND(AVG(EXTRACT(DAY FROM return_date - rental_date) * 24 + EXTRACT(HOUR FROM return_date - rental_date)), 2) AS avg_rental_duration,
        COUNT(
            CASE 
                WHEN 
					(EXTRACT(DAY FROM return_date - rental_date) * 24 + EXTRACT(HOUR FROM return_date - rental_date)) > 120 THEN 1
                ELSE 
					NULL
            END
        ) AS late_returns,
        (COUNT(
            CASE 
                WHEN 
				(EXTRACT(DAY FROM return_date - rental_date) * 24 + EXTRACT(HOUR FROM return_date - rental_date)) > 120 THEN 1
                ELSE 
					NULL
            END
        ) * 100.0 / COUNT(se_rental.rental_id)) AS late_return_percentage
    FROM rental AS se_rental
    INNER JOIN payment AS se_payment
        ON se_rental.customer_id = se_payment.customer_id
    GROUP BY 
		CAST(rental_date AS DATE)
    ORDER BY 
		CAST(rental_date AS DATE)
),
CTE_DAILY_RENTAL_COUNT AS(
    SELECT
        CAST(rental_date AS DATE) AS rental_day,
        COUNT(rental_id) AS total_rentals
    FROM rental
    GROUP BY 
		CAST(rental_date AS DATE)
    ORDER BY 
		CAST(rental_date AS DATE)
),
CTE_CURRENT_PREVIOUS AS(
    SELECT
        rental_day,
        total_rentals AS current_day,
        LAG(total_rentals, 1, 0) OVER (ORDER BY rental_day) AS previous_day,
        COALESCE(total_rentals - LAG(total_rentals, 1) OVER (ORDER BY rental_day), 0) AS rental_difference
    FROM CTE_DAILY_RENTAL_COUNT
)
SELECT
    CTE_DAILY_RENTAL_STATS.daily_date,
    CTE_DAILY_RENTAL_STATS.distinct_customer,
    CTE_DAILY_RENTAL_STATS.total_rentals,
    CTE_DAILY_RENTAL_STATS.avg_rental_duration,
    CTE_DAILY_RENTAL_STATS.late_returns,
    CTE_DAILY_RENTAL_STATS.late_return_percentage,
    current_previous.current_day,
    current_previous.previous_day,
    current_previous.rental_difference
FROM CTE_DAILY_RENTAL_STATS
JOIN CTE_CURRENT_PREVIOUS current_previous 
ON 
	CTE_DAILY_RENTAL_STATS.daily_date = current_previous.rental_day;



-- MONTHLY REPORT TABLE:

CREATE TABLE reporting_schema.zahraa_rihab_agg_monthly(
	rental_month VARCHAR, 
	total_rentals INTEGER, 
	total_revenue NUMERIC, 
	active_customers INTEGER, 
	avg_rented_per_customer NUMERIC, 
	staff_name VARCHAR, 
	staff_rental_count INTEGER, 
	top_category_1 VARCHAR,
	top_category_2 VARCHAR,
	top_category_3 VARCHAR
);

INSERT INTO reporting_schema.zahraa_rihab_agg_monthly(rental_month, total_rentals, total_revenue, active_customers, 
													  avg_rented_per_customer, staff_name, staff_rental_count, 
													  top_category_1, top_category_2, top_category_3)

WITH STAFF_PERFORMANCE AS (
    SELECT
        se_staff.staff_id,
        CONCAT(se_staff.first_name, ' ', se_staff.last_name) AS staff_name,
    	TO_CHAR(se_rental.rental_date, 'YYYY-MM') AS rental_month,
        COUNT(se_rental.rental_id) AS staff_rental_count,
        ROW_NUMBER() OVER(PARTITION BY TO_CHAR(se_rental.rental_date, 'YYYY-MM') ORDER BY COUNT(se_rental.rental_id) DESC) AS rentalnb
    FROM public.staff as se_staff
    LEFT JOIN public.rental as se_rental 
        ON se_staff.staff_id = se_rental.staff_id
    GROUP BY 
        se_staff.staff_id, 
        staff_name, 
        rental_month
),

RENTAL_PER_CUSTOMER AS (
    SELECT
        se_customer.customer_id, 
	    TO_CHAR(se_rental.rental_date, 'YYYY-MM') AS rental_month,
		COUNT(se_rental.rental_id) as rented_per_customer
    FROM public.customer as se_customer
    LEFT JOIN public.rental as se_rental
        ON se_rental.customer_id = se_customer.customer_id
    GROUP BY
        se_customer.customer_id, 
        rental_month
),

CTE_CATEGORY_RANK AS (
    SELECT 
        TO_CHAR(se_rental.rental_date, 'YYYY-MM') AS rental_month,
        se_category.name AS category_name,
        se_category.category_id,
        COUNT(se_rental.rental_id) AS total_rentals,
        ROW_NUMBER() OVER(PARTITION BY TO_CHAR(se_rental.rental_date, 'YYYY-MM') ORDER BY COUNT(se_rental.rental_id) DESC) AS category_rank
    FROM public.rental AS se_rental
    INNER JOIN public.inventory AS se_inventory
    ON se_inventory.inventory_id = se_rental.inventory_id
    INNER JOIN public.film_category AS se_film_category
    ON se_film_category.film_id = se_inventory.film_id
    INNER JOIN public.category AS se_category
    ON se_category.category_id = se_film_category.category_id
    GROUP BY
        se_category.category_id,
        category_name,
        rental_month
), 
CTE_TOP3_CATEGORIES AS (
    SELECT
        CTE_CATEGORY_RANK.rental_month, 
        MAX(CASE WHEN category_rank = 1 THEN category_name END) AS top_category_1,
        MAX(CASE WHEN category_rank = 2 THEN category_name END) AS top_category_2,
        MAX(CASE WHEN category_rank = 3 THEN category_name END) AS top_category_3
    FROM CTE_CATEGORY_RANK
    GROUP BY 
        rental_month
    ORDER BY
        rental_month
)

SELECT
    RENTAL_PER_CUSTOMER.rental_month,
    RENTAL_PER_CUSTOMER.total_rentals,
    RENTAL_PER_CUSTOMER.total_revenue,
    RENTAL_PER_CUSTOMER.active_customers,
    RENTAL_PER_CUSTOMER.avg_rented_per_customer, 
    STAFF_PERFORMANCE.staff_name,
    STAFF_PERFORMANCE.staff_rental_count,
    CTE_TOP3_CATEGORIES.top_category_1,
    CTE_TOP3_CATEGORIES.top_category_2,
    CTE_TOP3_CATEGORIES.top_category_3
FROM (
    SELECT
        RENTAL_PER_CUSTOMER.rental_month AS rental_month,
        COUNT(se_rental.rental_id) AS total_rentals,
        SUM(se_payment.amount) AS total_revenue,
        COUNT(DISTINCT se_customer.customer_id) AS active_customers,
        AVG(rented_per_customer) AS avg_rented_per_customer
    FROM
        public.rental AS se_rental
    LEFT JOIN public.payment AS se_payment
        ON se_rental.rental_id = se_payment.rental_id
    LEFT JOIN public.customer AS se_customer
        ON se_rental.customer_id = se_customer.customer_id
    LEFT JOIN RENTAL_PER_CUSTOMER
        ON RENTAL_PER_CUSTOMER.customer_id = se_customer.customer_id
    GROUP BY
        RENTAL_PER_CUSTOMER.rental_month
) AS RENTAL_PER_CUSTOMER
LEFT JOIN STAFF_PERFORMANCE AS STAFF_PERFORMANCE
    ON RENTAL_PER_CUSTOMER.rental_month = STAFF_PERFORMANCE.rental_month
LEFT JOIN CTE_TOP3_CATEGORIES
    ON RENTAL_PER_CUSTOMER.rental_month = CTE_TOP3_CATEGORIES.rental_month
ORDER BY
    RENTAL_PER_CUSTOMER.rental_month;



-- YEARLY REPORT TABLE:

CREATE TABLE reporting_schema.zahraa_rihab_agg_yearly(
	payment_year INTEGER, 
	rental_year INTEGER,
	total_rentals INTEGER,
	active_customers INTEGER, 
	total_revenue NUMERIC, 
	loyal_customer_id INTEGER, 
	most_rented_rating VARCHAR
);

INSERT INTO reporting_schema.zahraa_rihab_agg_yearly(payment_year, rental_year,total_rentals, active_customers, 
													 total_revenue, loyal_customer_id, most_rented_rating)
WITH RENTAL_TOTALS AS (
    SELECT
        EXTRACT(YEAR FROM se_rental.rental_date) AS rental_year,
        se_rental.customer_id,
        COUNT(se_rental.rental_id) AS total_rentals,
        RANK() OVER (PARTITION BY EXTRACT(YEAR FROM se_rental.rental_date) 
					 ORDER BY COUNT(se_rental.rental_id) DESC) AS rank_per_year
    FROM public.rental AS se_rental
    GROUP BY 
		rental_year, 
		se_rental.customer_id
),
REVENUE_TOTAL_CUSTOMERS AS (
    SELECT
        EXTRACT(YEAR FROM se_rental.rental_date) AS rental_year,
        EXTRACT(YEAR FROM se_payment.payment_date) AS payment_year,
        COUNT(se_rental.rental_id) AS total_rentals,
        COUNT(DISTINCT se_rental.customer_id) AS active_customers,
        SUM(se_payment.amount) AS total_revenue
    FROM public.rental AS se_rental
    INNER JOIN public.payment AS se_payment
        ON se_payment.rental_id = se_rental.rental_id
    GROUP BY 
		rental_year, 
		payment_year
),
Yearly_Rental_Rank AS (
    SELECT
        EXTRACT(YEAR FROM se_rental.rental_date) AS year,
        se_fim.rating,
        COUNT(*) AS rental_count,
        RANK() OVER (PARTITION BY EXTRACT(YEAR FROM se_rental.rental_date) 
					 ORDER BY COUNT(*) DESC) AS rank
    FROM
        public.rental AS se_rental
    INNER JOIN
        public.inventory AS se_inventory 
		ON se_rental.inventory_id = se_inventory.inventory_id
    INNER JOIN
        public.film AS se_fim 
		ON se_inventory.film_id = se_fim.film_id
    GROUP BY
        year, 
		se_fim.rating
)
SELECT
    REVENUE_TOTAL_CUSTOMERS.payment_year,
    REVENUE_TOTAL_CUSTOMERS.rental_year,
    REVENUE_TOTAL_CUSTOMERS.total_rentals,
    REVENUE_TOTAL_CUSTOMERS.active_customers AS active_customers,
    REVENUE_TOTAL_CUSTOMERS.total_revenue AS total_revenue,
    RENTAL_TOTALS.customer_id AS loyal_customer_id,
    Yearly_Rental_Rank.rating AS most_rented_rating
FROM REVENUE_TOTAL_CUSTOMERS
JOIN RENTAL_TOTALS
    ON REVENUE_TOTAL_CUSTOMERS.rental_year = RENTAL_TOTALS.rental_year
    AND RENTAL_TOTALS.rank_per_year = 1
JOIN Yearly_Rental_Rank
    ON REVENUE_TOTAL_CUSTOMERS.rental_year = Yearly_Rental_Rank.year
    AND Yearly_Rental_Rank.rank = 1
ORDER BY 
	REVENUE_TOTAL_CUSTOMERS.rental_year, 
	REVENUE_TOTAL_CUSTOMERS.payment_year;