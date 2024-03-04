-- public.hourly_salary definition

-- DROP TABLE IF EXISTS public.hourly_salary;

-- CREATE TABLE IF NOT EXISTS public.hourly_salary (
-- 	"year" int4 NULL,
-- 	"month" int4 NULL,
-- 	branch_id int4 NULL,
-- 	salary_per_hour int4 null,
-- 	job_date date null,
-- 	udate timestamptz default now() null,
-- 	CONSTRAINT pk_segment UNIQUE ("year","month",branch_id,job_date)
-- );

TRUNCATE TABLE hourly_salary;
WITH unique_employees AS(
	SELECT
		employee_id,
		branch_id,
		salary
	FROM(
		SELECT
			employe_id AS employee_id,
			branch_id,
			salary,
			RANK() OVER(PARTITION BY employe_id ORDER BY salary DESC) AS rnk
		from employees e
	) a
	WHERE rnk = 1
), employee_salary AS(
	SELECT
		extract(YEAR FROM CAST(t."date" AS date)) AS "year",
		extract(MONTH FROM CAST(t."date" AS date)) AS "month",
		e.branch_id,
		e.employee_id,
		e.salary
	FROM unique_employees e
	JOIN timesheets t ON t.employee_id = e.employee_id
	WHERE t.checkin != '' AND t.checkout != '' AND t.checkin != t.checkout AND t.checkin < t.checkout
	GROUP BY 1,2,3,4,5
), branch_total_salary_each_month AS(
	SELECT
		branch_id,
        "year",
        "month",
        SUM(salary) total_salary
	FROM employee_salary
	GROUP BY 1,2,3
), employee_working_hour AS(
	SELECT
		extract(YEAR FROM CAST(t."date" AS date)) AS "year",
		extract(MONTH FROM CAST(t."date" AS date)) AS "month",
		e.branch_id,
		e.employee_id,
		extract(hour from t.checkout::TIME-t.checkin::TIME) AS working_hour
	FROM timesheets t
	JOIN unique_employees e ON e.employee_id = t.employee_id
	WHERE t.checkin != '' AND t.checkout != '' AND t.checkin != t.checkout AND t.checkin < t.checkout
), branch_total_working_hour_each_month AS(
	SELECT
		branch_id,
		"year",
		"month",
		SUM(working_hour) total_hour
	FROM employee_working_hour
	GROUP BY 1,2,3
), branch_salary_per_hour_each_month AS(
	SELECT
		bts."year",
		bts."month",
		bts.branch_id,
		CAST((bts.total_salary/btw.total_hour) AS INT) AS salary_per_hour
	FROM branch_total_salary_each_month bts
	JOIN branch_total_working_hour_each_month btw ON bts.branch_id = btw.branch_id AND bts."year" = btw."year" AND bts."month" = btw."month"
)
INSERT INTO hourly_salary
SELECT
	*
FROM branch_salary_per_hour_each_month;