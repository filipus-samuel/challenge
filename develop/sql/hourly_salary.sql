CREATE TABLE IF NOT EXISTS public.hourly_salary (
	"year" int4 NULL,
	"month" int4 NULL,
	branch_id int4 NULL,
	salary_per_hour int4 null,
	job_date date null,
	udate timestamptz default now() null,
	CONSTRAINT pk_segment UNIQUE ("year","month",branch_id,job_date)
);