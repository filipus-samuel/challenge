import argparse
import pandas as pd

from math import floor
from datetime import datetime
from util.db.conn import PG_CONN
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert

db_user = PG_CONN.DB_USER
db_password = PG_CONN.DB_PASS
db_host = PG_CONN.DB_HOST
db_port = PG_CONN.DB_PORT
db_name = PG_CONN.DB_NAME


def extract(employees_filepath, timesheets_filepath):
    df_employees = pd.read_csv(employees_filepath)
    df_timesheets = pd.read_csv(timesheets_filepath)

    return df_employees, df_timesheets


def transform(df_employees, df_timesheets, filter_date):
    # Get unique employees data, take the highest salary
    df_employees = df_employees.rename(columns={"employe_id": "employee_id"})
    df_employees = df_employees.loc[df_employees.groupby("employee_id")["salary"].idxmax()].reset_index(drop=True)

    year_month = filter_date[:7]
    df_timesheets = df_timesheets[df_timesheets["date"].str.contains(year_month)].reset_index(drop=True)
    df_timesheets = df_timesheets[(df_timesheets["date"] >= year_month) & (df_timesheets["date"] <= filter_date)]
    df_timesheets = df_timesheets[
        (df_timesheets["checkin"] < df_timesheets["checkout"])
        & (df_timesheets["checkin"] != df_timesheets["checkout"])
        & (~df_timesheets["checkin"].isna())
        & (~df_timesheets["checkout"].isna())
    ].reset_index(drop=True)

    # Merge both df
    df_merge = pd.merge(df_employees, df_timesheets, how="inner", on="employee_id")

    # Get employee salary
    df_salary = df_merge[["branch_id", "employee_id", "salary", "date"]].copy()
    df_salary[["year", "month", "day"]] = df_salary["date"].str.split(pat="-", expand=True)
    df_salary["year"] = df_salary["year"].astype(int)
    df_salary["month"] = df_salary["month"].astype(int)
    df_salary = df_salary.drop(columns=["date", "day"]).drop_duplicates().reset_index(drop=True)

    # Get branch total salary each month
    df_total_salary_per_month = df_salary[["branch_id", "year", "month", "salary"]].copy()
    df_total_salary_per_month["total_salary"] = df_total_salary_per_month.groupby(["branch_id", "year", "month"])["salary"].transform("sum")
    df_total_salary_per_month = df_total_salary_per_month.drop(columns=["salary"]).drop_duplicates().reset_index(drop=True)

    # Get employee working hour each day
    df_work_hour = df_merge[["branch_id", "employee_id", "date", "checkin", "checkout"]].copy()
    df_work_hour[["year", "month", "day"]] = df_work_hour["date"].str.split(pat="-", expand=True)
    df_work_hour["year"] = df_work_hour["year"].astype(int)
    df_work_hour["month"] = df_work_hour["month"].astype(int)
    df_work_hour = df_work_hour.drop(columns=["date", "day"])
    df_work_hour["checkin"] = df_work_hour["checkin"].apply(lambda x: pd.to_timedelta(x))
    df_work_hour["checkout"] = df_work_hour["checkout"].apply(lambda x: pd.to_timedelta(x))
    df_work_hour["working_hour"] = [int(floor((y.total_seconds() - x.total_seconds()) / 3600)) for x, y in zip(df_work_hour["checkin"], df_work_hour["checkout"])]
    df_work_hour = df_work_hour[["branch_id", "employee_id", "year", "month", "working_hour"]]

    # Get branch total working hour each month
    df_total_work_hour_per_month = df_work_hour[["branch_id", "year", "month", "working_hour"]].copy()
    df_total_work_hour_per_month["total_hour"] = df_total_work_hour_per_month.groupby(["branch_id", "year", "month"])["working_hour"].transform("sum")
    df_total_work_hour_per_month = df_total_work_hour_per_month.drop(columns=["working_hour"])
    df_total_work_hour_per_month = df_total_work_hour_per_month.drop_duplicates(["year", "month", "branch_id"]).reset_index(drop=True)

    # Get branch salary per hour each month
    df_salary_per_hour = pd.merge(df_total_salary_per_month, df_total_work_hour_per_month, on=["branch_id", "year", "month"], how="inner")
    df_salary_per_hour["salary_per_hour"] = round(df_salary_per_hour["total_salary"] / df_salary_per_hour["total_hour"]).astype(int)
    df_salary_per_hour = df_salary_per_hour[["year", "month", "branch_id", "salary_per_hour"]].drop_duplicates().reset_index(drop=True)
    df_salary_per_hour["job_date"] = filter_date
    df_salary_per_hour["udate"] = datetime.now()

    return df_salary_per_hour


def load(df_salary_per_hour, table_name):
    engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

    # Load DataFrame into PostgreSQL table
    df_salary_per_hour.to_sql(table_name, engine, index=False, if_exists="append", method=insert_on_conflict_nothing)

    print(f"DataFrame loaded into {table_name} table on PostgreSQL successfully.")


def insert_on_conflict_nothing(table, conn, keys, data_iter):
    # "a" is the primary key in "conflict_table"
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=["year", "month", "branch_id", "job_date"])
    result = conn.execute(stmt)
    return result.rowcount


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-ef", metavar="--employees-filepath", help="employees filepath", required=True)
    parser.add_argument("-tf", metavar="--timesheets-filepath", help="timesheets filepath", required=True)
    parser.add_argument("-d", metavar="--date", help="daily filter date", required=True)
    args = parser.parse_args()
    employees_filepath = args.ef
    timesheets_filepath = args.tf
    filter_date = args.d
    table_name = "hourly_salary"

    df_employees, df_timesheets = extract(employees_filepath, timesheets_filepath)
    df_salary_per_hour = transform(df_employees, df_timesheets, filter_date)
    load(df_salary_per_hour, table_name)
