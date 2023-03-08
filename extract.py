jobs_df = pd.read_sql(sql = jobs_result.statement, con = engine.connect())
skills_df = pd.read_sql(sql = skills_result.statement, con = engine.connect())
salaries_df = pd.read_sql(sql = salaries_result.statement, con = engine.connect())
from config import connection

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
import pandas as pd
import functools as ft

# Prepairing connection to database
engine = create_engine(connection)

# Allows querying database(pulling data)
session = Session(engine)

# Reflect the tables in the database
Base = automap_base()
Base.prepare(engine, reflect = True)

# Mapped classes are created with default matching of the table names
jobs = Base.classes.jobs
skills = Base.classes.skills
salaries = Base.classes.salaries 


# Building queries to pull all Amazon data
jobs_result = session.query(jobs)
skills_result = session.query(skills)
salaries_result = session.query(salaries)

# Save variables into dataframes
jobs_df = pd.read_sql(sql = jobs_result.statement, con = engine.connect())
skills_df = pd.read_sql(sql = skills_result.statement, con = engine.connect())
salaries_df = pd.read_sql(sql = salaries_result.statement, con = engine.connect())
engine.dispose()

# Join all 3 tables together on their primary key, 
# I'm providing two ways of doing it below

#dfs = [jobs_df,skills_df,salaries_df]
#df_final = ft.reduce(lambda left, right: pd.merge(left, right, on='ID'), dfs)
#df_merged = ft.reduce(lambda  left,right: pd.merge(left,right,on=['id'], how='outer'), dfs)

# Make two merges

m1 = pd.merge(salaries_df, jobs_df, how = "inner", on = ["id"])
tot_merge = pd.merge(m1, skills_df, how = "inner", on = ["id"])

# Write this joined dataframe to the data / folder

tot_merge.to_csv("data/joined_data.csv", index = False)
