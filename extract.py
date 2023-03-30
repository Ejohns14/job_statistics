from config import connection
from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
import pandas as pd
import functools as ft


engine = create_engine(connection)
Base = automap_base()

Base.prepare(engine, reflect = True)
print(Base.classes.keys())

jobs = Base.classes.jobs
skills = Base.classes.skills
salaries = Base.classes.salaries 


session = Session(engine)

jobs_stmt = session.query(jobs)
salaries_stmt = session.query(salaries)
skills_stmt = session.query(skills)

jobs_df = pd.read_sql( jobs_stmt.statement, con = engine.connect())
salaries_df = pd.read_sql( salaries_stmt.statement, con = engine.connect())
skills_df = pd.read_sql( skills_stmt.statement, con = engine.connect())

result1 = pd.merge(jobs_df, salaries_df, how = "inner", on = ["id"])
result2 = pd.merge(result1, skills_df, how = "inner" ,on = ["id"])
print(result2)

result2.to_csv('data/joined_data.csv', index=False)


