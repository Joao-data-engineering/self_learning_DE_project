from db_conn import Base
from sqlalchemy import Column, String, Integer, Date

class ActorActressKPISchema(Base):
    __tablename__ = "actors_actress_kpis"
    nconst = Column(String, primary_key=True)
    role = Column(String, index=True)
    name = Column(String)
    average_rating = Column(Integer)
    total_run_time_minutes = Column(Integer)
    total_titles_as_principal = Column(Integer)

class EtlStatusSchema(Base):
    __tablename__ = "etl_pipeline_status"
    stage = Column(String, primary_key=True) 
    table_name = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    status = Column(String, primary_key=True)