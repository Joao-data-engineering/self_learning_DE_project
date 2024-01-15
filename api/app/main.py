from fastapi import FastAPI, Depends, HTTPException
from pydantic_model import ActorActressKPIModel, ActorActressQuery, EtlStatusModel, EtlStatusQuery
from schema import ActorActressKPISchema, EtlStatusSchema
from sqlalchemy.orm import Session
from typing import List
from db_conn import SessionLocal

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app = FastAPI()

@app.post("/top_actor_actress/", response_model=List[ActorActressKPIModel])
def query_actor_actress(query: ActorActressQuery, db: Session = Depends(get_db)):
    if query.role.lower() == "both":
        results = db.query(ActorActressKPISchema).all()
    else:
        results = db.query(ActorActressKPISchema).filter(ActorActressKPISchema.role == query.role).all()

    if not results:
        raise HTTPException(status_code=404, detail="No matching actors or actresses found")
    return results

@app.post("/etl_pipeline_status/", response_model=List[EtlStatusModel])
def query_etl_status(query: EtlStatusQuery, db: Session = Depends(get_db)):

    results = db.query(EtlStatusSchema).filter(EtlStatusSchema.date == query.date).all()

    if not results:
        raise HTTPException(status_code=404, detail="No matching date")
    return results