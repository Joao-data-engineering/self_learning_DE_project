from pydantic import BaseModel, validator
from typing import Optional
from datetime import date

class ActorActressKPIModel(BaseModel):
    nconst: Optional[str] = None
    role: Optional[str] = None
    name: Optional[str] = None
    average_rating: Optional[float] = None
    total_run_time_minutes: Optional[int] = None
    total_titles_as_principal: Optional[int] = None

    class Config:
        orm_mode = True

class EtlStatusModel(BaseModel):
    stage: str
    table_name: str
    date: date
    status: str

    class Config:
        orm_mode = True

class EtlStatusQuery(BaseModel):
    date: date

class ActorActressQuery(BaseModel):
    role: str

    @validator('role')
    def validate_role(cls, v):
        if v.lower() not in ['actor', 'actress', 'both']:
            raise ValueError('role must be actor, actress, or both')
        return v.lower()