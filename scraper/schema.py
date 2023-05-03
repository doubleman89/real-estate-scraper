from pydantic import BaseModel,root_validator
from uuid import UUID
from typing import Optional, Any

from datetime import date,datetime
from cassandra.util import datetime_from_uuid1


class PropertySchema(BaseModel):
    id: str
    date :date
    title : Optional[str]
    city : str
    cityRadius : int 
    propertyType : int 
    price : float

class PropertyListSchema(BaseModel):
    id: str
    title : Optional[str]

class PropertyRadiusListSchema(BaseModel):
    id: str
    city : str
    cityRadius :int

class PropertyScrapeEventSchema(BaseModel):
    uuid: UUID
    id: str
    title : Optional[str]
    price_str : Optional[str]
    propertyType : Optional[str]


class PropertyScrapeEventDetailSchema(BaseModel):
    title : Optional[str]
    price_str : Optional[str]
    datetime : Optional[datetime]
    created : Optional[Any] = None

    @root_validator(pre=True)
    def extra_create_time_from_uuid(cls,values):
        values['datetime'] = datetime_from_uuid1(values['uuid'])
        values['created'] = datetime_from_uuid1(values['uuid']).timestamp()
        return values
