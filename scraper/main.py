from typing import List  
from fastapi import FastAPI
from cassandra.cqlengine.management import sync_table,
import config,models, schema
from db import get_session
import crud



settings = config.get_settings()
app = FastAPI()

session = None 
propertyModel = models.PropertyModel
propertyScrapeEvent = models.PropertyScrapeEvent
radius = models.Radius

@app.on_event("startup")
def on_startup():
    global session
    session = get_session()
    sync_table(models.PropertyModel)
    sync_table(models.PropertyScrapeEvent)
    sync_table(models.Radius)


@app.get("/")
def read_index():
    return {"Hello":"World","name":settings.name }


@app.get("/properties",response_model=List[schema.PropertyListSchema])
def properties_list_view():
    return list(propertyModel.objects.all() )


@app.post("/events/scrape")
def events_scrape_list_view(data:schema.PropertyListSchema):
    property,_ = crud.add_scrape_event(data.dict())
    return property


@app.put("/events/update")
def events_scrape_list_view(data: schema.PropertyListSchema):
    property,_ = crud.add_scrape_event(data.dict())
    return property


@app.get("/properties/{id}")
def property_detail_view(id):
    data = dict(propertyModel.objects.get(id=id) )
    events = list(propertyScrapeEvent.objects().filter(id=id).limit(5))
    events = [schema.PropertyScrapeEventDetailSchema(**x) for x in events]
    data["events"]= events
    data["events_url"] = f"/properties/{id}/events"
    return data


@app.get("/properties/{id}/events",response_model=List[schema.
PropertyScrapeEventDetailSchema])
def property_scrapes_detail_view(id):
    return list(propertyScrapeEvent.objects().filter(id=id).limit(5))


# @app.get("/scrape/?={city}?={propertyType}?={radius}?={price}")
# def scrape(city, propertyType,radius,price):

