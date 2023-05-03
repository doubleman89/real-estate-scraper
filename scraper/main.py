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
    # time = []
    # for i in range(len(events)):
    #     event = events[i]
    #     print(type(event))
    #     time.append(datetime_from_uuid1(event["uuid"]))
    events = [schema.PropertyScrapeEventDetailSchema(**x) for x in events]
    # for i in range(len(events)):
    #     setattr(events[i], "datetime", time[i])
    data["events"]= events
    data["events_url"] = f"/properties/{id}/events"
    return data


@app.get("/properties/{id}/events",response_model=List[schema.
PropertyScrapeEventDetailSchema])
def property_scrapes_detail_view(id):
    return list(propertyScrapeEvent.objects().filter(id=id).limit(5))


# @app.get("/scrape/?={city}?={propertyType}?={radius}?={price}")
# def scrape(city, propertyType,radius,price):
#     multipleScrape(propertyType,city,(radius,),price)
    # # create query
    # query =QueryData(propertyType,city,radius,price)
    # query.createUrl()
    # # create scraper
    # scraper = Scraper(query,True)
    # # scrape 
    # data,listOfIDs = scraper.scrapeData()
    # # get radius data from cassandra
    # cityRadius = CityRadius(property=property,listOfIDs=listOfIDs)     

    # # update radius if it was already in database 
    # for i in range(len(data)):
    #     id = listOfIDs[i]
    #     data[i]["cityRadius"] = cityRadius.getEntryData(id,data[i]["city"],data[i]["cityRadius"])
    
    # # put data in database 
    # for d in data:
    #     newData =d.copy()
    #     newData["date"]=date.today()
    #     newData["title"] = newData["title"].encode("utf-8","ignore").decode("utf-8")
    #     crud.create_entry(newData)
