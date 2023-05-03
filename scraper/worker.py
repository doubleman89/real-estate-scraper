from celery import Celery
from celery.schedules import crontab
import config,db,models
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table

from celery.signals import (
    beat_init,
     worker_process_init
)

import scraper
from crud import create_entry
from datetime import date


celery_app = Celery(__name__)

settings = config.get_settings()
#REDIS_URL = "redis://redis:6379/0" # docker compose setting - celery & reids
REDIS_URL = settings.redis_url #docker containter setting - redis
celery_app.conf.broker_url =   REDIS_URL
celery_app.conf.result_backend =   REDIS_URL
# celery_app.conf.beat_schedule = {
#     'add-every-5-seconds': {
#         'task': 'worker.random_task',
#         'schedule': crontab(hour=16, minute=28, day_of_week=5),
#         'args': ("Maksym",)
#     },
#     # 'list_properties': {
#     #     'task': 'worker.list_properties',
#     #     'schedule': 10.0
#     # },
# }
celery_app.conf.enable_utc = True
celery_app.conf.timezone = "Europe/Warsaw" #change to your timezone 
propertyModel =models.PropertyModel
propertyScrapeEvent =models.PropertyScrapeEvent


@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Calls test('hello') every 10 seconds.
    #sender.add_periodic_task(10.0, random_task.s('hello'),expires = 10)
        # Executes every Thursday at 15:45 a.m.
    # get detached house data for poznan
    sender.add_periodic_task(
        crontab(hour=16, minute="*/1", day_of_week=5),
        scrape_properties.s(),
        scrape_properties.s(20,"poznan",(0,5,10,15),None,propertyModel = propertyModel)
    ),
    # get semi-detached houses data for poznan
    sender.add_periodic_task(
        crontab(hour=16, minute="*/1", day_of_week=5),
        scrape_properties.s(),
        scrape_properties.s(21,"poznan",(0,5,10,15),None,propertyModel = propertyModel)
    ),
    # get area data for poznan 
    sender.add_periodic_task(
        crontab(hour=16, minute="*/1", day_of_week=5),
        scrape_properties.s(),
        scrape_properties.s(30,"poznan",(0,5,10,15),None,propertyModel = propertyModel)
    ),



def celery_on_startup(*args,**kwargs):
    if connection.cluster is not None:
        connection.cluster.shutdown()
    if connection.session is not None:
        connection.session.shutdown()        
    cluster = db.get_cluster()
    session = cluster.connect()
    connection.register_connection(str(session),session=session)
    connection.set_default_connection(str(session))
    sync_table(propertyModel)
    sync_table(propertyScrapeEvent)

# started independentyly 
#beat_init.connect(celery_on_startup)
worker_process_init.connect(celery_on_startup)




# @celery_app.task
# def random_task(name):
#     print(f"Who thros a shoe? Honsetly {name}")

# @celery_app.task
# def list_properties():
#     #print(list(property.objects.all()))
#     print(list(property.objects().all().values_list("id",flat=True)))

@celery_app.task
def scrape_property(scraper_id):
    print("was here")
    s = scraper.Scraper.objects[scraper_id]
    s.scrapeSinglePage()

@celery_app.task
def scrape_properties(propertyType,city,radiusTuple,price,propertyModel):
    # print("Doing scraping")
    # q= property.objects().all().values_list("id", flat=True)
    # for id in q:
    #     scrape_id.delay(id)
    for radius in radiusTuple:
        # create query
        query =scraper.QueryData(propertyType,city,radius,price)
        # create scraper
        s = scraper.Scraper(query,True)
        # data =[]
        # listOfIDs =[]
        while(True):
            scrape_property(scraper_id=id(s))
            # convert JSON data 
            if s.checkNewScrapedData():
                break
            # data+=newData
            # listOfIDs+=newList
            s.query.queryUpdate()
        # get radius data from cassandra
        # cityRadius = scraper.CityRadius(property=propertyModel,listOfIDs=s.scrapedListOfIDs)     

        # # update radius if it was already in database 
        s.radiusUpdate(property=propertyModel)
        # for i in range(len(s.scrapedData)):
        #     id = listOfIDs[i]
        #     data[i]["cityRadius"] = cityRadius.getEntryData(id,data[i]["city"],data[i]["cityRadius"])
        
        # put data in database 
        for d in s.scrapedData:
            newData =d.copy()
            newData["date"]=date.today()
            newData["title"] = newData["title"].encode("utf-8","ignore").decode("utf-8")
            create_entry(newData)
        
    return list(propertyModel.objects.filter(date = date.today()) )
