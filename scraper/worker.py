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

#worker local vars
celeryDockerInUse = False

# start and configure celery
celery_app = Celery(__name__)
settings = config.get_settings()
if celeryDockerInUse:
    REDIS_URL = "redis://redis:6379/0" # docker compose setting - celery & reids
else:
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
celery_app.conf.broker_connection_max_retries = 3 
propertyModel =models.PropertyModel
propertyScrapeEvent =models.PropertyScrapeEvent


@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Calls test('hello') every 10 seconds.
    #sender.add_periodic_task(10.0, random_task.s('hello'),expires = 10)
        # Executes every Thursday at 15:45 a.m.
    # get detached house data for poznan
    sender.add_periodic_task(
        crontab(hour=15, minute=20, day_of_week="*/1"),
        scrape_properties.s(20,"poznan",(0,5,10,15),None)
    ),
    # get semi-detached houses data for poznan
    sender.add_periodic_task(
        crontab(hour=15, minute=38, day_of_week="*/1"),
        scrape_properties.s(21,"poznan",(0,5,10,15),None)
    ),
    # get area data for poznan 
    sender.add_periodic_task(
        crontab(hour="*/1", minute="*/2", day_of_week="*/1"),
        scrape_properties.s(20,"poznan",(0,),None)
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

@celery_app.task
def scrape_property(scraper_id):
    s = scraper.Scraper.objects[scraper_id]
    s.scrapeSinglePage()


@celery_app.task
def scrape_properties(propertyType,city,radiusTuple,price):

    for radius in radiusTuple:
        
        # create query
        query =scraper.QueryData(propertyType,city,radius,price)
        print(query)
        # create scraper
        s = scraper.Scraper(query,True)

        while(True):
            scrape_property(scraper_id=id(s))
            # check if scrapping is done 
            if s.checkNewScrapedData():
                break
            s.query.queryUpdate()
 
        # # update radius if it was already in database 
        s.radiusUpdate(property=propertyModel)
 
        # put data in database 
        for d in s.scrapedData:
            newData =d.copy()
            newData["date"]=date.today()
            newData["title"] = newData["title"].encode("utf-8","ignore").decode("utf-8")
            create_entry(newData)
            print(newData)

