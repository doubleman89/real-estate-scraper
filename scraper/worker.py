from celery import Celery
from celery.schedules import crontab
import config,db,models
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table

from celery.signals import (
    beat_init,
    worker_process_init,
    worker_ready
)
from celery.utils.log import get_task_logger
import scraper
from crud import create_entry
from datetime import date

propertyModel =models.PropertyModel
propertyScrapeEvent =models.PropertyScrapeEvent

def celery_on_startup(*args, **kwargs):
    if connection.cluster is not None:
        connection.cluster.shutdown()
    if connection.session is not None:
        connection.session.shutdown()        
    # cluster = db.get_cluster()
    # session = cluster.connect()
    db.get_session()
    logger.info("connected")
    # connection.register_connection(str(session),session=session)
    # connection.set_default_connection(str(session))
    sync_table(propertyModel)
    sync_table(propertyScrapeEvent)


# started independentyly 
beat_init.connect(celery_on_startup)
#celeryd_init.connect(celery_on_startup)
worker_process_init.connect(celery_on_startup)
worker_ready.connect(celery_on_startup)

logger = get_task_logger(__name__)
#worker local vars
celeryDockerInUse = False
redisInUse = False 

# start and configure celery
celery_app = Celery(__name__)
settings = config.get_settings()
if redisInUse == False:
    # BROKER_URL = 'amqp://myuser:mypassword@localhost:5672/myvhost'
    # BROKER_URL = 'amqp://myuser:mypassword@rabbitmq:5672/myvhost'
    BROKER_URL = settings.rabbitmq_url
    # RESULT_BACKEND = 'rpc://myuser:mypassword@localhost:5672/myvhost'
    # RESULT_BACKEND = 'rpc://myuser:mypassword@rabbitmq:5672/myvhost'
    RESULT_BACKEND = settings.rabbitmq_backend
# elif celeryDockerInUse:
#     BROKER_URL = "redis://redis:6379/0" # docker compose setting - celery & reids
#     RESULT_BACKEND = BROKER_URL
# else:
#     BROKER_URL = settings.redis_url #docker containter setting - redis
#     RESULT_BACKEND = BROKER_URL
celery_app.conf.broker_url =   BROKER_URL
celery_app.conf.result_backend =   RESULT_BACKEND
celery_app.conf.enable_utc = True
celery_app.conf.timezone = "Europe/Warsaw" #change to your timezone 
celery_app.conf.broker_connection_max_retries =  30



@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        crontab(hour=0, minute=15, day_of_week="*/1"),
        scrape_properties.s(20,"poznan",(0,5,10,15),None)
    ),
    # get semi-detached houses data for poznan
    sender.add_periodic_task(
        crontab(hour=0, minute=30, day_of_week="*/1"),
        scrape_properties.s(21,"poznan",(0,5,10,15),None)
    ),
    # get area data for poznan 
    sender.add_periodic_task(
        crontab(hour=0, minute=45, day_of_week="*/1"),
        scrape_properties.s(30,"poznan",(0,5,10,15),None)
    ),

# memory  for scraper objects 
# celeryObjects = {}

@celery_app.task
def scrape_property(scraper_id):
    s = scraper.Scraper.objects[scraper_id]
    # s = celeryObjects[scraper_id]
    s.scrapeSinglePage()
    # logger.info("Scraped")

@celery_app.task
def transfer_data(radius,scrapedData):
    # put data in database 
    for d in scrapedData:
        #create new entry only for specific radius, in case of other city radius - skip 
        if d["cityRadius"] != radius:
            continue
        newData =d.copy()
        newData["date"]=date.today()
        newData["title"] = newData["title"].encode("utf-8","ignore").decode("utf-8")
        create_entry(newData)
        # logger.info(newData)
    logger.info("transferred")


@celery_app.task
def scrape_properties(propertyType,city,radiusTuple,price):

    # create query
    query =scraper.QueryData(propertyType,city,0,price)
    logger.info(query)
    # create scraper
    s = scraper.Scraper(query,True)
    # celeryObjects[id(s)]= s
    for radius in radiusTuple:
        # init query data for every radius 
        s.query.cityRadius = radius
        s.query.pageNo = 1


        while(True):
            scrape_property(scraper_id=id(s))
            # check if scrapping is done 
            if s.checkNewScrapedData():
                # check if there can be scrapped another age range
                updateAge = s.query.updateAge()
                if updateAge:
                    pass
                else:
                    break
            else:
                s.query.queryUpdate()
            

 
        # # update radius if it was already in database 
        s.radiusUpdate(property=propertyModel)
 
        # put data in database 
        transfer_data.delay(radius,scrapedData=s.scrapedData)
        # for d in s.scrapedData:
        #     #create new entry only for specific radius, in case of other city radius - skip 
        #     if d["cityRadius"] != radius:
        #         continue
        #     newData =d.copy()
        #     newData["date"]=date.today()
        #     newData["title"] = newData["title"].encode("utf-8","ignore").decode("utf-8")
        #     create_entry(newData)
        #     logger.info(newData)
        #     # print(newData)
    # close agent
    s.driver.quit()
    

