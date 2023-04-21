from celery import Celery
from celery.schedules import crontab
import config,db,models
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table

from celery.signals import (
    beat_init,
     worker_process_init
)

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

@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Calls test('hello') every 10 seconds.
    #sender.add_periodic_task(10.0, random_task.s('hello'),expires = 10)
        # Executes every Thursday at 15:45 a.m.
    sender.add_periodic_task(
        crontab(hour=16, minute="*/1", day_of_week=5),
        scrape_properties.s(),
    ),
    # sender.add_periodic_task(
    #     crontab(hour=16, minute="*/2", day_of_week=5),
    #     scrape_properties.s(),
    # )

property =models.Property
propertyScrapeEvent =models.PropertyScrapeEvent

def celery_on_startup(*args,**kwargs):
    if connection.cluster is not None:
        connection.cluster.shutdown()
    if connection.session is not None:
        connection.session.shutdown()        
    cluster = db.get_cluster()
    session = cluster.connect()
    connection.register_connection(str(session),session=session)
    connection.set_default_connection(str(session))
    sync_table(property)
    sync_table(propertyScrapeEvent)

# started independentyly 
#beat_init.connect(celery_on_startup)
worker_process_init.connect(celery_on_startup)




@celery_app.task
def random_task(name):
    print(f"Who thros a shoe? Honsetly {name}")

@celery_app.task
def list_properties():
    #print(list(property.objects.all()))
    print(list(property.objects().all().values_list("id",flat=True)))

@celery_app.task
def scrape_id(id):
    print(id)

@celery_app.task
def scrape_properties():
    print("Doing scraping")
    q= property.objects().all().values_list("id", flat=True)
    for id in q:
        scrape_id.delay(id)