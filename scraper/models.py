from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.util import datetime_from_uuid1





# List View -> Detail View

class Property(Model): #-> table
    __keyspace__ = "scraper_app"
    id = columns.Text(primary_key = True,required=True)
    propertyType = columns.Text()
    title : str = columns.Text()
    price_str = columns.Text(default ="0.0")



# Detail View for ID 
class PropertyScrapeEvent(Model): #-> table
    __keyspace__ = "scraper_app"
    uuid=columns.UUID(primary_key=True)
    id = columns.Text(index=True)
    propertyType = columns.Text()
    title : str = columns.Text()
    price_str = columns.Text(default ="0.0")


data ={
    "id":"454",
    "title":"dupa",
    "propertyType":"dziura"
}
