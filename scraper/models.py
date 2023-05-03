from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model




# List View -> Detail View

class PropertyModel(Model): #-> table
    __keyspace__ = "scraper_app"
    id = columns.Text(primary_key = True,required=True)
    date = columns.Date(primary_key= True)
    title : str = columns.Text()
    city : str = columns.Text(index=True)
    cityRadius = columns.Integer(index=True)
    propertyType = columns.Integer(index=True)
    price = columns.Float(default ="0.0",index=True)
    size = columns.Float(index=True)



# Detail View for ID 
class PropertyScrapeEvent(Model): #-> table
    __keyspace__ = "scraper_app"
    uuid=columns.UUID(primary_key=True)
    id = columns.Text(index=True)
    date = columns.Date(index=True)
    city : str = columns.Text()
    cityRadius = columns.Integer()
    propertyType = columns.Integer()
    price = columns.Float(default ="0.0")
    size = columns.Float()

class Radius(Model):
    __keyspace__ = "scraper_app"
    id = columns.Text(primary_key = True,required=True)
    city : str = columns.Text()
    cityRadius = columns.Text()