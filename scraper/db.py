from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine.connection import register_connection,set_default_connection
import pathlib
import config



# declare Base Dir and cluster bandle path 
BASE_DIR = pathlib.Path(__file__).resolve().parent
CLUSTER_BUNDLE = str(BASE_DIR/ "ignored" / "connect.zip")
# get cached settings 
settings = config.get_settings()

# # load environment variable from ._env - not neeeded anymore, because data are cached (config.get_settings())
# ENV_DIR = BASE_DIR.parent
# ENV_FILE_PATH = ENV_DIR / ".env"
# dotenv.load_dotenv(str(ENV_FILE_PATH))

# DB_CLIENT_ID =os.environ["ASTRA_DB_CLIENT_ID"] - values taken from os directy every time 
DB_CLIENT_ID =settings.db_client_id # cached data
# DB_CLIENT_SECRET =os.environ["ASTRA_DB_CLIENT_SECRET"] - values taken from os directy every time 
DB_CLIENT_SECRET =settings.db_client_secret  # cached data

def get_cluster():
    cloud_config= {
        'secure_connect_bundle': CLUSTER_BUNDLE
    }
    auth_provider = PlainTextAuthProvider(DB_CLIENT_ID , DB_CLIENT_SECRET)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    return cluster

def get_session():

    cluster = get_cluster()
    session = cluster.connect()
    register_connection(str(session),session=session)
    set_default_connection(str(session))
    return session
