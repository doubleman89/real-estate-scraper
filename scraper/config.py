from functools import lru_cache
from pydantic import BaseSettings,Field
import os

if os.getenv("CQLENG_ALLOW_SCHEMA_MANAGEMENT") is None: 
    os.environ["CQLENG_ALLOW_SCHEMA_MANAGEMENT"] ="1"

class Settings(BaseSettings):
    name : str = Field (...,env="PROJ_NAME")
    db_client_id : str = Field(...,env= "ASTRA_DB_CLIENT_ID")
    db_client_secret : str = Field(...,env= "ASTRA_DB_CLIENT_SECRET")
    # redis_url : str = Field(...,env= "REDIS_URL")
    rabbitmq_url : str = Field(...,env ="RABBITMQ_URL")
    rabbitmq_backend : str = Field(...,env ="RABBITMQ_BACKEND")

    
    class Config:
        env_file = ".env"
        print(os.path.dirname('current path'))

@lru_cache
def get_settings():
    return Settings()
