from pydantic_settings import BaseSettings

class LoadConfig(BaseSettings):
    LOG_PATH: str

    MYSQL_HOST: str
    MYSQL_PORT: int
    MYSQL_USERNAME: str
    MYSQL_PASSWORD: str

    RABBITMQ_HOST: str
    RABBITMQ_USERNAME: str
    RABBITMQ_PASSWORD: str

    class Config:
        env_file = ".env"
    
config = LoadConfig()