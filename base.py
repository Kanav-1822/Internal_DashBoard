try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:
    from pydantic import BaseSettings


class Settings(BaseSettings):
    DATABASE_PORT: int
    POSTGRES_PASSWORD: str
    POSTGRES_USER: str
    POSTGRES_DB: str
    POSTGRES_HOST: str
    POSTGRES_HOSTNAME: str
    CH_DB_PORT: int
    CH_DB_PASSWORD: str
    CH_DB_USER: str
    CH_DB_HOST: str

    SMTP_HOST: str
    SMTP_PASSWORD: str
    SMTP_PORT: int
    SMTP_USER: str
    SMTP_DEFAULT_FROM_ADDRESS: str
    SMTP_DEFAULT_SUBJECT: str
    SENDER: str
    SENDERNAME: str

    APPLICATION_VERSION: str = "0.0.0-LOCAL"

    @property
    def VERSION(self):
        return self.APPLICATION_VERSION

    try:
        model_config = SettingsConfigDict(env_file="./.env", extra="ignore")
    except NameError:

        class Config:
            env_file = "./.env"


settings = Settings()
