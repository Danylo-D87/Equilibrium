from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Read variables defined in docker-compose.yml and .env
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_PORT: int

    # Database names (hardcoded or can be moved to env)
    DB_NAME_MARKET: str = "market_data"
    DB_NAME_STATS: str = "app_stats"

    # Magic: Pydantic automatically finds the .env file
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def raw_db_url(self) -> str:
        # Format: postgresql+asyncpg://user:pass@host:port/dbname
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME_MARKET}"

    @property
    def stats_db_url(self) -> str:
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME_STATS}"


settings = Settings()
