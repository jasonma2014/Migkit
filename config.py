import os  # Explicitly import os
import yaml  # Explicitly import yaml

class Config:
    def __init__(self):
        # Get environment variable PROFILE, default to 'dev'
        profile = os.getenv("PROFILE", "dev")
        config_file = f"application-{profile}.yml"

        # Check if the configuration file exists
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Configuration file {config_file} not found.")

        # Read YAML configuration file
        with open(config_file, "r") as file:
            config_data = yaml.safe_load(file)

        # Database connection configuration
        self.SOURCE_DB_URI = config_data.get("source_db_uri", "mysql+pymysql://user:password@localhost/source_db")
        self.TARGET_DB_URI = config_data.get("target_db_uri", "postgresql+psycopg2://user:password@localhost/target_db")

# Instantiate the configuration object
config = Config()