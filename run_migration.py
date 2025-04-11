from config import Config  # Import Config class from config module
from framework import DataMigrationFramework
import data_migration

# Initialize the framework
framework = DataMigrationFramework()

# Automatically register functions annotated with @phase
framework.register_phases_from_annotations(data_migration)

if __name__ == "__main__":
    # Execute the data migration process
    framework.run()