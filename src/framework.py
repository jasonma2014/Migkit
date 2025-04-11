from enum import Enum  # Import Enum from enum module

# Define phase enumeration type
class Phase(Enum):
    EXTRACT = "Extract"
    TRANSFORM = "Transform"
    LOAD = "Load"

# Modify decorator to accept enum members
def phase(phase_enum):
    def decorator(func):
        func.phase = phase_enum.value  # Store the string value corresponding to the enum
        return func
    return decorator

class DataMigrationFramework:
    def __init__(self):
        self.phases = {}

    def register_phase(self, func):
        if hasattr(func, "phase"):
            self.phases[func.phase] = func

    def register_phases_from_annotations(self, module):
        # Scan all functions in the module and register phases based on @phase annotations
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if callable(attr) and hasattr(attr, "phase"):  # Ensure only callable objects are processed
                self.register_phase(attr)

    def run(self):
        # Execute phases in predefined order: Extract -> Transform -> Load
        predefined_order = [Phase.EXTRACT.value, Phase.TRANSFORM.value, Phase.LOAD.value]
        for phase_name in predefined_order:
            if phase_name in self.phases:
                print(f"Running phase: {phase_name}...")
                self.phases[phase_name]()
            else:
                raise ValueError(f"Phase '{phase_name}' is not registered!")
