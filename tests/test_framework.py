import pytest
from src.framework import Phase, phase, DataMigrationFramework

# Test fixtures
@pytest.fixture
def framework():
    return DataMigrationFramework()

# Test functions for phase decorator
@phase(Phase.EXTRACT)
def test_extract():
    return "extracted"

@phase(Phase.TRANSFORM)
def test_transform():
    return "transformed"

@phase(Phase.LOAD)
def test_load():
    return "loaded"

# Test Phase enum
def test_phase_enum():
    assert Phase.EXTRACT.value == "Extract"
    assert Phase.TRANSFORM.value == "Transform"
    assert Phase.LOAD.value == "Load"
    assert len(Phase) == 3

# Test phase decorator
def test_phase_decorator():
    assert hasattr(test_extract, "phase")
    assert test_extract.phase == "Extract"
    assert hasattr(test_transform, "phase")
    assert test_transform.phase == "Transform"
    assert hasattr(test_load, "phase")
    assert test_load.phase == "Load"

# Test framework registration
def test_register_phase(framework):
    framework.register_phase(test_extract)
    assert "Extract" in framework.phases
    assert framework.phases["Extract"] == test_extract

def test_register_phases_from_annotations(framework):
    # Create a test module with phase-annotated functions
    class TestModule:
        @phase(Phase.EXTRACT)
        def test_extract(self):
            pass

        @phase(Phase.TRANSFORM)
        def test_transform(self):
            pass

    test_module = TestModule()
    framework.register_phases_from_annotations(test_module)
    assert "Extract" in framework.phases
    assert "Transform" in framework.phases

# Test phase execution order
def test_phase_execution_order(framework):
    execution_order = []
    
    @phase(Phase.EXTRACT)
    def extract():
        execution_order.append("Extract")
        return "data"
    
    @phase(Phase.TRANSFORM)
    def transform(data):
        execution_order.append("Transform")
        assert data == "data"
        return f"transformed_{data}"
    
    @phase(Phase.LOAD)
    def load(data):
        execution_order.append("Load")
        assert data == "transformed_data"
        return f"loaded_{data}"
    
    framework.register_phase(extract)
    framework.register_phase(transform)
    framework.register_phase(load)
    
    result = framework.run()
    assert execution_order == ["Extract", "Transform", "Load"]
    assert result == "loaded_transformed_data"

# Test missing phase error
def test_missing_phase_error(framework):
    @phase(Phase.TRANSFORM)
    def transform():
        pass
    
    framework.register_phase(transform)
    
    with pytest.raises(ValueError, match="Phase 'Extract' is not registered!"):
        framework.run()

# Test phase value passing
def test_phase_value_passing(framework):
    @phase(Phase.EXTRACT)
    def extract():
        return "test_data"
    
    @phase(Phase.TRANSFORM)
    def transform(data):
        assert data == "test_data"
        return f"transformed_{data}"
    
    @phase(Phase.LOAD)
    def load(data):
        assert data == "transformed_test_data"
        return f"loaded_{data}"
    
    framework.register_phase(extract)
    framework.register_phase(transform)
    framework.register_phase(load)
    
    result = framework.run()
    assert result == "loaded_transformed_test_data" 