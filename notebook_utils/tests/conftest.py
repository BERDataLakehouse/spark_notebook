class WarehouseResponse:
    """Fake Governance service response to getting the user or group warehouse prefix."""

    def __init__(self, value: str) -> None:
        self.sql_warehouse_prefix = value
