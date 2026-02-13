from dagster import Definitions, load_assets_from_modules

from .defs import assets
from .resources import GirderClient

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "girder": GirderClient(
            api_url="https://your-girder-instance.com/api/v1",
            api_key="your-api-key-here"
        )
    }
)