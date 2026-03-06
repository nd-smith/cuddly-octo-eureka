"""FastAPI app for EventHub utility dashboard."""

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

try:
    from core.security.ssl_dev_bypass import apply_ssl_dev_bypass

    apply_ssl_dev_bypass()
except ImportError:
    pass

from pathlib import Path

from fastapi import FastAPI
from fastapi.templating import Jinja2Templates

from pipeline.tools.eventhub_ui.routes import _helpers
from pipeline.tools.eventhub_ui.routes.bulk_checkpoints import (
    router as bulk_checkpoints_router,
)
from pipeline.tools.eventhub_ui.routes.checkpoints import router as checkpoints_router
from pipeline.tools.eventhub_ui.routes.dedup import router as dedup_router
from pipeline.tools.eventhub_ui.routes.injector import router as injector_router
from pipeline.tools.eventhub_ui.routes.lag import router as lag_router
from pipeline.tools.eventhub_ui.routes.logs import router as logs_router
from pipeline.tools.eventhub_ui.routes.message_viewer import (
    router as message_viewer_router,
)
from pipeline.tools.eventhub_ui.routes.overview import router as overview_router
from pipeline.tools.eventhub_ui.routes.pipeline_status import (
    router as pipeline_status_router,
)
from pipeline.tools.eventhub_ui.routes.replay import router as replay_router
from pipeline.tools.eventhub_ui.routes.search import router as search_router

app = FastAPI(title="EventHub Utility", docs_url=None, redoc_url=None)

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
_helpers.templates = templates

app.include_router(overview_router)
app.include_router(lag_router)
app.include_router(checkpoints_router)
app.include_router(logs_router)
app.include_router(message_viewer_router)
app.include_router(injector_router)
app.include_router(replay_router)
app.include_router(search_router)
app.include_router(pipeline_status_router)
app.include_router(bulk_checkpoints_router)
app.include_router(dedup_router)
