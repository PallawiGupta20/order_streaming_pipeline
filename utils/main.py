from fastapi import FastAPI, Response
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import time
import random
import json

app = FastAPI(title="Distributed Order Streaming Engine")

# --- PROMETHEUS PRODUCTION OBSERVABILITY INSTRUMENTATION ---
ORDER_VOLUME_COUNTER = Counter(
    'order_processing_total_volume', 
    'Total count of processed asynchronous orders handled by the system',
    ['status', 'item_category']
)
LATENCY_HISTOGRAM = Histogram(
    'order_pipeline_latency_seconds', 
    'Network processing latency window metrics mapping performance boundaries'
)

class OrderPayload(BaseModel):
    customer_id: str
    item_category: str
    order_value: float

@app.post("/api/v1/orders/process")
def initialize_order_stream(payload: OrderPayload):
    start_time = time.time()
    
    # Simulate dynamic database transactional bottlenecks
    time.sleep(random.uniform(0.02, 0.45))
    
    # Induce unpredictable synthetic network drops to simulate edge cases (95% pass rate)
    if random.random() > 0.95:
        ORDER_VOLUME_COUNTER.labels(status="FAILED", item_category=payload.item_category).inc()
        return {"status": "REJECTED", "message": "Internal Queue Timeout Exception"}

    # Track operational performance limits
    duration = time.time() - start_time
    LATENCY_HISTOGRAM.observe(duration)
    ORDER_VOLUME_COUNTER.labels(status="SUCCESS", item_category=payload.item_category).inc()
    
    return {
        "status": "QUEUED_ON_KAFKA_STREAM",
        "trackingId": f"TRK-{random.randint(1000, 9999)}",
        "latency_ms": round(duration * 1000, 2)
    }

@app.get("/metrics")
def expose_telemetry_metrics():
    """Serves performance logging data arrays to Prometheus scraping agents."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
