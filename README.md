# XtremeFlow

> **"Exhaust rate limits, not patience. Squeezing maximum throughput from every second."**

### 🦅 About

**XtremeFlow** is a high-performance asynchronous task scheduler engineered to push **Large Language Model (LLM)** workloads to their absolute physical limits.

**The Problem:**
LLM providers throttle your velocity through a combination of **Concurrency**, **RPS**/**RPM** or **TPS**/**TPM**. Most schedulers are defensive—they wait too long, leave gaps in your schedule, and waste capacity. In high-volume production, idle time is a lost resource.

**The XtremeFlow Philosophy:**
Stop being polite with your rate limits. **XtremeFlow is offensive.** It is designed to saturate your provider's capacity with surgical precision. Using a unique **Backpressure Reflex**, it maintains peak velocity until the very moment a limit is hit, executes a synchronized global cool-down, and resumes at full speed the millisecond the provider allows.

> ⚠️ **Limitation:** XtremeFlow is currently optimized for **single-process** `asyncio` applications. It manages state in-memory and does not support distributed rate limiting (e.g., Redis-based) out of the box.

### ⚡ Key Features

* **Aggressive Saturation**: Engineered to fill every available millisecond of your allowed rate, ensuring zero wasted throughput.
* **Backpressure Reflex**: Automatically detects 429 triggers and orchestrates a global **Exponential Backoff** across all workers to stay in perfect sync with provider resets.
* **Dynamic Calibration**: Supports post-request reporting of *actual* usage to instantly "refund" over-estimated capacity back to the scheduler.
* **Async-Native**: Built on `asyncio` for low-latency scheduling where every microsecond counts.
* **KV Cache Optimization**: Provides utilities to maximize KV cache utilization across parallel LLM requests, dramatically reducing token consumption and improving throughput.
* **Async Pipeline**: Producer-consumer pipeline for streaming workloads with automatic backpressure handling.

### 🚀 Quick Start

```python
import asyncio
from openai import RateLimitError
from xtremeflow.scheduler.rate_limit import auto_backoff
from xtremeflow.scheduler.token import TokenRateScheduler, report_token_usage

# Initialize: 10 concurrent slots, 50k TPM
scheduler = TokenRateScheduler(
    max_concurrency=10,
    max_tpm=50000
)

@auto_backoff(retry_for=RateLimitError, base_retry_after=2.0)
async def call_llm_api(prompt: str):
    """
    Wraps LLM call with Backpressure Reflex.
    Global synchronization ensures you don't keep hitting the wall during cooldown.
    """
    print(f"Executing task: {prompt}")
    
    # Simulated API call
    await asyncio.sleep(1)
    
    # Calibration: Refund unused quota to the scheduler
    report_token_usage(actual=450)
    
    return "success"

async def main():
    tasks = []
    for i in range(10):
        # Dispatch with an estimated cost to saturate the current limit
        t = await scheduler.start_task(
            call_llm_api(f"Task {i}"), 
            estimated_tokens=500
        )
        tasks.append(t)
    
    results = await asyncio.gather(*tasks)
    print(f"XtremeFlow: Successfully processed {len(results)} tasks at peak throughput.")

if __name__ == "__main__":
    asyncio.run(main())
```

### 🔥 Performance Tools

Beyond rate limiting, XtremeFlow provides utilities to maximize token efficiency and throughput.

**KV Cache Optimization** (`kv_batch`)
```python
from xtremeflow.kvbatch import kv_batch

# First request establishes KV cache, rest run in parallel
task = kv_batch(
    llm_score(prompt) for prompt in same_job_with_different_resumes
)
results = await task
```
Reduces token consumption by 40-60% for batched requests with shared prefixes.

**Async Pipeline** (`async_pipeline`)
```python
from xtremeflow.pipeline import async_pipeline

# Producer: scheduler-controlled, exhausts this tier's rate limit
async def producer(queue: asyncio.Queue):
    async for item in source:
        task = await scheduler.start_task(llm_api(item), estimated_tokens=1000)
        await queue.put(task)

# Processor: slower sequential processing, yields to next tier
async def process_item(item):
    result = await item
    return await db_write(result)  # Different rate limit tier

async for result in async_pipeline(producer, process_item):
    yield result  # Can chain to another tier
```
Decouples rate limit tiers—exhausting each tier's limit frees up quota for other tasks immediately, maximizing overall system throughput.