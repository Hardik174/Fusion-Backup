"""
Timer decorator module for measuring execution duration.

Provides a reusable decorator `@timer` to measure and log the start time,
end time, and total execution duration of any callable.
"""

import functools
import logging
import time
from typing import Callable, Any

# Get logger for timer module
logger = logging.getLogger("fusion_report_engine.timer")

def timer(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    A decorator that logs the start time, end time, and execution duration of a function.

    Args:
        func: The function to be timed.

    Returns:
        The wrapped function.
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_epoch = time.time()
        start_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_epoch))
        logger.info("Starting execution of '%s' at %s", func.__name__, start_time_str)
        
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            end_epoch = time.time()
            end_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_epoch))
            duration = end_epoch - start_epoch
            logger.info(
                "Finished execution of '%s' at %s. Duration: %.6f seconds",
                func.__name__,
                end_time_str,
                duration
            )
            
    return wrapper
