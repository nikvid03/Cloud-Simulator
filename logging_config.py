import logging
import logging.handlers
import os
import sys


def setup_logging(log_dir: str = "logs", log_file: str = "simulator.log") -> None:
    """
    Configure root logger with:
    - Console handler (stdout, DEBUG+)
    - Rotating file handler (logs/simulator.log, DEBUG+, 10 MB x 5 backups)

    Format: HH:MM:SS.mmm [LEVEL] module_name: message

    Guards against double-initialization (safe to call multiple times).
    """
    root = logging.getLogger()
    if root.handlers:
        return  # already configured

    root.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(fmt)
    root.addHandler(console_handler)

    # Rotating file handler
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        filename=os.path.join(log_dir, log_file),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)
