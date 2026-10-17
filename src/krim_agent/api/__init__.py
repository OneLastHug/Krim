"""HTTP 工厂只在 lifespan 显式装配资源。"""

from .server import create_app

__all__ = ["create_app"]
