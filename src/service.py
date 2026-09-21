"""服务启动入口：python -m src.service"""
from __future__ import annotations

import uvicorn


def main() -> None:
    uvicorn.run(
        "src.app:app",
        host="0.0.0.0",
        port=int(__import__("os").environ.get("PORT", "8080")),
        reload=False,
    )


if __name__ == "__main__":
    main()
