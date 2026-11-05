from __future__ import annotations

import uvicorn

from inventory_a2a_server.app import ServerSettings, create_app


def main() -> None:
    settings = ServerSettings.from_environment()
    uvicorn.run(
        create_app(settings=settings),
        host=settings.host,
        port=settings.port,
    )


if __name__ == "__main__":
    main()
