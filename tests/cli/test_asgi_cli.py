import subprocess
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from typing import Any, Generator, List, Optional, Type

import pytest
import requests


@pytest.fixture
def generate_template(tmp_path: Path) -> Generator[Any, None, None]:
    @contextmanager
    def factory(
        code: str, filename: str = "temp_app.py"
    ) -> Generator[Path, None, None]:
        temp_dir: Path = tmp_path / "faststream_templates"
        temp_dir.mkdir(exist_ok=True)

        file_path: Path = temp_dir / filename
        cleaned_code: str = dedent(code).strip()

        file_path.write_text(cleaned_code)

        try:
            yield file_path
        finally:
            file_path.unlink(missing_ok=True)

    return factory


class FastStreamCLI(threading.Thread):
    def __init__(self, command: List[str], wait_time: float = 5.0) -> None:
        super().__init__()
        self.command = command
        self.wait_time = wait_time
        self.process: Optional[subprocess.Popen[bytes]] = None

    def run(self) -> None:
        self.process = subprocess.Popen(self.command)
        self.process.wait()

    def stop(self) -> None:
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()

    def __enter__(self) -> None:
        self.start()
        time.sleep(self.wait_time)

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        self.stop()
        self.join()


def test_run_uvicorn(generate_template: Any) -> None:
    app_code = """
    from faststream import FastStream
    from faststream.nats import NatsBroker
    from faststream.asgi import AsgiResponse, get

    broker = NatsBroker()

    @get
    async def liveness_ping(scope):
        return AsgiResponse(b"hello world", status_code=200)

    app = FastStream(broker).as_asgi(
        asgi_routes=[
            ("/liveness", liveness_ping),
        ],
        asyncapi_path="/docs",
    )
    """
    with generate_template(app_code) as app_path:
        module_name = str(app_path).replace(".py", "")
        port = 5005
        with FastStreamCLI(
            ["faststream", "run", f"{module_name}:app", "--port", f"{port}"]
        ):
            r = requests.get(f"http://127.0.0.1:{port}/liveness")
            assert r.text == "hello world"
            assert r.status_code == 200
