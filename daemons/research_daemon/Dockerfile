# Research Daemon runtime image.
# Kept deliberately minimal — OpenClaw will provide the sandbox harness and
# decide how to invoke the daemon. This image only guarantees the code
# imports cleanly and all dependencies are installed.

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml README.md SKILL.md ./
COPY research_daemon ./research_daemon

RUN pip install .

# OpenClaw invokes the daemon via the `research-daemon` console script.
# Individual subcommands are passed at container runtime, e.g.
#   docker run research-daemon fetch-quote AAPL
# The default entrypoint below serves as a healthcheck — it returns the
# CLI's top-level help and exits 0 if the package and script are intact.
ENTRYPOINT ["research-daemon"]
CMD ["--help"]
