FROM python:3.12-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY requirements.txt /tmp/requirements-phase3.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements-phase3.txt && python -m pip check
WORKDIR /workspace
ENTRYPOINT ["python", "/workspace/src/run_phase3.py"]
