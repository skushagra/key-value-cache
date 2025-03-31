FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN pip install uv

# Explicitly create a virtual environment using uv
RUN uv venv .venv

# Activate the virtual environment and install dependencies
RUN uv pip install -r pyproject.toml

EXPOSE 7171

CMD ["uv", "run", "server"]
