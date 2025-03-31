FROM debian:latest

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHON_VERSION=3.12

# Install required dependencies and Python 3.12
RUN apt update && apt install -y \
    curl \
    ca-certificates \
    software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt update && apt install -y \
    python3.12 \
    python3.12-venv \
    python3.12-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -fsSL https://astral.sh/uv/install.sh | sh

# Set Python 3.12 as the default
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1

# Verify installation
RUN python3 --version && uv --version

RUN uv pip install -r pyproject.toml

CMD ["uv", "run", "server"]