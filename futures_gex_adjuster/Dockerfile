# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Install the project into `/app`
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libffi-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy project configuration files
COPY pyproject.toml ./

# Install dependencies directly without requiring a lockfile
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -e .

# Copy application code
COPY . .

# Create a logs directory
RUN mkdir -p /app/logs && chmod -R 777 /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Reset the entrypoint
ENTRYPOINT []

# Run the script - update this to your actual script name
CMD ["uv", "run", "futures_gex.py"]