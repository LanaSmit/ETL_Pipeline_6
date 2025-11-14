# Pipeline_Docker
A containerized data pipeline built with Docker Desktop, PostgreSQL, and WSL2 (Ubuntu for Windows).
It extracts data from parquet file and loads it into a PostgreSQL database using Python scripts.
All sensitive or personal configuration details are managed securely through a .env file (excluded from version control).
The setup uses Docker Compose for container management and runs smoothly on Windows via WSL2 integration.

## ✅ 1. Prerequisites

Ensure you have installed:

- **WSL2 (Ubuntu 22.04 / 24.04)**
- **Docker Desktop**
- **Git**
- **Python 3.9+** (optional for local tests)

Check Docker inside WSL:

```bash
docker --version
docker ps
