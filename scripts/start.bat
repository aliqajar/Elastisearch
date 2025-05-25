@echo off
REM Distributed Search Engine Startup Script for Windows
REM This script sets up the environment and starts all services

echo 🚀 Starting Distributed Search Engine
echo ======================================

REM Check if Docker is installed and running
echo Checking Docker installation...
docker --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Docker is not installed. Please install Docker Desktop first.
    pause
    exit /b 1
)

docker info >nul 2>&1
if errorlevel 1 (
    echo ❌ Docker is not running. Please start Docker Desktop first.
    pause
    exit /b 1
)
echo ✅ Docker is installed and running

REM Check if Docker Compose is installed
echo Checking Docker Compose installation...
docker-compose --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Docker Compose is not installed. Please install Docker Compose first.
    pause
    exit /b 1
)
echo ✅ Docker Compose is installed

REM Create necessary directories
echo Creating data directories...
if not exist "data\indexes\shard-0" mkdir data\indexes\shard-0
if not exist "data\indexes\shard-1" mkdir data\indexes\shard-1
if not exist "data\indexes\shard-2" mkdir data\indexes\shard-2
if not exist "data\tries\shard-0" mkdir data\tries\shard-0
if not exist "data\tries\shard-1" mkdir data\tries\shard-1
echo ✅ Data directories created

REM Stop existing containers
echo Stopping any existing containers...
docker-compose down >nul 2>&1
echo ✅ Existing containers stopped

REM Build and start services
echo Building and starting services...
echo Building Docker images...
docker-compose build --parallel

echo Starting infrastructure services (PostgreSQL, Redis, Kafka)...
docker-compose up -d postgres redis zookeeper kafka

echo Waiting for infrastructure services to be ready...
timeout /t 10 /nobreak >nul

echo Starting application services...
docker-compose up -d

echo ✅ All services started

REM Wait for services to be healthy
echo Waiting for services to become healthy...
set /a attempts=0
:healthcheck
set /a attempts+=1
curl -s http://localhost:8080/health >nul 2>&1
if errorlevel 1 (
    if %attempts% lss 30 (
        echo .
        timeout /t 2 /nobreak >nul
        goto healthcheck
    ) else (
        echo ❌ Services failed to become healthy within timeout
        echo Check logs with: docker-compose logs
        pause
        exit /b 1
    )
)
echo ✅ API Gateway is healthy

REM Display usage information
echo.
echo 🎉 Distributed Search Engine is now running!
echo.
echo API Endpoints:
echo   - API Gateway:     http://localhost:8080
echo   - Health Check:    http://localhost:8080/health
echo   - System Stats:    http://localhost:8080/stats
echo.
echo Quick Commands:
echo   # Load sample data
echo   python scripts\load_sample_data.py
echo.
echo   # Search documents
echo   curl -X POST http://localhost:8080/search ^
echo     -H "Content-Type: application/json" ^
echo     -d "{\"query\": \"machine learning\", \"size\": 10}"
echo.
echo   # Get typeahead suggestions
echo   curl "http://localhost:8080/typeahead?q=mach&limit=5"
echo.
echo Management Commands:
echo   # View logs
echo   docker-compose logs -f
echo.
echo   # Stop services
echo   docker-compose down
echo.
echo   # Restart services
echo   docker-compose restart
echo.

echo ✅ Startup completed successfully!
echo.
echo Press any key to load sample data, or Ctrl+C to exit...
pause >nul

REM Load sample data
echo Loading sample data...
python scripts\load_sample_data.py
if errorlevel 1 (
    echo ❌ Failed to load sample data. Please check Python installation.
    echo You can run it manually: python scripts\load_sample_data.py
)

echo.
echo 🎉 Setup complete! The search engine is ready to use.
pause 