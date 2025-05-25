#!/bin/bash

# Distributed Search Engine Startup Script
# This script sets up the environment and starts all services

set -e  # Exit on any error

echo "🚀 Starting Distributed Search Engine"
echo "======================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check if Docker is installed and running
check_docker() {
    print_info "Checking Docker installation..."
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        print_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    print_status "Docker is installed and running"
}

# Check if Docker Compose is installed
check_docker_compose() {
    print_info "Checking Docker Compose installation..."
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    print_status "Docker Compose is installed"
}

# Create necessary directories
create_directories() {
    print_info "Creating data directories..."
    
    mkdir -p data/indexes/shard-{0,1,2}
    mkdir -p data/tries/shard-{0,1}
    
    print_status "Data directories created"
}

# Check system resources
check_resources() {
    print_info "Checking system resources..."
    
    # Check available memory (Linux/macOS)
    if command -v free &> /dev/null; then
        available_mem=$(free -m | awk 'NR==2{printf "%.0f", $7}')
        if [ "$available_mem" -lt 4096 ]; then
            print_warning "Available memory is ${available_mem}MB. Recommended: 8GB+"
        else
            print_status "Memory check passed (${available_mem}MB available)"
        fi
    elif command -v vm_stat &> /dev/null; then
        # macOS
        print_info "Memory check skipped on macOS"
    fi
    
    # Check available disk space
    available_disk=$(df . | awk 'NR==2 {print $4}')
    if [ "$available_disk" -lt 10485760 ]; then  # 10GB in KB
        print_warning "Available disk space might be low. Recommended: 10GB+"
    else
        print_status "Disk space check passed"
    fi
}

# Stop existing containers
stop_existing() {
    print_info "Stopping any existing containers..."
    
    if docker-compose ps -q | grep -q .; then
        docker-compose down
        print_status "Existing containers stopped"
    else
        print_info "No existing containers found"
    fi
}

# Build and start services
start_services() {
    print_info "Building and starting services..."
    
    # Build images
    print_info "Building Docker images..."
    docker-compose build --parallel
    
    # Start infrastructure services first
    print_info "Starting infrastructure services (PostgreSQL, Redis, Kafka)..."
    docker-compose up -d postgres redis zookeeper kafka
    
    # Wait for infrastructure to be ready
    print_info "Waiting for infrastructure services to be ready..."
    sleep 10
    
    # Start application services
    print_info "Starting application services..."
    docker-compose up -d
    
    print_status "All services started"
}

# Wait for services to be healthy
wait_for_health() {
    print_info "Waiting for services to become healthy..."
    
    max_attempts=30
    attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -s http://localhost:8080/health > /dev/null 2>&1; then
            print_status "API Gateway is healthy"
            break
        fi
        
        attempt=$((attempt + 1))
        echo -n "."
        sleep 2
    done
    
    if [ $attempt -eq $max_attempts ]; then
        print_error "Services failed to become healthy within timeout"
        print_info "Check logs with: docker-compose logs"
        exit 1
    fi
}

# Display service status
show_status() {
    print_info "Service Status:"
    echo ""
    
    # Check each service
    services=(
        "api-gateway:8080"
        "search-service-1:8001"
        "search-service-2:8002" 
        "search-service-3:8003"
        "typeahead-service-1:8011"
        "typeahead-service-2:8012"
        "indexing-service-1:8021"
        "indexing-service-2:8022"
        "indexing-service-3:8023"
        "data-ingestion:8030"
    )
    
    for service in "${services[@]}"; do
        name=$(echo $service | cut -d: -f1)
        port=$(echo $service | cut -d: -f2)
        
        if curl -s http://localhost:$port/health > /dev/null 2>&1; then
            print_status "$name (port $port)"
        else
            print_error "$name (port $port) - Not responding"
        fi
    done
    
    echo ""
    print_info "Infrastructure Services:"
    
    # Check PostgreSQL
    if docker-compose exec -T postgres pg_isready > /dev/null 2>&1; then
        print_status "PostgreSQL"
    else
        print_error "PostgreSQL - Not ready"
    fi
    
    # Check Redis
    if docker-compose exec -T redis redis-cli ping > /dev/null 2>&1; then
        print_status "Redis"
    else
        print_error "Redis - Not responding"
    fi
    
    # Check Kafka (basic check)
    if docker-compose ps kafka | grep -q "Up"; then
        print_status "Kafka"
    else
        print_error "Kafka - Not running"
    fi
}

# Display usage information
show_usage() {
    echo ""
    print_info "🎉 Distributed Search Engine is now running!"
    echo ""
    echo "API Endpoints:"
    echo "  - API Gateway:     http://localhost:8080"
    echo "  - Health Check:    http://localhost:8080/health"
    echo "  - System Stats:    http://localhost:8080/stats"
    echo ""
    echo "Quick Commands:"
    echo "  # Load sample data"
    echo "  python3 scripts/load_sample_data.py"
    echo ""
    echo "  # Search documents"
    echo "  curl -X POST http://localhost:8080/search \\"
    echo "    -H 'Content-Type: application/json' \\"
    echo "    -d '{\"query\": \"machine learning\", \"size\": 10}'"
    echo ""
    echo "  # Get typeahead suggestions"
    echo "  curl 'http://localhost:8080/typeahead?q=mach&limit=5'"
    echo ""
    echo "  # Index a document"
    echo "  curl -X POST http://localhost:8080/index \\"
    echo "    -H 'Content-Type: application/json' \\"
    echo "    -d '{\"title\": \"Test\", \"content\": \"Test content\"}'"
    echo ""
    echo "Management Commands:"
    echo "  # View logs"
    echo "  docker-compose logs -f"
    echo ""
    echo "  # Stop services"
    echo "  docker-compose down"
    echo ""
    echo "  # Restart services"
    echo "  docker-compose restart"
    echo ""
    echo "  # Scale services"
    echo "  docker-compose up -d --scale search-service-1=2"
    echo ""
}

# Main execution
main() {
    # Parse command line arguments
    LOAD_SAMPLE_DATA=false
    SKIP_HEALTH_CHECK=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --load-sample-data)
                LOAD_SAMPLE_DATA=true
                shift
                ;;
            --skip-health-check)
                SKIP_HEALTH_CHECK=true
                shift
                ;;
            --help|-h)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --load-sample-data    Load sample documents after startup"
                echo "  --skip-health-check   Skip waiting for services to be healthy"
                echo "  --help, -h           Show this help message"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    # Run startup sequence
    check_docker
    check_docker_compose
    check_resources
    create_directories
    stop_existing
    start_services
    
    if [ "$SKIP_HEALTH_CHECK" = false ]; then
        wait_for_health
    fi
    
    show_status
    show_usage
    
    # Load sample data if requested
    if [ "$LOAD_SAMPLE_DATA" = true ]; then
        echo ""
        print_info "Loading sample data..."
        
        if command -v python3 &> /dev/null; then
            python3 scripts/load_sample_data.py
        else
            print_warning "Python3 not found. Please run manually:"
            print_info "python3 scripts/load_sample_data.py"
        fi
    fi
    
    echo ""
    print_status "Startup completed successfully!"
}

# Run main function
main "$@" 