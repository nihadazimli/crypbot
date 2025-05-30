#!/bin/bash

set -e  # Exit on any error

# Configuration
REMOTE_HOST="nihad@116.202.102.134"
REMOTE_PATH="/home/nihad"
PROJECT_NAME="crypbot"
DOCKER_IMAGE="crypbot:latest"
CONTAINER_NAME="crypbot-container"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

# Function to create zip excluding unnecessary files
create_deployment_package() {
    log "Creating deployment package..."

    # Remove old zip if exists
    rm -f ${PROJECT_NAME}.zip

    # Create zip excluding unnecessary files
    zip -r ${PROJECT_NAME}.zip . \
        -x "*.git*" \
        -x "*.zip" \
        -x "*.log" \
        -x "*docs*" \
        -x "*coverage*" \
        -x "*.DS_Store*" \
        -x "*__pycache__*" \
        -x "*.idea*" \
        -x "*tmp*" \
        -x "*test*" \
        -x "deploy.sh"

    log "Package created: ${PROJECT_NAME}.zip ($(du -h ${PROJECT_NAME}.zip | cut -f1))"
}

# Function to upload to remote server
upload_to_server() {
    log "Uploading to remote server..."

    scp ${PROJECT_NAME}.zip ${REMOTE_HOST}:${REMOTE_PATH}/

    if [ $? -eq 0 ]; then
        log "Upload successful"
    else
        error "Upload failed"
        exit 1
    fi
}

# Function to execute remote deployment
deploy_on_remote() {
    log "Executing remote deployment..."

    ssh ${REMOTE_HOST} "bash -s" << EOF
        set -e
        cd ${REMOTE_PATH}

        echo "🚀 Starting deployment on remote server..."

        # Extract new version
        echo "📁 Extracting new version..."
        rm -rf ${PROJECT_NAME}
        unzip -q ${PROJECT_NAME}.zip -d ${PROJECT_NAME}

        # Build new Docker image
        echo "🐳 Building Docker image..."
        cd ${PROJECT_NAME}
        docker build -t ${DOCKER_IMAGE} .

        if [ \$? -ne 0 ]; then
            echo "❌ Docker build failed"
            exit 1
        fi

        # Stop and remove old container
        echo "🛑 Stopping old container..."
        docker stop ${CONTAINER_NAME} 2>/dev/null || true
        docker rm ${CONTAINER_NAME} 2>/dev/null || true

        # Start new container
        echo "▶️  Starting new container..."
        docker run -d --name ${CONTAINER_NAME} \
            --restart unless-stopped \
            -v \$(pwd)/config:/app/config \
            -v \$(pwd)/logs:/app/logs \
            ${DOCKER_IMAGE}

        # Wait a moment and check if container is running
        sleep 5
        if docker ps | grep -q ${CONTAINER_NAME}; then
            echo "✅ Deployment successful! Container is running."
            docker ps | grep ${CONTAINER_NAME}
        else
            echo "❌ Container failed to start"
            docker logs ${CONTAINER_NAME}
            exit 1
        fi

        # Cleanup
        rm -f ${REMOTE_PATH}/${PROJECT_NAME}.zip

        echo "🎉 Deployment completed successfully!"
EOF

    if [ $? -eq 0 ]; then
        log "Remote deployment successful!"
    else
        error "Remote deployment failed!"
        exit 1
    fi
}

# Function to check application health
health_check() {
    log "Performing health check..."

    ssh ${REMOTE_HOST} << EOF
        # Wait for application to start
        sleep 10

        # Check if container is still running
        if docker ps | grep -q ${CONTAINER_NAME}; then
            echo "✅ Container is running"

            # Check logs for any errors
            echo "📋 Recent logs:"
            docker logs --tail 20 ${CONTAINER_NAME}
        else
            echo "❌ Container is not running"
            echo "📋 Container logs:"
            docker logs ${CONTAINER_NAME}
            exit 1
        fi
EOF
}

# Main deployment flow
# Main deployment flow
main() {
    log "🚀 Starting deployment process..."

    create_deployment_package
    upload_to_server
    deploy_on_remote
    health_check

    # ✅ Start monitoring after successful deployment
    log "🔍 Starting container monitoring..."
    ssh ${REMOTE_HOST} << 'EOF'
        cd /home/nihad/

        # Stop any existing monitoring
        pkill -f "docker events.*crypbot-container" 2>/dev/null || true

        # Start new monitoring in background
        nohup ./monitor.sh > /tmp/monitor.log 2>&1 &

        echo "✅ Monitoring started"
EOF

    # Cleanup local zip
    rm -f ${PROJECT_NAME}.zip

    log "🎉 Deployment completed successfully!"
    log "📱 Telegram monitoring is active"
}


# Check if SSH connection works
log "Testing SSH connection..."
if ! ssh -o ConnectTimeout=10 ${REMOTE_HOST} "echo 'SSH connection successful'"; then
    error "Cannot connect to remote server"
    exit 1
fi

# Run main deployment
main
