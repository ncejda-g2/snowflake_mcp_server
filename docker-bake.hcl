variable "IMG_SHA" {
  default = "latest"
}

variable "PYTHON_VERSION" {
  default = "3.12-slim"
}

variable "REGISTRY" {
  default = "ghcr.io"
}

variable "REPO_NAME" {
  default = "g2crowd/g2-mcp-server"
}

variable "APP_NAME" {
  default = "g2-mcp-server"
}

variable "ENVIRONMENT" {
  default = "dev"
}

// Base target with common settings
target "base" {
  dockerfile = "Dockerfile"
  platforms = ["linux/arm64"]
  context = "."
  args = {
    PYTHON_VERSION = "${PYTHON_VERSION}"
  }
}

// Base stage target for caching
target "base-stage" {
  inherits = ["base"]
  target = "base"
  tags = ["${REGISTRY}/${REPO_NAME}:base-${IMG_SHA}"]
  // Local cache for docker driver compatibility
  cache-from = ["type=local,src=/tmp/buildx-cache"]
  cache-to = ["type=local,dest=/tmp/buildx-cache,mode=max"]
}

// Production dependencies target
target "prod-deps" {
  inherits = ["base"]
  target = "prod-deps"
  tags = ["${REGISTRY}/${REPO_NAME}:prod-deps-${IMG_SHA}"]
  cache-from = ["type=local,src=/tmp/buildx-cache"]
  cache-to = ["type=local,dest=/tmp/buildx-cache,mode=max"]
}

// Build target for development/testing
target "build" {
  inherits = ["base"]
  target = "build"
  tags = ["${REGISTRY}/${REPO_NAME}:build-${IMG_SHA}"]
  cache-from = ["type=local,src=/tmp/buildx-cache"]
  cache-to = ["type=local,dest=/tmp/buildx-cache,mode=max"]
}

// Final production image target
target "default" {
  inherits = ["base"]
  target = "final"
  tags = [
    "${REGISTRY}/${REPO_NAME}-${ENVIRONMENT}:${IMG_SHA}",
    "${REGISTRY}/${REPO_NAME}-${ENVIRONMENT}:latest"
  ]
  cache-from = ["type=local,src=/tmp/buildx-cache"]
  cache-to = ["type=local,dest=/tmp/buildx-cache,mode=max"]
}

// Registry cache targets (for CI/CD with compatible drivers)
target "prod-deps-registry" {
  inherits = ["base"]
  target = "prod-deps"
  tags = ["${REGISTRY}/${REPO_NAME}:prod-deps-${IMG_SHA}"]
  cache-from = [
    "type=registry,ref=${REGISTRY}/${REPO_NAME}:base",
    "type=registry,ref=${REGISTRY}/${REPO_NAME}:prod-deps"
  ]
  cache-to = ["type=registry,ref=${REGISTRY}/${REPO_NAME}:prod-deps,mode=max"]
}

target "default-registry" {
  inherits = ["base"]
  target = "final"
  tags = [
    "${REGISTRY}/${REPO_NAME}-${ENVIRONMENT}:${IMG_SHA}",
    "${REGISTRY}/${REPO_NAME}-${ENVIRONMENT}:latest"
  ]
  cache-from = [
    "type=registry,ref=${REGISTRY}/${REPO_NAME}:base",
    "type=registry,ref=${REGISTRY}/${REPO_NAME}:prod-deps",
    "type=registry,ref=${REGISTRY}/${REPO_NAME}-${ENVIRONMENT}:latest"
  ]
  cache-to = ["type=registry,ref=${REGISTRY}/${REPO_NAME}-${ENVIRONMENT}:latest,mode=max"]
}

// Group target to build all stages for optimal caching
group "all" {
  targets = ["base-stage", "prod-deps", "build", "default"]
}
