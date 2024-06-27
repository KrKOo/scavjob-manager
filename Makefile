# Name of the binary to be built
BINARY_NAME=scavjob_manager

# Build directory
BUILD_DIR=build

# Default target
all: build

build:
	@echo "Building the binary..."
	@mkdir -p $(BUILD_DIR)
	@go build -o $(BUILD_DIR)/$(BINARY_NAME) scavjob_manager.go

clean:
	@rm -rf $(BUILD_DIR)

help:
	@echo "Makefile Usage:"
	@echo "  make         - Builds the binary"
	@echo "  make clean   - Cleans up the build directory"
	@echo "  make help    - Displays this help message"

.PHONY: all build clean help