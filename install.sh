#!/bin/sh
set -e

# zawinski installer
# Usage: curl -fsSL https://raw.githubusercontent.com/femtomc/zawinski/main/install.sh | sh

REPO="femtomc/zawinski"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"

# Detect OS and architecture
detect_platform() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)

    case "$OS" in
        linux)
            case "$ARCH" in
                x86_64) PLATFORM="linux-x86_64" ;;
                *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
            esac
            ;;
        darwin)
            case "$ARCH" in
                x86_64) PLATFORM="macos-x86_64" ;;
                arm64) PLATFORM="macos-aarch64" ;;
                *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
            esac
            ;;
        *)
            echo "Unsupported OS: $OS"
            exit 1
            ;;
    esac
}

# Get latest release version
get_latest_version() {
    curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" | \
        grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/'
}

main() {
    detect_platform

    VERSION="${VERSION:-$(get_latest_version)}"
    if [ -z "$VERSION" ]; then
        echo "Error: Could not determine latest version"
        exit 1
    fi

    BINARY_NAME="zawinski-$PLATFORM"
    DOWNLOAD_URL="https://github.com/$REPO/releases/download/$VERSION/$BINARY_NAME"

    echo "Installing zawinski $VERSION for $PLATFORM..."

    # Create install directory
    mkdir -p "$INSTALL_DIR"

    # Download binary
    echo "Downloading $DOWNLOAD_URL..."
    curl -fsSL "$DOWNLOAD_URL" -o "$INSTALL_DIR/zawinski"
    chmod +x "$INSTALL_DIR/zawinski"

    echo ""
    echo "zawinski installed to $INSTALL_DIR/zawinski"

    # Check if in PATH
    if ! echo "$PATH" | grep -q "$INSTALL_DIR"; then
        echo ""
        echo "Add to your PATH:"
        echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
    fi

    echo ""
    echo "Get started:"
    echo "  zawinski init"
    echo "  zawinski topic new general -d 'General discussion'"
    echo "  zawinski post general -m 'Hello, agents!'"
}

main
