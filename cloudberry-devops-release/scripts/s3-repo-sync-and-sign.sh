#!/bin/bash

set -euo pipefail

# Description:
# This script automates several tasks related to managing RPM repositories in AWS S3.
# It handles the following operations:
#   1. Syncing an RPM repository from an S3 bucket to a local directory.
#   2. Signing all RPMs in the local repository with a specified GPG key.
#   3. Updating and signing the repository metadata.
#   4. Exporting the GPG public key and placing it in the repository for client use.
#   5. Optionally, uploading changes back to the S3 bucket and deleting files in S3 that no longer exist locally.
#   6. Decrypting and importing a GPG private key used for signing.
#   7. A mode to only decrypt and import the GPG private key.
#   8. Identifying and copying a newly built RPM to the appropriate repository.

# Function to display detailed usage information
usage() {
  cat << EOF
Usage: $0 [OPTIONS]

This script automates several tasks related to managing RPM repositories in AWS S3.
It can be used to sync repositories from S3, sign RPMs with a GPG key, update and sign repository metadata,
and optionally upload changes back to S3.

Options:
  -c                            Configure AWS credentials using 'aws configure'.
  -s <s3-bucket>                Specify the S3 bucket and path to sync (required for S3 operations).
  -d <local-dir>                Specify the local directory to sync to (default: ~/repo).
  -k <encrypted-key-file>       Specify the encrypted GPG private key file to import (optional).
  -g <gpg-key-id>               Specify the GPG key ID or email to use for signing (required for signing operations).
  --upload-with-delete          Sync local changes to S3, deleting files in S3 that no longer exist locally.
  --s3-sync-only                Perform only the S3 sync to the local directory, inform the user, and exit.
  --import-gpg-key-only         Decrypt and import the GPG private key, then exit. No other operations will be performed.
  --copy-new-rpm                Copy the newly built RPM(s) to the appropriate repository directory based on architecture and version.
  -h, --help                    Display this help message and exit.

Examples:
  # Sync an S3 repository to a local directory and sign RPMs with a GPG key
  $0 -s s3://mybucket/repo -g mygpgkey@example.com

  # Sync an S3 repository only, without signing RPMs or performing other operations
  $0 -s s3://mybucket/repo --s3-sync-only

  # Decrypt and import a GPG private key, then exit
  $0 -k ~/path/to/encrypted-gpg-key.asc --import-gpg-key-only

  # Copy newly built RPMs to the appropriate repository and sign them
  $0 --copy-new-rpm -g mygpgkey@example.com

Notes:
  - The -s option is required for any operation that interacts with S3, such as syncing or uploading with delete.
  - The -g option is required for any operation that involves signing RPMs or repository metadata.
  - When using --upload-with-delete, ensure that you have the necessary permissions to delete objects in the specified S3 bucket.
  - If you only want to perform local operations (e.g., copying RPMs, signing), you do not need to specify the -s option.

EOF
}

# Parse options and arguments
GPG_KEY_ID=""
UPLOAD_WITH_DELETE=false
S3_SYNC_ONLY=false
IMPORT_GPG_KEY_ONLY=false
COPY_NEW_RPM=false
CONFIGURE_AWS=false
LOCAL_DIR=~/repo

# Function to check if required commands are available
check_commands() {
  local cmds=("aws" "gpg" "shred" "createrepo" "rpm" "find")
  for cmd in "${cmds[@]}"; do
    if ! command -v "$cmd" &> /dev/null; then
      echo "Error: Required command '$cmd' not found. Please install it before running the script."
      exit 1
    fi
  done
}

# Parse options
while [[ "$#" -gt 0 ]]; do
  case $1 in
    -c) CONFIGURE_AWS=true; shift ;;
    -s) S3_BUCKET="$2"; shift 2 ;;
    -d) LOCAL_DIR="$2"; shift 2 ;;
    -k) ENCRYPTED_KEY_FILE="$2"; shift 2 ;;
    -g) GPG_KEY_ID="$2"; shift 2 ;;
    --upload-with-delete) UPLOAD_WITH_DELETE=true; shift ;;
    --s3-sync-only) S3_SYNC_ONLY=true; shift ;;
    --import-gpg-key-only) IMPORT_GPG_KEY_ONLY=true; shift ;;
    --copy-new-rpm) COPY_NEW_RPM=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1"; usage; exit 1 ;;
  esac
done

check_commands

# AWS credentials configuration (optional)
if [ "$CONFIGURE_AWS" = true ]; then
  echo "Configuring AWS credentials..."
  aws configure
fi

# Decrypt and import GPG private key if in import-only mode or not in sync-only mode
if [ -n "${ENCRYPTED_KEY_FILE:-}" ]; then
  DECRYPTED_KEY_FILE="${ENCRYPTED_KEY_FILE%.*}"
  echo "Decrypting GPG private key..."
  gpg --decrypt --output "$DECRYPTED_KEY_FILE" "$ENCRYPTED_KEY_FILE"

  # Check if the key is already imported
  if gpg --list-keys | grep -q "$GPG_KEY_ID"; then
    echo "GPG key already imported."
  else
    gpg --import "$DECRYPTED_KEY_FILE"
  fi

  # Securely delete the decrypted key file
  shred -u "$DECRYPTED_KEY_FILE"

  # Exit if only importing GPG key
  if [ "$IMPORT_GPG_KEY_ONLY" = true ]; then
    echo "GPG key has been decrypted and imported successfully. Exiting."
    exit 0
  fi
fi

# Check access to the S3 bucket and perform sync only if needed
if [ "$IMPORT_GPG_KEY_ONLY" = false ] && [ "$S3_SYNC_ONLY" = false ] && [ "$COPY_NEW_RPM" = false ] && [ "$UPLOAD_WITH_DELETE" = false ]; then
  if [ -z "${S3_BUCKET:-}" ]; then
    echo "Error: S3 bucket (-s) is required."
    exit 1
  fi

  echo "Checking access to S3 bucket $S3_BUCKET..."
  if ! aws s3 ls "$S3_BUCKET" &> /dev/null; then
    echo "Error: Unable to access S3 bucket $S3_BUCKET. Please check your AWS credentials and permissions."
    exit 1
  fi

  # Sync the S3 repository to the local directory
  mkdir -p "$LOCAL_DIR"
  echo "Syncing S3 repository from $S3_BUCKET to $LOCAL_DIR..."
  aws s3 sync "$S3_BUCKET" "$LOCAL_DIR"

  # Check if the operation is `s3-sync-only`
  if [ "$S3_SYNC_ONLY" = true ]; then
    echo "S3 sync operation completed successfully."
    exit 0
  fi
fi

# Copy the newly built RPM to the appropriate repository
if [ "$COPY_NEW_RPM" = true ]; then
  echo "Identifying the newly built RPMs..."

  for ARCH in x86_64 noarch; do
    RPM_DIR=~/rpmbuild/RPMS/$ARCH

    # Check if the RPM directory exists
    if [ ! -d "$RPM_DIR" ]; then
      echo "Warning: Directory $RPM_DIR does not exist. Skipping $ARCH."
      continue
    fi

    # Find all matching RPMs and copy them to the appropriate repository directory
    NEW_RPMS=$(find "$RPM_DIR" -name "cloudberry-*.rpm" ! -name "*debuginfo*.rpm")
    if [ -n "$NEW_RPMS" ]; then
      for NEW_RPM in $NEW_RPMS; do
        # Determine the repository (el8 or el9) based on the RPM filename
        if echo "$NEW_RPM" | grep -q "\.el8\."; then
          TARGET_REPO="$LOCAL_DIR/el8/$ARCH"
        elif echo "$NEW_RPM" | grep -q "\.el9\."; then
          TARGET_REPO="$LOCAL_DIR/el9/$ARCH"
        else
          echo "Error: Unable to determine the correct repository for $NEW_RPM. Exiting."
          exit 1
        fi

        # Ensure the target repository directory exists
        mkdir -p "$TARGET_REPO"

        # Copy the RPM to the target repository
        echo "Copying $NEW_RPM to $TARGET_REPO..."
        cp "$NEW_RPM" "$TARGET_REPO/"
        echo "Copy operation completed."
      done
    else
      echo "No matching RPMs found in $RPM_DIR."
    fi
  done
fi

# Define the directories for `el8` and `el9` repositories
REPO_DIRS=("$LOCAL_DIR/el8/x86_64" "$LOCAL_DIR/el8/noarch" "$LOCAL_DIR/el9/x86_64" "$LOCAL_DIR/el9/noarch")

# Traverse each repository directory (el8 and el9) and sign RPMs
for REPO_DIR in "${REPO_DIRS[@]}"; do
  if [ -d "$REPO_DIR" ]; then
    echo "Processing repository at $REPO_DIR..."

    # Export GPG public key for clients and place it in the root of the repository
    TEMP_GPG_KEY=$(mktemp)
    echo "Exporting GPG public key to temporary location..."
    gpg --armor --export "$GPG_KEY_ID" > "$TEMP_GPG_KEY"

    # Import the GPG public key to RPM database
    echo "Importing GPG public key into RPM database..."
    sudo rpm --import "$TEMP_GPG_KEY"

    # Sign each RPM in the directory
    echo "Signing RPM packages in $REPO_DIR..."
    find "$REPO_DIR" -name "*.rpm" -exec rpm --addsign --define "_gpg_name $GPG_KEY_ID" {} \;

    # Verify that RPMs were signed successfully
    echo "Verifying RPM signatures in $REPO_DIR..."
    find "$REPO_DIR" -name "*.rpm" -exec rpm -Kv {} \;

    # Recreate the repository metadata
    echo "Updating repository metadata in $REPO_DIR..."
    createrepo --update "$REPO_DIR"

    # Sign the repository metadata, automatically overwriting if the file already exists
    echo "Signing repository metadata in $REPO_DIR..."
    gpg --batch --yes --detach-sign --armor --local-user "$GPG_KEY_ID" "$REPO_DIR/repodata/repomd.xml"

    # Copy the public key to each repo
    cp "$TEMP_GPG_KEY" "$REPO_DIR/RPM-GPG-KEY-cloudberry"

    # Clean up temporary GPG key
    rm -f "$TEMP_GPG_KEY"
  else
    echo "Warning: Repository directory $REPO_DIR does not exist. Skipping..."
  fi
done

# Upload changes to S3 with --delete option if requested
if [ "$UPLOAD_WITH_DELETE" = true ]; then
  if [ -z "${S3_BUCKET:-}" ]; then
    echo "Error: S3 bucket (-s) is required for upload with delete."
    exit 1
  fi

  echo "Uploading local changes to S3 with --delete option..."
  aws s3 sync "$LOCAL_DIR" "$S3_BUCKET" --delete
  echo "S3 sync with --delete completed."
fi

# Print completion message
echo "S3 repository sync, RPM signing, metadata signing, and public key export completed successfully."
