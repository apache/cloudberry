# Greengage Database Packaging System

## Overview

This documentation describes the Debian packaging system for Greengage Database located in the `gpAux/` subdirectory. The system builds Debian packages using a custom Makefile and `debian/rules` file.

## Location and Structure

The packaging system is located in:

```text
./gpAux/
```

The main components are:

- `Makefile` - Defines packaging targets, version management, and artifact collection
- `debian/rules` - Debian build rules with custom overrides
- `debian/control` - Package metadata and dependencies
- Other standard Debian packaging files

## Key Components

### Makefile Targets

1. **Version Management**:
   - `../VERSION`: Generates version file using `../getversion`
   - `version-vars`: Sets build variables (`FULL_VERSION`, `PACKAGE_VERSION`, `IS_RELEASE`, `STABILITY`, `BUILD_TYPE`) from `../VERSION`
   - `version-info`: Displays version information for debugging

2. **Packaging Targets**:
   - `pkg`: Default target (aliases to `pkg-deb`)
   - `pkg-deb`: Builds Debian package, preserves environment variables, and collects specific artifacts (`.deb`, `.ddeb`, `.build`, `.buildinfo`, `.changes`)
   - `changelog`: Generates `debian/changelog` using version variables
   - `debian/install`: Creates installation manifest

### Debian Rules File

The `debian/rules` file uses debhelper (dh) with custom overrides:

1. **Distribution-specific Dependencies**:
   - Detects Ubuntu 22.04 and adds `python2.7` dependency

2. **Build Process Overrides**:
   - Skips standard configure and build steps
   - Uses the project's `make dist` target for installation
   - Unsets standard compiler flags to avoid conflicts
   - Enables parallel builds using all available CPU cores

3. **Control File Generation**:
   - Adds Python dependencies for Ubuntu 22.04

## Usage

### Building the Package

From the project root directory, run:

```bash
make -C ./gpAux pkg-deb
```

### Custom Installation Paths

To customize installation paths, set environment variables:

```bash
make -C ./gpAux pkg-deb GGROOT=/custom/path GPDIR=custom_dir
```

### Environment Variables

- `GGROOT`: Installation root directory (default: `/opt/greengagedb`)
- `GPDIR`: Subdirectory under `GGROOT` (default: `<Package>` from `debian/control`)
- `ARTIFACTS_DIR`: Directory for build artifacts (default: `$(CURDIR)/../Package`)

## Build Process Details

1. **Version Generation**:
   - Runs `../getversion` to create `../VERSION`
   - Processes version string into `FULL_VERSION` and `PACKAGE_VERSION`
   - Sets `IS_RELEASE` and `STABILITY` for changelog generation

2. **Package Building**:
   - Executes `debuild` with preserved environment variables (`GGROOT`, `GPDIR`, `PACKAGE_NAME`)
   - Skips signing with `-us -uc` flags
   - Collects specific build artifacts (`.deb`, `.ddeb`, `.build`, `.buildinfo`, `.changes`) into `ARTIFACTS_DIR`

3. **Installation**:
   - Uses `make dist` for installation into `debian/tmp`
   - Generates file manifest in `debian/install`

## Dependencies

The packaging system automatically handles:

- `Ubuntu 22.04` detection and `python2.7` dependency
- Other distributions may require manual dependency configuration

## Maintenance

### Updating Package Metadata

Edit `debian/control` to update:

- Package description
- Maintainer information
- General dependencies

### Adding Distribution Support

Modify distribution detection in `debian/rules`:

```makefile
ifeq ($(LSB_SI),Ubuntu)
    ifeq ($(LSB_SR),22.04)
        DEPS=python2.7
    endif
    ifeq ($(LSB_SR),20.04)
        DEPS=python2.7
    endif
endif
```

## Notes

- Skips tests (`DEB_BUILD_OPTIONS=nocheck`) for faster builds
- Unsets compiler flags to avoid conflicts with the project's build system
- Enables parallel builds using all available CPU cores
- Builds without signing for development convenience
- Collects only specific build artifacts (`.deb`, `.ddeb`, `.build`, `.buildinfo`, `.changes`) into `$(CURDIR)/../Package`
