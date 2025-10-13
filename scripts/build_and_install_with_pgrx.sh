#!/bin/bash

# fail if trying to reference a variable that is not set.
set -u

# exit immediately if a command exits with a non-zero status
set -e

PGVERSION=""
SOURCEDIR=""
INSTALL="False"
CLEAN="False"
help="false"
PACKAGEDIR=""
profile=""

while getopts "d:v:ichp:r:" opt; do
  case $opt in
    d) SOURCEDIR="$OPTARG"
    ;;
    v) PGVERSION="$OPTARG"
    ;;
    i) INSTALL="True"
    ;;
    c) CLEAN="True"
    ;;
    h) help="true"
    ;;
    p) PACKAGEDIR="$OPTARG"
    ;;
    r) profile="$OPTARG"
    ;;
  esac

  # Assume empty string if it's unset since we cannot reference to
  # an unset variabled due to "set -u".
  case ${OPTARG:-""} in
    -*) echo "Option $opt needs a valid argument. use -h to get help."
    exit 1
    ;;
  esac
done

if [ "$help" == "true" ]; then
    echo "Usage: $0 -d <source_directory> -v <postgres_version> [-i] [-h]"
    echo "  -d <source_directory>   : Directory containing the source code to build and install (defaults to current dir)."
    echo "  -v <postgres_version>   : Version of PostgreSQL to use (e.g., 12, 13, 14, 15)."
    echo "  -i                      : Install the built extension into PostgreSQL."
    echo "  -h                      : Display this help message."
    echo "  -p <package_directory>  : Directory to store the built package (optional)."
    echo "  -r <profile>            : Build profile to use (optional, e.g., release, dev)."
    echo "  -c                      : Clean the build artifacts before building."
    exit 0
fi

if [ "$SOURCEDIR" == "" ]; then
    SOURCEDIR=$(pwd)
fi

if [ ! -f "$SOURCEDIR/Cargo.toml" ]; then
  echo "Error: Cargo.toml not found in source directory: $SOURCEDIR"
  exit 1
fi

if [ "$PACKAGEDIR" != "" ] && [ "$INSTALL" == "True" ]; then
    echo "Cannot specify both package directory and install option."
    exit 1
fi

if [ "$PGVERSION" == "" ]; then
    PGVERSION=$(pg_config --version | awk '{print $2}' | cut -d. -f1)
    echo "Using default PostgreSQL version: $PGVERSION"
fi

source="${BASH_SOURCE[0]}"
while [[ -h $source ]]; do
   scriptroot="$( cd -P "$( dirname "$source" )" && pwd )"
   source="$(readlink "$source")"

   # if $source was a relative symlink, we need to resolve it relative to the path where the
   # symlink file was located
   [[ $source != /* ]] && source="$scriptroot/$source"
done

scriptDir="$( cd -P "$( dirname "$source" )" && pwd )"

. $scriptDir/utils.sh

pgBinDir=$(GetPostgresPath $PGVERSION)
PATH=$pgBinDir:$PATH;
pg_config_path=$pgBinDir/pg_config

# Install cargo-pgrx
# use cargo toml-cli to parse the toml file and get the pgrx version.
if command -v toml > /dev/null; then
    echo "toml-cli is already installed."
else
    echo "Installing toml-cli..."
    cargo install toml-cli
fi

# Get pgrx version from Cargo.toml using toml-cli
pgrxVersionRequired=$(toml get $SOURCEDIR/Cargo.toml dependencies.pgrx.version 2>/dev/null | tr -d '"' | sed 's/=//')
if [ -z "$pgrxVersionRequired" ]; then
  pgrxVersionRequired=$(toml get $SOURCEDIR/Cargo.toml dependencies.pgrx 2>/dev/null | tr -d '"' | sed 's/=//')
fi

if [ -z "$pgrxVersionRequired" ]; then
  echo "Error: Could not find pgrx version in $SOURCEDIR/Cargo.toml"
  exit 1
else
  echo "Using pgrx version $pgrxVersionRequired"
fi

pgrxInstallRequired="false"
if command -v cargo-pgrx > /dev/null; then
    pgrxVersionInstalled=$(cargo pgrx --version | awk '{print $2}')
    if [ "$pgrxVersionInstalled" != "$pgrxVersionRequired" ]; then
      pgrxInstallRequired="true"
    else
      echo "cargo-pgrx version $pgrxVersionInstalled is already installed."
    fi
else
  pgrxInstallRequired="true"
fi

if [ "$pgrxInstallRequired" == "true" ]; then
    echo "Installing cargo-pgrx..."
    cargo install --locked cargo-pgrx@${pgrxVersionRequired}
fi

cargo pgrx init --pg$PGVERSION $pg_config_path

packageProfileArg=""
installProfileArg=""
if [ "$profile" != "" ]; then
    packageProfileArg="--profile $profile"
    installProfileArg=$packageProfileArg
else
    installProfileArg="--release"
    packageProfileArg=""
fi

pushd $SOURCEDIR
if [ "$CLEAN" == "True" ]; then
    cargo clean
fi

if [ "$INSTALL" == "True" ]; then
    cargo pgrx install --sudo --pg-config $pg_config_path $installProfileArg
elif [ "$PACKAGEDIR" != "" ]; then
    cargo pgrx package --pg-config $pg_config_path --out-dir $PACKAGEDIR $packageProfileArg --no-default-features
fi
popd
