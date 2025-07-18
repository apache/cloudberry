#!/usr/bin/env bash

extract_rpm_to_tar () {
    rpm2cpio gpdb_package/greengage-db-*.rpm | cpio -idvm

    local tarball="${PWD}/gpdb_artifacts/bin_gpdb.tar.gz"
    pushd usr/local/greengage-db-*
        tar czf "${tarball}" ./*
    popd
}
extract_deb_to_tar () {
  ar -x gpdb_package/greengage-db-*.deb
  tar -xf data.tar.xz
  local tarball="${PWD}/gpdb_artifacts/bin_gpdb.tar.gz"
  pushd usr/local/greengage-db-*
      tar czf "${tarball}" ./*
  popd
}

if test -n "$(find gpdb_package -maxdepth 1 -name '*.rpm' -print -quit)"
then
    extract_rpm_to_tar "${@}"
else
    extract_deb_to_tar "${@}"
fi
