# upgrade_extension test
The upgrade_extension test case will fail if
run it locally. Because it calls
"install_new_version_diskquota" function which is
defined in concourse/scripts/upgrade_extension.sh.
You can write this function by yourself and
export it locally if you want to run it successfully.
