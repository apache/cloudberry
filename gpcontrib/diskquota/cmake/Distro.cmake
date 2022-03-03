# Cmake utility to identify the distribution names. Currently Below distributions
# can be identified:
# - centos6
# - centos7
# - unbuntu18
include_guard()

set(DISTRO_NAME "" CACHE STRING "Distribution name of current build environment")

if(NOT DISTRO_NAME)
    SET(DISTRO_NAME unknown)
    if(EXISTS "/etc/redhat-release")
        file(READ /etc/redhat-release rh_release)
        string(REGEX MATCH "CentOS release 6.*" matched6 "${rh_release}")
        string(REGEX MATCH "CentOS Linux release 7.*" matched7 "${rh_release}")
        string(REGEX MATCH "Red Hat Enterprise Linux release 8.*" matched_rhel8 "${rh_release}")
        string(REGEX MATCH "CentOS Linux release 8.*" matched_centos8 "${rh_release}")
        if (matched6)
            set(DISTRO_NAME rhel6)
        elseif(matched7)
            set(DISTRO_NAME rhel7)
        elseif(matched_rhel8 OR matched_centos8)
            set(DISTRO_NAME rhel8)
        endif()
    elseif(EXISTS "/etc/os-release")
        file(READ /etc/os-release os_release)
        string(REGEX MATCH "ID=ubuntu" isubuntu "${os_release}")
        string(REGEX MATCH "VERSION_ID=\"18.04\"" matched1804 "${os_release}")
        if (isubuntu AND matched1804)
            SET(DISTRO_NAME ubuntu18.04)
        endif()

        string(REGEX MATCH "ID=photon" isphoton "${os_release}")
        string(REGEX MATCH "VERSION_ID=3.0" matched30 "${os_release}")
        if (isphoton AND matched30)
            SET(DISTRO_NAME photon3)
        endif()
    endif()
endif()
