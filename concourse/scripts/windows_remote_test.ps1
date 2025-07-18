$progressPreference = 'silentlyContinue'
Start-Process msiexec.exe -Wait -ArgumentList '/I greengage-clients-x86_64.msi /quiet'
$env:PATH="C:\Program Files\Greengage\greengage-clients\bin;C:\Program Files\curl-win64-mingw\bin;" + $env:PATH
