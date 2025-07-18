call "C:\Program Files\Greengage\greengage-clients\greengage_clients_path.bat"
del gpfdist_ssl\tbl2.tbl
gpfdist -p 7070 -d .\ --ssl gpfdist_ssl\certs_matching
