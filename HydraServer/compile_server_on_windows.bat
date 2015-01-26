make clean
make server.c
gcc -c -IC:\Python27\include -o server.o server.c
gcc -static -LC:\Python27\Libs -o server.exe server.o -lpython27
pause