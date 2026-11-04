/* 
 * SAS_READ - mimicking the "read" side of sas
 *
 * This program is mimicking the actual SAS backend, it opens a socket and 
 * starts listening on that socket for data sent from the database.
 *
 * When data is received it reads until the socket and writes the data to
 * stdout.
 *
 * The real SAS backend will want to do something more intelligent.
 *
 */
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <unistd.h>
#include <errno.h>

/* 
 * When BINARY_OUTPUT is defined this program blindly reads from the
 * socket and writes the binary data directly to STDOUT.
 *
 * When BINARY_OUTPUT is NOT defined this program parses the data it
 * receives and writes translated data to STDOUT
 */
#define BINARY_OUTPUT

/* Define a 1K buffer, probably should be larger in practice */
#define BUFFER_SIZE 1024
char buffer[BUFFER_SIZE];

int read_data(int sockfd, char *buf, int len)
{
	int remaining = len;
	int n         = 1;

	while (remaining > 0 && n > 0)
	{
		n = read(sockfd, buf, remaining);
		if (n > 0)
		{
			remaining -= n;
			buf += n;
		}
	}
	if (remaining > 0)
		return n;        /* either 0: end of buffer, or -1 error */
	return len;          /* We read everything we expected */
}

int main(int argc, char *argv[])
{
	int sockfd, newsockfd, portno;
	struct sockaddr_in serv_addr, cli_addr;
	socklen_t serv_len = sizeof(serv_addr);
	socklen_t cli_len  = sizeof(cli_addr);
	int n, total;

	/* Get the port number from the arguments */
	if (argc != 2) 
	{
		fprintf(stderr,"usage: %s <port-number>\n", argv[0]);
		exit(1);
	}
	portno = atoi(argv[1]);

	/* Open a socket and bind to the port */
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) 
	{
		fprintf(stderr, "Error opening socket\n");
		exit(1);
	}
	bzero((char *) &serv_addr, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = htons( (int16_t) portno);
	serv_len = sizeof(serv_addr);
	if (bind(sockfd, (struct sockaddr *) &serv_addr, serv_len) < 0)
	{
		fprintf(stderr, "ERROR binding socket\n");
		exit(1);
	}

	/* Listen on the socket */
	fprintf(stderr, "Listening on port %d...\n", portno);
	listen(sockfd, 5);
	newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &cli_len);
	if (newsockfd < 0)
	{
		fprintf(stderr, "ERROR on accept\n");
		exit(1);
	}

	fprintf(stderr, "Connection established\n");

	/* Read data until the connection is closed */
	total = 0;
	do 
	{		 
#ifdef BINARY_OUTPUT
		n = read(newsockfd, buffer, BUFFER_SIZE);
		total += n;
		fwrite(buffer, 1, n, stdout);

#else
		/* Naive example using the known data format of the example table:
		 *   CREATE TABLE example(
		 *      id       varchar(10),
		 *      name     varchar(20),
		 *      value1   float8,
		 *      value2   float8,
		 *      value3   float8,
		 *      value4   float8
		 *   );
		 *
		 * text fields are always sent length prefixed
		 */
		int    id_len, name_len;
		char   id[11], name[21];
		double value1, value2, value3, value4;

		n = read_data(newsockfd, (char*) &id_len, sizeof(id_len));
		if (n <= 0 || id_len < 0 || id_len >= 4000)
			break;
		if (id_len > 0)
		{
			n = read_data(newsockfd, id, id_len);
			id[id_len] = '\0';
		}
		read_data(newsockfd, (char*) &name_len, sizeof(name_len));
		if (name_len > 0 && name_len < 4000)
		{
			n = read_data(newsockfd, name, name_len);
			name[name_len] = '\0';
		}
		read_data(newsockfd, (char*) &value1, sizeof(value1));
		read_data(newsockfd, (char*) &value2, sizeof(value2));
		read_data(newsockfd, (char*) &value3, sizeof(value3));
		read_data(newsockfd, (char*) &value4, sizeof(value4));
		if (n == 0)
		{
			fprintf(stderr, "%s: invalid record format recieved\n", argv[0]);
			exit(1);
		}

		total += id_len + name_len + sizeof(double)*4 + sizeof(int)*2;

		fprintf(stdout, "%s %s %f %f %f %f\n",
				id, name, value1, value2, value3, value4);
#endif
	}
	while (n > 0);
	
	fprintf(stderr, "%s: read %d bytes\n", argv[0], total);
	
	if (n < 0) 
	{
		fprintf(stderr, "ERROR reading from socket\n");
		exit(1);
	}
	if (n > 0)
	{
		fprintf(stderr, "%s: invalid record format recieved\n", argv[0]);
		exit(1);
	}

	return 0; 
}
