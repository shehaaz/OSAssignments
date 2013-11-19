#include "common/dfs_common.h"
#include <pthread.h>

/**
 * create a thread and activate it
 * entry_point - the function exeucted by the thread
 * args - argument of the function
 * return the handler of the thread
 */
inline pthread_t * create_thread(void * (*entry_point)(void*), void *args)
{
	// create the thread and run it
	pthread_t * thread = (pthread_t *)malloc(sizeof(pthread_t));
	assert(pthread_create(thread, NULL, entry_point, args) == 0);

	return thread;
}

/**
 * create a socket and return
 */
int create_tcp_socket()
{
	//create the socket and return the file descriptor
	return socket(AF_INET, SOCK_STREAM, 0);
}

/**
 * create the socket and connect it to the destination address
 * return the socket fd
 */
int create_client_tcp_socket(char* address, int port)
{
	printf("Creating create_client_tcp_socket with port: %d\n", port);
	fflush(stdout);
	assert(port >= 0 && port < 65536);
	int socket = create_tcp_socket();

	if (socket == INVALID_SOCKET) return 1;
	// connect it to the destination port
	sockaddr_in socketAddr;
	socketAddr.sin_family = AF_INET;
	socketAddr.sin_port = htons(port);
	// socketAddr.sin_addr.s_addr = gethostbyname(address);

	// socketAddr.sin_addr.s_addr = 0x7f000001;
	inet_aton(address, &(socketAddr.sin_addr));
	printf("connecting to socket: %d and port %d\n", socket, port);
	assert(connect(socket, (sockaddr *)&socketAddr, sizeof(socketAddr)) == 0);
	return socket;
}

/**
 * create a socket listening on the certain local port and return
 */
int create_server_tcp_socket(int port)
{
	assert(port >= 0 && port < 65536);
	int socket = create_tcp_socket();
	if (socket == INVALID_SOCKET) return 1;
	// listen on local port
	sockaddr_in socketAddr;
  memset(&socketAddr, '0', sizeof(socketAddr));

	socketAddr.sin_family = AF_INET;
	socketAddr.sin_port = htons(port);
	socketAddr.sin_addr.s_addr = htonl(INADDR_ANY);
	// inet_pton(AF_INET, "127.0.0.1", &(socketAddr.sin_addr));

	if(bind(socket, (sockaddr *) &socketAddr, sizeof(socketAddr)) != 0) {
		printf("create_server_tcp_socket: error binding to socket\n");
		exit(1);
	}
	// printf("server socket: %d and port: %d\n", socket, port);
	listen(socket, 5);

	return socket;
}

/**
 * socket - connecting socket
 * data - the buffer containing the data
 * size - the size of buffer, in byte
 */
void send_data(int socket, void* data, int size)
{
	assert(data != NULL);
	assert(size >= 0);
	if (socket == INVALID_SOCKET) return;
	// send data through socket
	assert(write(socket, data, size) >= 0);
}

/**
 * receive data via socket
 * socket - the connecting socket
 * data - the buffer to store the data
 * size - the size of buffer in byte
 */
void receive_data(int socket, void* data, int size)
{
	assert(data != NULL);
	assert(size >= 0);
	if (socket == INVALID_SOCKET) return;
	// fetch data via socket
	assert(read(socket, data, size) >=0);
}



// #include "common/dfs_common.h"
// #include <pthread.h>
// /**
//  * create a thread and activate it
//  * entry_point - the function exeucted by the thread
//  * args - argument of the function
//  * return the handler of the thread
//  */
// inline pthread_t* create_thread(void * (*entry_point)(void*), void *args)
// {
// 	//TODO: create the thread and run it
// 	pthread_t *thread = (pthread_t *) malloc(sizeof(pthread_t)); //made room in memory for pthread

// 	pthread_t p_thread;

// 	pthread_create(&p_thread,NULL,entry_point,args);

// 	thread = &p_thread;

// 	return thread;
// }

// /**
//  * create a socket and return
//  */
// int create_tcp_socket()
// {
// 	//TODO:create the socket and return the file descriptor 
// 	return socket(AF_INET, SOCK_STREAM, 0);
// }

// /**
//  * create the socket and connect it to the destination address
//  * return the socket fd
//  */
// int create_client_tcp_socket(char* address, int port)
// {
// 	assert(port >= 0 && port < 65536);
// 	sockaddr_in endpoint_addr;

// 	//TODO: connect it to the destination port

// 	//create socket
// 	int socket = create_tcp_socket();
// 	if (socket == INVALID_SOCKET) return 1;

// 	//fill in end-point Info
// 	endpoint_addr.sin_family = AF_INET;
// 	endpoint_addr.sin_port = htons(port);
// 	//converts the Internet host address from the IPv4 numbers-and-dots notation into binary form
// 	inet_aton(address, &(endpoint_addr.sin_addr));

// 	//connect
// 	connect(socket, (sockaddr *)&endpoint_addr, sizeof(sockaddr_in));


// 	return socket;
// }

// /**
//  * create a socket listening on the certain local port and return
//  */
// int create_server_tcp_socket(int port)
// {
// 	assert(port >= 0 && port < 65536);
	
// 	int socket = create_tcp_socket();
// 	if (socket == INVALID_SOCKET) return 1;
// 	//TODO: listen on local port

// 	struct sockaddr_in serv_addr;
// 	bzero((char *) &serv_addr, sizeof(serv_addr));

//     serv_addr.sin_family = AF_INET;
//     serv_addr.sin_addr.s_addr = INADDR_ANY;
//     serv_addr.sin_port = htons(port);

//     if (bind(socket, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) 
//               error("ERROR on binding");


//     if (listen(socket, 5) == -1) {
// 		printf("error while listening\n");
// 		return 1;
// 	}      


// 	return socket;
// }

// /**
//  * socket - connecting socket
//  * data - the buffer containing the data
//  * size - the size of buffer, in byte
//  */
// void send_data(int socket, void* data, int size)
// {
// 	assert(data != NULL);
// 	assert(size >= 0);
// 	if (socket == INVALID_SOCKET) return;
// 	//TODO: send data through socket
// 	/*
// 	 * Once a connection has been established,
// 	 * both ends can both read and write to the connection.
// 	 * Everything written by the client will be read by the server,
// 	 * and everything written by the server will be read by the client.
// 	 * */
// 	write(socket,data,size);
// }

// /**
//  * receive data via socket
//  * socket - the connecting socket
//  * data - the buffer to store the data
//  * size - the size of buffer in byte
//  */
// void receive_data(int socket, void* data, int size)
// {
// 	assert(data != NULL);
// 	assert(size >= 0);
// 	if (socket == INVALID_SOCKET) return;
// 	//TODO: fetch data via socket

// 	/*
// 	 * Once a connection has been established,
// 	 * both ends can both read and write to the connection.
// 	 * Everything written by the client will be read by the server,
// 	 * and everything written by the server will be read by the client.
// 	 * */
// 	read(socket,data,size);
// }