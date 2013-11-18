#include "client/dfs_client.h"
#include "datanode/ext.h"

int connect_to_nn(char* address, int port)
{
	assert(address != NULL);
	assert(port >= 1 && port <= 65535);
	//TODO: create a socket and connect it to the server (address, port)
	//assign return value to client_socket 
	int client_socket = -1;
	struct sockaddr_in nn_addr;

	memset((void *) &nn_addr, 0, sizeof(nn_addr));
	nn_addr.sin_family = AF_INET;
	nn_addr.sin_addr.s_addr = inet_addr(address);
	nn_addr.sin_port = htons(port);

	//create a TCP socket
	if ((client_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror("client, socket");
		exit(1);
	}

	//connect to namenode
	if (connect(client_socket, (struct sockaddr *) &nn_addr, sizeof(nn_addr))
			== -1) {
		perror("client, connect");
		exit(1);
	}

	return client_socket;
}

int modify_file(char *ip, int port, const char* filename, int file_size, int start_addr, int end_addr)
{
	int namenode_socket = connect_to_nn(ip, port);
	if (namenode_socket == INVALID_SOCKET) return -1;
	FILE* file = fopen(filename, "rb");
	assert(file != NULL);

	//TODO:fill the request and send
	dfs_cm_client_req_t request;
	
	//TODO: receive the response
	dfs_cm_file_res_t response;

	//TODO: send the updated block to the proper datanode

	fclose(file);
	return 0;
}

int push_file(int namenode_socket, const char* local_path)
{
	assert(namenode_socket != INVALID_SOCKET);
	assert(local_path != NULL);

	int i, len = 0;
	FILE* file = fopen(local_path, "rb");
	assert(file != NULL);

	FILE *fp = NULL;
	fp = fopen(local_path, "rb");
	fseek(fp, 0, SEEK_END);
	len = ftell(file);
	fseek(fp, 0, SEEK_SET);

	// Create the push request
	dfs_cm_client_req_t request;
	strcpy(request.file_name, local_path);
	request.file_size = len;
	request.req_type = 1;

	if (send(namenode_socket, (void *) &request, sizeof(dfs_cm_client_req_t), 0) == -1) {
		perror("client -> namendoe, send");
		exit(1);
	}

	//TODO:fill the fields in request and 
	
	//TODO:Receive the response
	dfs_cm_file_res_t response;

	if (recv(namenode_socket, &response.query_result.blocknum,
			sizeof(response.query_result.blocknum), 0) == -1) {
		perror("client -> namenode, recv");
		exit(1);
	}

	if (recv(namenode_socket, &response.query_result.filename,
			sizeof(response.query_result.filename), 0) == -1) {
		perror("client -> namenode, recv");
		exit(1);
	}

	if (recv(namenode_socket, &response.query_result.block_list,
			sizeof(response.query_result.block_list), 0) == -1) {
		perror("client -> namenode, recv");
		exit(1);
	}

	//TODO: Send blocks to datanodes one by one
	int dn_sock_fd = 0;
	struct sockaddr_in block_dst;
	char *buffer = NULL;
	dfs_cli_dn_req_t push_blk_req;

	for (i = 0; i < response.query_result.blocknum; i++) {

		if ((dn_sock_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
			perror("client -> datanode, socket");
			exit(1);
		}

		memset((void *) &block_dst, 0, sizeof(block_dst));
		block_dst.sin_family = AF_INET;
		block_dst.sin_addr.s_addr = inet_addr(
				response.query_result.block_list[i].loc_ip);
		block_dst.sin_port = htons(response.query_result.block_list[i].loc_port);


		if (connect(dn_sock_fd, &block_dst, sizeof(block_dst)) == -1) {
			perror("client -> datanode, connect");
			exit(1);
		}

		buffer = (char *) malloc(DFS_BLOCK_SIZE);
		fseek(file, (i * DFS_BLOCK_SIZE), SEEK_SET);
		fread(buffer, 1, DFS_BLOCK_SIZE, file);

		push_blk_req.block = response.query_result.block_list[i];
		memcpy(push_blk_req.block.content, buffer, DFS_BLOCK_SIZE);
		push_blk_req.op_type = 1;

		if (send(dn_sock_fd, &push_blk_req, sizeof(dfs_cli_dn_req_t), 0)
				== -1) {
			perror("client -> datanode, send");
			exit(1);
		}

		free(buffer);
		close(dn_sock_fd);
	}

	fclose(file);
	return 0;
}

int pull_file(int namenode_socket, const char *filename)
{
	assert(namenode_socket != INVALID_SOCKET);
	assert(filename != NULL);

	int i, response_len = 0, len = 0;
	char *buffer = NULL;
	int dn_sock_fd = 0;
	struct sockaddr_in block_dst;
	dfs_cli_dn_req_t pull_blk_req;

	//TODO: fill the request, and send
	dfs_cm_client_req_t req;
	dfs_cm_file_res_t res;

	strcpy(req.file_name, filename);
	req.file_size = 0;
	req.req_type = 0;

	//send request to namenode through namenode_socket
	if (send(namenode_socket, &req, sizeof(req), 0) < 0) {
		perror("client -> namenode, send");
		exit(1);
	}

	//TODO: Get the response

	if (recv(namenode_socket, &res.query_result.blocknum,
			sizeof(res.query_result.blocknum), 0) == -1) {
		perror("client -> namenode, recv");
		exit(1);
	}

	if (recv(namenode_socket, &res.query_result.filename,
			sizeof(res.query_result.filename), 0) == -1) {
		perror("client -> namenode, recv");
		exit(1);
	}

	if (recv(namenode_socket, &res.query_result.block_list,
			sizeof(res.query_result.block_list), 0) == -1) {
		perror("client -> namenode, recv");
		exit(1);
	}

	//TODO: Receive blocks from datanodes one by one
	for (i = 0; i < res.query_result.blocknum; i++) {

		if ((dn_sock_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
			perror("client -> datanode, socket");
			exit(1);
		}

		memset((void *) &block_dst, 0, sizeof(block_dst));
		block_dst.sin_family = AF_INET;
		block_dst.sin_addr.s_addr = inet_addr(
				res.query_result.block_list[i].loc_ip);
		block_dst.sin_port = htons(res.query_result.block_list[i].loc_port);

		if (connect(dn_sock_fd, &block_dst, sizeof(block_dst)) == -1) {
			perror("client -> datanode, connect");
			exit(1);
		}

		pull_blk_req.op_type = 0;
		pull_blk_req.block = res.query_result.block_list[i];

		if (send(dn_sock_fd, &pull_blk_req, sizeof(pull_blk_req), 0) == -1) {
			perror("client -> datanode, send");
			exit(1);
		}

		buffer = (char *) malloc(DFS_BLOCK_SIZE);
		if (recv(dn_sock_fd, buffer, DFS_BLOCK_SIZE, 0) == -1) {
			perror("client -> datanode, recv");
			exit(1);
		}

		memcpy(res.query_result.block_list[i].content, buffer, DFS_BLOCK_SIZE);

		free(buffer);
		close(dn_sock_fd);

	}
	
	FILE *file = fopen(filename, "wb");
	//TODO: resemble the received blocks into the complete file
	for (i = 0; i < res.query_result.blocknum; i++) {
		fwrite(res.query_result.block_list[i].content, DFS_BLOCK_SIZE, 1, file);
	}
	fclose(file);
	return 0;
}

dfs_system_status *get_system_info(int namenode_socket)
{
	assert(namenode_socket != INVALID_SOCKET);
	//TODO fill the result and send 
	dfs_cm_client_req_t request;
	//req_type: query datanodes
	request.req_type = 2;

	//TODO: get the response
	dfs_system_status *response; 
	//get the number of data nodes

	response = get_system_information(namenode_socket,request); //This function is in the namenode

	return response;		
}

int send_file_request(char **argv, char *filename, int op_type)
{
	int namenode_socket = connect_to_nn(argv[1], atoi(argv[2]));
	if (namenode_socket < 0)
	{
		return -1;
	}

	int result = 1;
	switch (op_type)
	{
		case 0:
			result = pull_file(namenode_socket, filename);
			break;
		case 1:
			result = push_file(namenode_socket, filename);
			break;
	}
	close(namenode_socket);
	return result;
}

dfs_system_status *send_sysinfo_request(char **argv)
{
	int namenode_socket = connect_to_nn(argv[1], atoi(argv[2]));
	if (namenode_socket < 0)
	{
		return NULL;
	}
	dfs_system_status* ret =  get_system_info(namenode_socket);
	close(namenode_socket);
	return ret;
}
