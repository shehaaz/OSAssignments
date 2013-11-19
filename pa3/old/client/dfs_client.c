#include "client/dfs_client.h"
#include "datanode/ext.h"

int connect_to_nn(char* address, int port)
{
	assert(address != NULL);
	assert(port >= 1 && port <= 65535);
	//TODO: create a socket and connect it to the server (address, port)
	//assign return value to client_socket 
	int client_socket = -1;

	client_socket = create_client_tcp_socket(address,port);

	assert(client_socket != -1);

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
	FILE* file = fopen(local_path, "rb");
	assert(file != NULL);

	// Create the push request
	dfs_cm_client_req_t request;

	// fill the fields in request and
	strcpy(request.file_name, local_path);
	fseek(file, 0L, SEEK_END);
	request.file_size = ftell(file);
	fseek(file, 0L, SEEK_SET);
	request.req_type = 1;
	printf("Client: pushing file with name: %s, size: %d\n", request.file_name, request.file_size);
	send_data(namenode_socket, (void *)&request, sizeof(request));

	// Receive the response
	dfs_cm_file_res_t response;
	receive_data(namenode_socket, (void *)&response, sizeof(response));


	// Send blocks to datanodes one by one
	int i;
	printf("Client: push_file blocknum %d withn name: %s\n", response.query_result.blocknum, response.query_result.filename);
	for (i = 0; i < response.query_result.blocknum; i++)
	{
		fread(&(response.query_result.block_list[i].content), DFS_BLOCK_SIZE, 1, file);
		dfs_cli_dn_req_t req;
		req.op_type = 1;
		memcpy(&(req.block), &(response.query_result.block_list[i]), sizeof(dfs_cm_block_t));
		int client_socket = create_client_tcp_socket(response.query_result.block_list[i].loc_ip, response.query_result.block_list[i].loc_port);
		send_data(client_socket, (void *)&req, sizeof(req));
	}

	fclose(file);
	return 0;
 //    assert(namenode_socket != INVALID_SOCKET);
 //    assert(local_path != NULL);
 //    FILE* file = fopen(local_path, "rb");
 //    assert(file != NULL);

 //    // Create the push request
 //    dfs_cm_client_req_t request;

 //    //TODO:fill the fields in request and

 //    //set file_name
 //    strcpy(request.file_name, local_path);

 //    fseek(file, 0L, SEEK_END);

 //    request.file_size = ftell(file);

	// fseek(file, 0L, SEEK_SET);
	// request.req_type = 1;

	// send_data(namenode_socket, (void *)&request, sizeof(dfs_cm_client_req_t));

 //    //TODO:Receive the response
 //    dfs_cm_file_res_t response;
 //    receive_data(namenode_socket, (void *)&response, sizeof(dfs_cm_client_req_t));

 //    //TODO: Send blocks to datanodes one by one
 //    int i;
 //    int stop = response.query_result.blocknum;

	// for (i = 0; i < stop; i++)
	// {
	// 	fread(&(response.query_result.block_list[i].content), DFS_BLOCK_SIZE, 1, file);
	// 	dfs_cli_dn_req_t req;
	// 	req.op_type = 1;
	// 	memcpy(&(req.block), &(response.query_result.block_list[i]), sizeof(dfs_cm_block_t));
	// 	int client_socket = create_client_tcp_socket(response.query_result.block_list[i].loc_ip, response.query_result.block_list[i].loc_port);
	// 	send_data(client_socket, (void *)&req, sizeof(req));
	// }


 //    fclose(file);
 //    return 0;
}

int pull_file(int namenode_socket, const char *filename)
{
	assert(namenode_socket != INVALID_SOCKET);
	assert(filename != NULL);

	// fill the request, and send
	dfs_cm_client_req_t request;
	strcpy(request.file_name, filename);
	request.file_size = 0;
	request.req_type = 0;
	send_data(namenode_socket, (void *)&request, sizeof(request));

	// Get the response
	dfs_cm_file_res_t response;
	receive_data(namenode_socket, (void *)&response, sizeof(response));

	// Receive blocks from datanodes one by one
	dfs_cm_block_t *block;
	dfs_cm_block_t blocks[response.query_result.blocknum];

	printf("Client: pull_file Blocknumber %d\n", response.query_result.blocknum);
	int i = 0;
	for (; i < response.query_result.blocknum; ++i)
	{
		block = &(response.query_result.block_list[i]);
		int client_socket = create_client_tcp_socket(block->loc_ip, block->loc_port);

		// Send request to datanode
		dfs_cli_dn_req_t dnrequest;
		dnrequest.op_type = 0;
		memcpy(&(dnrequest.block), (void *)block, sizeof(dfs_cm_block_t));
		send_data(client_socket, (void *)&dnrequest, sizeof(dnrequest));

		// Get the response from datanode
		dfs_cm_block_t dnresponse;
		receive_data(client_socket, (void *)&dnresponse, sizeof(dnresponse));
		// printf("block content %s\n", dnresponse.content);
		memcpy(&(blocks[dnresponse.block_id]), &dnresponse, sizeof(dfs_cm_block_t));
		printf("Received datanode at position node: %d with block_id: %d\n", dnresponse.dn_id, dnresponse.block_id);
	}

	FILE *file = fopen(filename, "wb");
	// resemble the received blocks into the complete file
	printf("Writing into file: %s with content: \n", filename);
	for (i = 0; i < response.query_result.blocknum; ++i)
	{
		//printf("content at %d is: %s\n", i, (blocks[i].content));
		fwrite(&(blocks[i].content), DFS_BLOCK_SIZE, 1, file);
	}

	fclose(file);
	return 0;
 //    assert(namenode_socket != INVALID_SOCKET);
 //    assert(filename != NULL);

 //    //TODO: fill the request, and send
 //    dfs_cm_client_req_t request;
 //    request.req_type = 0; // 0-read
 //    strcpy(request.file_name, filename); //set the filename
 //    request.file_size = 0;
 //    //WRITE the filled in request to the namenode_socket
 //    send_data(namenode_socket, (void *) &request, sizeof(dfs_cm_client_req_t));

 //    //TODO: Get the response from the name_node
 //    dfs_cm_file_res_t response;
 //    receive_data(namenode_socket, (void *)&response, sizeof(response));

 //    //TODO: Receive blocks from datanodes one by one

	// dfs_cm_block_t *block;
	// dfs_cm_block_t blocks[response.query_result.blocknum];

	// int i;
	// int stop = response.query_result.blocknum;
	// for (i = 0; i < stop; ++i)
	// {
	// 	block = &(response.query_result.block_list[i]);
	// 	int client_socket = create_client_tcp_socket(block->loc_ip, block->loc_port);

	// 	// Send request to datanode
	// 	dfs_cli_dn_req_t dnrequest;
	// 	dnrequest.op_type = 0;
	// 	memcpy(&(dnrequest.block), (void *)block, sizeof(dfs_cm_block_t));
	// 	send_data(client_socket, (void *)&dnrequest, sizeof(dnrequest));

	// 	// Get the response from datanode
	// 	dfs_cm_block_t dnresponse;
	// 	receive_data(client_socket, (void *)&dnresponse, sizeof(dnresponse));
	// 	memcpy(&(blocks[dnresponse.block_id]), &dnresponse, sizeof(dfs_cm_block_t));
	// 	printf("Received datanode at position node: %d with block_id: %d\n", dnresponse.dn_id, dnresponse.block_id);
	// }

	// //Write

 //    FILE *file = fopen(filename, "wb");
 //    //TODO: resemble the received blocks into the complete file
	// for (i = 0; i < stop; ++i)
	// {
	// 	fwrite(&(blocks[i].content), DFS_BLOCK_SIZE, 1, file);
	// }
 //    fclose(file);
 //    return 0;
}

dfs_system_status *get_system_info(int namenode_socket)
{
	assert(namenode_socket != INVALID_SOCKET);
	//TODO fill the result and send 
	dfs_cm_client_req_t request;
	//req_type: query datanodes
	request.req_type = 2;
	send_data(namenode_socket, (void *) &request, sizeof(dfs_cm_client_req_t));

	//TODO: get the response
	dfs_system_status *response; 
	//create space for the response
	response = (dfs_system_status *) malloc(sizeof(dfs_system_status));
	//get system information from the name_node
	receive_data(namenode_socket, (void *) response, sizeof(dfs_system_status));

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
