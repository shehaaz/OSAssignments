#include "client/dfs_client.h"
#include "datanode/ext.h"

int connect_to_nn(char* address, int port)
{
	assert(address != NULL);
	assert(port >= 1 && port <= 65535);
	//TODO: create a socket and connect it to the server (address, port)
	//assign return value to client_socket 
	int client_socket = -1;

	//create client tcp socket with the paramenters
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

	// fill the fields in request and send
        strcpy(request.file_name, filename);
        fseek(file, 0L, SEEK_END);
        request.file_size = ftell(file);
        fseek(file, 0L, SEEK_SET);
        request.req_type = 1;
        send_data(namenode_socket, (void *)&request, sizeof(request));
	
	//TODO: receive the response
	dfs_cm_file_res_t response;
	receive_data(namenode_socket, (void *)&response, sizeof(response));


	//TODO: send the updated block to the proper datanode

	// Send blocks to datanodes one by one
        int i;
        int stop = response.query_result.blocknum;;
        for (i = 0; i < stop; i++)
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
}

int push_file(int namenode_socket, const char* local_path)
{

	int i, response_len = 0, len = 0;
	FILE *fp = NULL;
	char buffer[DFS_BLOCK_SIZE];
	int dn_sock_fd = 0;
	struct sockaddr_in block_dst;
	dfs_cm_client_req_t request;
	dfs_cm_file_res_t response;
	dfs_cli_dn_req_t push_blk_req;


	//TODO: assign values to the fields of req

	strcpy(request.file_name, local_path);
    request.req_type = 1;

    fp = fopen(local_path, "rb");
    fseek(fp, 0, SEEK_END);
    request.file_size = ftell(fp);
    fseek(fp, 0, SEEK_SET);


	//TODO: send request to namenode

	if (send(namenode_socket, &request, sizeof(dfs_cm_client_req_t), 0) == -1) 
	{
        	perror("ERROR sending data");
         	exit(1);
	}


    void *res_buffer = &response;

    while (1) {

        response_len = recv(namenode_socket, res_buffer, sizeof(dfs_cm_file_res_t), 0);

        res_buffer = res_buffer + response_len;
        len = len + response_len;

        if (len == sizeof(dfs_cm_file_res_t)) {
            break;
        }

    }

	//TODO: contact to datanode
	for (i = 0; i < response.query_result.blocknum; i++)
	{
		//TODO:send block to datanode

		block_dst.sin_family = AF_INET;
		block_dst.sin_addr.s_addr = inet_addr(response.query_result.block_list[i].loc_ip);
        block_dst.sin_port = htons(response.query_result.block_list[i].loc_port);

        fseek(fp, (i * DFS_BLOCK_SIZE), SEEK_SET);
        fread(buffer, 1, DFS_BLOCK_SIZE, fp);

		push_blk_req.op_type = 1;
		strcpy(push_blk_req.block.owner_name, response.query_result.block_list[i].owner_name);
		push_blk_req.block.block_id = i;
		strcpy(push_blk_req.block.loc_ip, response.query_result.block_list[i].loc_ip);
		push_blk_req.block.loc_port = response.query_result.block_list[i].loc_port;




	        if ((dn_sock_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) 
	        {
	        	perror("socket error");
	         	exit(1);
	        }

	        if (connect(dn_sock_fd, (struct sockaddr *) &block_dst, sizeof(block_dst)) == -1) 
	        {
	        	perror("connect error");
	         	exit(1);
	        }

	        int count;
			for (count = 0; count < DFS_BLOCK_SIZE; count++) 
			{
				push_blk_req.block.content[count] = buffer[count];
			}

	        if (send(dn_sock_fd, &push_blk_req, sizeof(dfs_cli_dn_req_t), 0) == -1) 
	        {
	        	perror("sending error");
	         	exit(1);
	        }

	        close(dn_sock_fd);

		}

		fclose(fp);

		return 0;
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

	int i;
	int stop = response.query_result.blocknum;
	for (i = 0; i < stop; ++i)
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
		memcpy(&(blocks[dnresponse.block_id]), &dnresponse, sizeof(dfs_cm_block_t));
	}

	FILE *file = fopen(filename, "wb");
	// resemble the received blocks into the complete file
	for (i = 0; i < stop; ++i)
	{
		fwrite(&(blocks[i].content), DFS_BLOCK_SIZE, 1, file);
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
