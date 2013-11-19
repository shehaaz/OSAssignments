#include "namenode/dfs_namenode.h"
#include <assert.h>
#include <unistd.h>

dfs_datanode_t* dnlist[MAX_DATANODE_NUM];
dfs_cm_file_t* file_images[MAX_FILE_COUNT];
int fileCount;
int dncnt; //datanode count
int safeMode = 1;

int mainLoop(int server_socket)
{
	while (safeMode == 1)
	{
		printf("the namenode is running in safe mode\n");
		sleep(5);
	}

	for (;;)
	{
		sockaddr_in client_address;
		unsigned int client_address_length = sizeof(client_address);
		int client_socket = -1;
		//TODO: accept the connection from the client and assign the return value to client_socket
		client_socket = accept(server_socket, (struct sockaddr *) &client_address, &client_address_length);

		assert(client_socket != INVALID_SOCKET);


		//TODO: receive requests from client and fill it in request

		/* fetch data via client_socket once the connection
		has been established. */
		if(client_socket != -1){
			dfs_cm_client_req_t request;
			receive_data(client_socket,(void *) &request, sizeof(dfs_cm_client_req_t));
			requests_dispatcher(client_socket, request);
		}
		close(client_socket);
	}
	return 0;
}

static void *heartbeatService()
{
	int socket_handle = create_server_tcp_socket(50030);
	register_datanode(socket_handle);
	close(socket_handle);
	return 0;
}


/**
 * start the service of namenode
 * argc - count of parameters
 * argv - parameters
 */
int start(int argc, char **argv)
{
	assert(argc == 2);

	int i = 0;
	for (i = 0; i < MAX_DATANODE_NUM; i++) dnlist[i] = NULL;
	for (i = 0; i < MAX_FILE_COUNT; i++) file_images[i] = NULL;

	//TODO:create a thread to handle heartbeat service
	//you can implement the related function in dfs_common.c and call it here

	create_thread(heartbeatService,NULL); //function in dfs_common.c

	//TODO: create a socket to listen to the client requests and replace the value of server_socket with the socket's fd
	int server_socket = INVALID_SOCKET;
	server_socket =  create_server_tcp_socket(atoi(argv[1]));

	assert(server_socket != INVALID_SOCKET);

	return mainLoop(server_socket);
}

int register_datanode(int heartbeat_socket)
{
	for (;;)
	{
		int datanode_socket = -1;
		sockaddr_in sock_addr;
		int sock_len = sizeof(struct sockaddr_in);

		//TODO: accept connection from DataNodes and assign return value to datanode_socket;
		datanode_socket = accept(heartbeat_socket, (struct sockaddr *) &sock_addr, &sock_len);
		assert(datanode_socket != INVALID_SOCKET);

		dfs_cm_datanode_status_t datanode_status;

		//receive datanode's status via datanode_socket
		receive_data(datanode_socket,(void *) &datanode_status, sizeof(dfs_cm_datanode_status_t));

		if (datanode_status.datanode_id < MAX_DATANODE_NUM)
		{
			//TODO: fill dnlist
			//principle: a datanode with id of n should be filled in dnlist[n - 1] (n is always larger than 0)
			int datanode_id = datanode_status.datanode_id;

			if(dnlist[datanode_id - 1] == NULL)
			{
				//Making room for a datanode
				dnlist[datanode_id - 1] = (dfs_datanode_t *) malloc(sizeof(dfs_datanode_t));
				dncnt++; //increment the datanode count
			}

			//filling in the information for the datanode
			dnlist[datanode_id - 1]->live_marks++;
			strcpy(dnlist[datanode_id-1]->ip, inet_ntoa(sock_addr.sin_addr));
			dnlist[datanode_id - 1]->port = datanode_status.datanode_listen_port;

			safeMode = 0;
		}
		close(datanode_socket);
	}
	return 0;
}

int get_file_receivers(int client_socket, dfs_cm_client_req_t request)
{
    printf("Responding to request for block assignment of file '%s'!\n", request.file_name);

    dfs_cm_file_t** end_file_image = file_images + MAX_FILE_COUNT;
    dfs_cm_file_t** file_image = file_images;

    // Try to find if there is already an entry for that file
    while (file_image != end_file_image)
    {
            if (*file_image != NULL && strcmp((*file_image)->filename, request.file_name) == 0) break;
            ++file_image;
    }

    if (file_image == end_file_image)
    {
            // There is no entry for that file, find an empty location to create one
            file_image = file_images;
            while (file_image != end_file_image)
            {
                    if (*file_image == NULL) break;
                    ++file_image;
            }

            if (file_image == end_file_image) return 1;
            // Create the file entry
            *file_image = (dfs_cm_file_t*)malloc(sizeof(dfs_cm_file_t));
            memset(*file_image, 0, sizeof(*file_image));
            strcpy((*file_image)->filename, request.file_name);
            (*file_image)->file_size = request.file_size;
            (*file_image)->blocknum = 0;
    }

    int block_count = (request.file_size + (DFS_BLOCK_SIZE - 1)) / DFS_BLOCK_SIZE;

    int first_unassigned_block_index = (*file_image)->blocknum;
    (*file_image)->blocknum = block_count;
    int next_data_node_index = 0;
    int stop =  block_count + first_unassigned_block_index;

    //TODO:Assign data blocks to datanodes, round-robin style (see the Documents)

	dfs_datanode_t *data_node;
	dfs_cm_block_t *block;
    int index;
	for (index = first_unassigned_block_index; index < stop; index++)
	{

		data_node = dnlist[next_data_node_index];
		assert(data_node != NULL);

		/*
		 * Set the data block to the file descriptor's memory location
		 * We will modify this memory location and memcpy it later into the response
		 * sent to the client */
		block = &((*file_image)->block_list[index]);

		//Populate the block
		strcpy(block->owner_name, request.file_name);
		strcpy(block->loc_ip, data_node->ip);
		block->dn_id = data_node->dn_id;
		block->block_id = index;
		block->loc_port = data_node->port;

		//Using the technique learnt in class to find the next free index
		next_data_node_index = (next_data_node_index + 1) % (dncnt);
	}

    dfs_cm_file_res_t response;
    memset(&response, 0, sizeof(response));
    //TODO: fill the response and send it back to the client

    //Filling in the data
    memcpy(&(response.query_result), *file_image, sizeof(dfs_cm_file_t));
    //sending the data to the client_socket
    send_data(client_socket, (void *) &response, sizeof(dfs_cm_file_res_t));
    return 0;
}

int get_file_location(int client_socket, dfs_cm_client_req_t request)
{

	int i = 0;
	int FileImgIndex;
	dfs_cm_file_res_t file_dsp;
	
	for (i = 0; i < MAX_FILE_COUNT; ++i)
	 {
	 	dfs_cm_file_t* file_image = file_images[i];
	 	if (file_image == NULL) continue;
	 	if (strcmp(file_image->filename, request.file_name) != 0) continue;

	 	dfs_cm_file_res_t response;
	 	//TODO: fill the response and send it back to the client

	 	//Fill response with zeros
		bzero((char *) &response, sizeof(response));
		//copy file into response
		memcpy(&(response.query_result), file_image, sizeof(dfs_cm_file_t));
		//send response to client_socket
		send_data(client_socket, (void *)&response, sizeof(dfs_cm_file_res_t));

	 	return 0;
	 }
	 //FILE NOT FOUND
	 return 1;
}

void get_system_information(int client_socket, dfs_cm_client_req_t request)
{
	assert(client_socket != INVALID_SOCKET);
	//TODO:fill the response and send back to the client
	dfs_system_status response;

	//set datanode_num to data_node count
	response.datanode_num = dncnt;
	//memory copy the data_node list into the response
	memcpy(response.datanodes, *dnlist, sizeof(response.datanodes));

	send_data(client_socket, (void *) &response, sizeof(dfs_system_status));
}

int get_file_update_point(int client_socket, dfs_cm_client_req_t request)
{
	int i = 0;
	for (i = 0; i < MAX_FILE_COUNT; ++i)
	{
		dfs_cm_file_t* file_image = file_images[i];
		if (file_image == NULL) continue;
		if (strcmp(file_image->filename, request.file_name) != 0) continue;
		dfs_cm_file_res_t response;
		//TODO: fill the response and send it back to the client
		// Send back the data block assignments to the client
		memset(&response, 0, sizeof(response));
		memcpy(&(response.query_result), file_image, sizeof(dfs_cm_file_t));
		//printf("Namenode: sending update point of %s\n", response.query_result.file_name);

		send_data(client_socket, (void *)&response, sizeof(dfs_cm_file_res_t));
		return 0;
	}
	//FILE NOT FOUND
	return 1;
}

int requests_dispatcher(int client_socket, dfs_cm_client_req_t request)
{
	//0 - read, 1 - write, 2 - query, 3 - modify
	switch (request.req_type)
	{
		case 0:
			get_file_location(client_socket, request);
			break;
		case 1:
			get_file_receivers(client_socket, request);
			break;
		case 2:
			get_system_information(client_socket, request);
			break;
		case 3:
			get_file_update_point(client_socket, request);
			break;
	}
	return 0;
}

int main(int argc, char **argv)
{
	int i = 0;
	for (; i < MAX_DATANODE_NUM; i++)
		dnlist[i] = NULL;
	return start(argc, argv);
}
