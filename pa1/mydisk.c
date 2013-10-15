#include "mydisk.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

FILE *thefile;     /* the file that stores all blocks */
int max_blocks;    /* max number of blocks given at initialization */
int disk_type;     /* disk type, 0 for HDD and 1 for SSD */
int cache_enabled = 0; /* is cache enabled? 0 no, 1 yes */
int Disk_Latency = 0;


//store the cache_block in a limited size queue. fetching using the block_id
//after fetching manipulate the cache_file

int mydisk_init(char const *file_name, int nblocks, int type)
{
	/* TODO: 1. use the proper mode to open the disk file
	 * 2. fill zeros 
	 */
	disk_type = type;
	max_blocks = nblocks;

	if(!(disk_type==0 || disk_type==1)){

		printf("The disk type is not valid");
		return 1;
	}

	thefile=fopen(file_name,"w+");

	if(thefile != NULL){
		int i;
		for (i = 0; i < (nblocks*BLOCK_SIZE); ++i)
		{
			fprintf(thefile,
					"%d%d%d%d%d%d%d%d%d%d",
					'\0','\0','\0','\0','\0','\0','\0','\0','\0','\0');
		}

		return 0;
	}
	else{
		return 1;
	}
}

void mydisk_close()
{
	/* TODO: clean up whatever done in mydisk_init()*/

	fclose(thefile);
	thefile = NULL;
}

int mydisk_read_block(int block_id, void *buffer)
{

	if(0 <= block_id <= max_blocks && buffer != NULL){
		if (cache_enabled) {
			/* TODO: 1. check if the block is cached
			 * 2. if not create a new entry for the block and read from disk
			 * 3. fill the requested buffer with the data in the entry
			 * 4. return proper return code
			 */
			if (get_cached_block(block_id) != NULL){
				buffer = get_cached_block(block_id);
			}else{
				create_cached_block(block_id);
				fseek(thefile,block_id*BLOCK_SIZE,SEEK_SET);
				fread(buffer,1,BLOCK_SIZE,thefile);
			}
			return 0;
		} else {
			/* TODO: use standard C function to read from disk
			 */
			fseek(thefile,block_id*BLOCK_SIZE,SEEK_SET);
			fread(buffer,1,BLOCK_SIZE,thefile);
			return 0;
		}
	}
	else {
		return 1;
	}
}

int mydisk_write_block(int block_id, void *buffer)
{
	/* TODO: this one is similar to read_block() except that
	 * you need to mark it dirty
	 */


	if(0 <= block_id <= max_blocks && buffer != NULL){
		if (cache_enabled) {
			/* TODO: 1. check if the block is cached
			 * 2. if not create a new entry for the block and read from disk
			 * 3. fill the requested buffer with the data in the entry
			 * 4. return proper return code
			 */
			if (get_cached_block(block_id) != NULL){
				mark_dirty(block_id);
			}else{
				create_cached_block(block_id);
				fseek(thefile,block_id*BLOCK_SIZE,SEEK_SET);
				fwrite(buffer,1,BLOCK_SIZE,thefile);
			}
			return 0;
		} else {
			fseek(thefile,block_id*BLOCK_SIZE,SEEK_SET);
			fwrite(buffer,1,BLOCK_SIZE,thefile);
			return 0;
		}
	}
	else{
		return 1;
	}
}

int mydisk_read(int start_address, int nbytes, void *buffer)
{
	int jump_BLOCK_SIZE,start_block_id, stop_block_id, amount_to_read;

	/* TODO: 1. first, always check the parameters
	 * 2. a loop which process one block each time
	 * 2.1 offset means the in-block offset
	 * amount means the number of bytes to be moved from the block
	 * (starting from offset)
	 * remaining means the remaining bytes before final completion
	 * 2.2 get one block, copy the proper portion
	 * 2.3 update offset, amount and remaining
	 * in terms of latency calculation, monitor if cache hit/miss
	 * for each block access
	 */

	if(start_address >= 0 && nbytes >= 0 && (start_address + nbytes) < (max_blocks*BLOCK_SIZE)){


		start_block_id = start_address/BLOCK_SIZE;

		stop_block_id = (start_address + nbytes)/BLOCK_SIZE;

		amount_to_read = (stop_block_id-start_block_id+1) * BLOCK_SIZE;

		char temp_reader[amount_to_read];

		int i;


		jump_BLOCK_SIZE = 0;
		for(i=start_block_id; i<= stop_block_id; i++){
			/*
			 * Read a BLOCK and write to temp_reader
			 * Jump BLOCK_SIZE over the data written to temp_reader
			 * Reads all BLOCKS from start_block_id to stop_block_id
			 * */
			mydisk_read_block(i,temp_reader + jump_BLOCK_SIZE);
			jump_BLOCK_SIZE += BLOCK_SIZE;
		}

		//Only write to buffer the data FROM start_address for nbytes
		memcpy(buffer,&temp_reader[start_address%BLOCK_SIZE],nbytes);

		//calculate latency
		if(disk_type == 0){
			report_latency(HDD_SEEK+max_blocks*HDD_READ_LATENCY);
		}else{
			report_latency(max_blocks*SSD_READ_LATENCY);
		}

		return 0;
	}else{
		return 1;
	}
}

int mydisk_write(int start_address, int nbytes, void *buffer)
{
	/* TODO: similar to read, except the partial write problem
	 * When a block is modified partially, you need to first read the block,
	 * modify the portion and then write the whole block back
	 */

	int amount_to_read,start_block_id, stop_block_id, jump_BLOCK_SIZE;

	if(start_address >= 0 && nbytes >= 0 && (start_address + nbytes) < (max_blocks*BLOCK_SIZE)){

		start_block_id = start_address/BLOCK_SIZE;
		stop_block_id = (start_address + nbytes)/BLOCK_SIZE;

		char temp_reader[(stop_block_id-start_block_id+1) * BLOCK_SIZE];

		jump_BLOCK_SIZE = 0;
		int i;
		for(i=start_block_id; i<= stop_block_id; i++){
			mydisk_read_block(i,temp_reader + jump_BLOCK_SIZE);
			jump_BLOCK_SIZE += BLOCK_SIZE;
		}


		//copy the buffer exactly into its spot in temp_reader for nbytes
		memcpy(temp_reader + start_address%BLOCK_SIZE,buffer,nbytes);

		/*
		 * Same as mydisk_read, but this time it is writing and then jumping BLOCK_SIZE
		 * */
		jump_BLOCK_SIZE = 0;
		for(i=start_block_id; i<= stop_block_id; i++){
			mydisk_write_block(i,temp_reader+jump_BLOCK_SIZE);
			jump_BLOCK_SIZE += BLOCK_SIZE;
		}

		//calculate latency
		if(disk_type == 0){
			report_latency(HDD_SEEK+max_blocks*HDD_WRITE_LATENCY);
		}else{
			report_latency(max_blocks*SSD_WRITE_LATENCY);
		}

		return 0;
	}else{
		return 1;
	}

}
