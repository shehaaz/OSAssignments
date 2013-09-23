#include "mydisk.h"
#include <string.h>
#include <stdio.h>

FILE *thefile;     /* the file that stores all blocks */
int max_blocks;    /* max number of blocks given at initialization */
int disk_type;     /* disk type, 0 for HDD and 1 for SSD */
int cache_enabled = 0; /* is cache enabled? 0 no, 1 yes */

typedef struct cache_block {

	int block_id;
	FILE *cache_file;
	int dirty_block; // 1=Dirty Block

} cache_block;

//store the cache_block in a limited size queue. fetching using the block_id
//after fetching manipulate the cache_file

int mydisk_init(char const *file_name, int nblocks, int type)
{
	/* TODO: 1. use the proper mode to open the disk file
	 * 2. fill zeros 
	 */
	disk_type = type;
	max_blocks = nblocks;

	thefile=fopen(file_name,"w+");

	if(thefile != NULL){
		int i;
		for (i = 0; i < (nblocks*BLOCK_SIZE); ++i)
		{
			//'\0' is the integer value of "0"
			fprintf(thefile,
					"%d%d%d%d%d%d%d%d%d%d",
					'\0','\0','\0','\0','\0','\0','\0','\0','\0','\0'); //how big is integers in C? sizeof('/0') = 4 bytes
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

	free(thefile);
	thefile = NULL;
	fclose(thefile);
}

int mydisk_read_block(int block_id, void *buffer)
{
	//block_id should be positive and not sure if the buffer != NULL
	if(0 <= block_id <= max_blocks && buffer != NULL){
		if (cache_enabled) {
			/* TODO: 1. check if the block is cached
			 * 2. if not create a new entry for the block and read from disk
			 * 3. fill the requested buffer with the data in the entry
			 * 4. return proper return code
			 */
			return 0;
		} else {
			/* TODO: use standard C function to read from disk
			 */
			//Buffer is guaranteed to have BLOCK_SIZE bytes; is this real Bytes or the bytes in the FILE?
			//e.g: Max_blocks=5 & block_size = 2:
			//you will be reading 16 objects of size 4 Bytes each
			fseek(thefile,block_id*BLOCK_SIZE*8,SEEK_SET);
			fread(buffer,4,sizeof(buffer),thefile);
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
	//how do you mark a block as dirty? should blocks be structs?

	if(0 <= block_id <= max_blocks && buffer != NULL){
		fseek(thefile,block_id*BLOCK_SIZE*8,SEEK_SET);
		fwrite(buffer,4,sizeof(buffer),thefile);
		return 0;
	}
	else{
		return 1;
	}
}

int mydisk_read(int start_address, int nbytes, void *buffer)
{
	int offset, remaining, amount, block_id;
	int cache_hit = 0, cache_miss = 0;

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

	//looks like we have to use structs
	int start_block_id, stop_block_id;

	start_block_id = start_address/BLOCK_SIZE;

	stop_block_id = (start_address + nbytes)/BLOCK_SIZE;

	char temp[(stop_block_id-start_block_id+1) * BLOCK_SIZE];

	int i;

	for(i=start_block_id; i<= stop_block_id; i++){
		mydisk_read_block(i,temp); //read block into temp
	}
	memcpy(buffer,&temp[start_address%BLOCK_SIZE],nbytes);
//	if(start_address % BLOCK_SIZE  == 0){
//		mydisk_read_block(start_address,buffer);
//	}
//	else{
//		char temp[BLOCK_SIZE];
//		mydisk_read_block(start_address/BLOCK_SIZE,temp);
//		memcpy(buffer,&temp[start_address],nbytes);
//	}

	return 0;
}

int mydisk_write(int start_address, int nbytes, void *buffer)
{
	/* TODO: similar to read, except the partial write problem
	 * When a block is modified partially, you need to first read the block,
	 * modify the portion and then write the whole block back
	 */

	int start_block_id, stop_block_id;

	start_block_id = start_address/BLOCK_SIZE;
	stop_block_id = (start_address + nbytes)/BLOCK_SIZE;

	char temp_reader[start_address%BLOCK_SIZE];
	mydisk_read_block(start_block_id,temp_reader);

	char result[sizeof(temp_reader)+nbytes];

	strcat(result,temp_reader);
	strcat(result, buffer);

	mydisk_write_block(start_block_id,result);

	return 0;
}
