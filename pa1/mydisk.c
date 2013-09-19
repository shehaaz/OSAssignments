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

	thefile=fopen(file_name,"w");

	if(thefile != NULL){
		int i;
		for (i = 0; i < (nblocks*BLOCK_SIZE); ++i)
		{
			//'\0' is the integer value of "0"
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

	free(thefile);
	thefile = NULL;
	fclose(thefile);
}

int mydisk_read_block(int block_id, void *buffer)
{
	//block_id should be positive and not sure if the buffer != NULL
	if(0 < block_id <= max_blocks && buffer != NULL){
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
			fseek(thefile,block_id*BLOCK_SIZE*8,SEEK_SET);
			fread(buffer,1,BLOCK_SIZE*8,thefile);
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
	if(block_id > 0 && buffer != NULL){
		fseek(thefile,block_id*BLOCK_SIZE*8,SEEK_SET);
		fwrite(buffer,1,BLOCK_SIZE*8,thefile);
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


	return 0;
}

int mydisk_write(int start_address, int nbytes, void *buffer)
{
	/* TODO: similar to read, except the partial write problem
	 * When a block is modified partially, you need to first read the block,
	 * modify the portion and then write the whole block back
	 */
	return 0;
}
