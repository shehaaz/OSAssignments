#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mydisk.h"

/* The cache entry struct */
typedef struct cache_entry
{
	int block_id;
	int is_dirty;
	char content[BLOCK_SIZE];
	struct cache_entry *next;
}cache_entry;

int cache_blocks;  /* number of blocks for the cache buffer */
cache_entry *HeadCacheBlock;
cache_entry *Kick;
int counter =0;

/* TODO: some helper functions e.g. find_cached_entry(block_id) */

int init_cache(int nblocks)
{
	/* TODO: allocate proper data structure (ring buffer)
	 * initialize entry data so that the the ring buffer is empty
	 */
	//use circular array showed in class
	cache_entry *current;
	cache_entry *tail;
	//CACHE_BLOCKS = cache_size_in_blocks;
	if (nblocks == 0){
		printf("\nNo Cache Blocks specified");
		return -1;}
	int i;
	for(i=0;i<nblocks;i++)
	{
		current = malloc(sizeof(cache_entry));
		current -> block_id = -1;
		current -> is_dirty = 0;
		//current->content = malloc(sizeof(char)*BLOCK_SIZE);
		//Implementing a circular queue for the Cache
		if(i==0){
			HeadCacheBlock = current;
			tail = current;
		}
		else{
			current->next = HeadCacheBlock;
			HeadCacheBlock = current;
		}
	}
	//Joining the ends of the queue together
	if (nblocks != 0)
		tail->next = HeadCacheBlock;
	//printf("\nCache successfully initialized");
	return 0;
}

int close_cache()
{
	/* TODO: release the memory for the ring buffer */
	cache_entry *current;
	cache_entry *a;
	current = HeadCacheBlock;
	int i;
	for(i = 0;i<cache_blocks;i++){
		if (current->is_dirty == 1){
			fseek(thefile,current->block_id*BLOCK_SIZE,SEEK_SET);
			fwrite(current->content,1,BLOCK_SIZE,thefile);
			current->is_dirty = 0;
		}
		current = current->next;

	}


	a = HeadCacheBlock;
	for(i =0; i <= cache_blocks;i++){
		current = a;
		a=current->next;
		// free(current);
	}


	// free memory
	return 0;
}

void *get_cached_block(int block_id)
{
	/* TODO: find the entry, return the content 
	 * or return NULL if nut found 
	 */

	cache_entry *current;
	current=HeadCacheBlock;
	int i;
	for(i = 0;i<cache_blocks;i++){
		if (current->block_id==block_id) {
			return current;
		}
		current =  current->next;
	}

	return NULL;

}

void *create_cached_block(int block_id)
{
	/* TODO: create a new entry, insert it into the ring buffer
	 * It might kick an exisitng entry.
	 * Remember to write dirty block back to disk
	 * Note that: think if you can use mydisk_write_block() to 
	 * flush dirty blocks to disk\
	 */
	counter = counter % 4;
	int i;
	cache_entry *current;
	current=HeadCacheBlock;
	for(i =0; i < counter; i++){
		current->next;
		//if(cache_blocks<4) cache_blocks++;
	}
	// Modify the current block
	current->block_id = block_id;
	current->is_dirty=0;
	// Have to modify contents?? verify
	// next block is already set from init
	counter ++;
	return current;
}

void mark_dirty (int block_id)
{
	cache_entry *current;
	current=HeadCacheBlock;
	int i;
	for(i = 0;i< cache_blocks;i++){
		if (current->block_id == block_id){
			current->is_dirty = 1;
		}
		current = current->next;
	}
}
