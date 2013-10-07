#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mydisk.h"

/* The cache entry struct */
typedef struct cache_entry
{
	int block_id;
	int is_dirty;
	void *content;
	struct cache_entry *next;
}cache_entry;

cache_entry *HeadCacheBlock;
int num_cache_blocks;

/* TODO: some helper functions e.g. find_cached_entry(block_id) */

int init_cache(int nblocks)
{
	/* TODO: allocate proper data structure (ring buffer)
	 * initialize entry data so that the the ring buffer is empty
	 */
	//use circular array showed in class
	num_cache_blocks = nblocks;
	printf("init_cache");

	if (nblocks == 0){
		printf("\nNo Cache Blocks specified");
		return -1;}

	HeadCacheBlock = malloc(sizeof(cache_entry));
	HeadCacheBlock->next = NULL;



	return 0;
}

int close_cache()
{
	/* TODO: release the memory for the ring buffer */

	//Loop through and send data in the cache to the HDD

	//Loop and free the memory


	cache_entry *temp1;
	cache_entry *temp2;
	temp1 = HeadCacheBlock;

	while(temp1->next != NULL){
		if (temp1->is_dirty == 1){
			fseek(thefile,temp1->block_id*BLOCK_SIZE,SEEK_SET);
			fwrite(temp1->content,1,BLOCK_SIZE,thefile);
			temp1->is_dirty = 0;
		}
		temp1 = temp1->next;
	}


	temp2 = HeadCacheBlock;
	while(temp2 != NULL){
		temp1 = temp2;
		temp2 = temp1->next;
		free(temp1);
	}


	return 0;
}

void *get_cached_block(int block_id)
{
	/* TODO: find the entry, return the content 
	 * or return NULL if nut found 
	 */

	//Loop find the Block_id and return the content

	cache_entry *temp;
	temp=HeadCacheBlock;

	while(temp->next != NULL){
		if (temp->block_id==block_id) {
			return temp->content;
		} else {
			temp =  temp->next;
		}
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

	if(cache_length() == num_cache_blocks){
		remove_first();
		add_last(block_id);
	}else{
		add_last(block_id);
	}

	return 0;
}

void add_last(int block_id){

	cache_entry *new_entry;
	new_entry = malloc(sizeof(cache_entry));
	new_entry->block_id = block_id;
	new_entry->content = cache_buffer;
	new_entry->is_dirty = 0;

	cache_entry *temp;

	temp = HeadCacheBlock;

	while(temp->next != NULL){
		temp = temp->next;
	}

	new_entry->next = NULL;
	temp->next = new_entry;

}

void remove_first(){

	cache_entry *new_first;

	new_first->next = HeadCacheBlock->next->next;
	if(HeadCacheBlock->next->is_dirty == 1){
		fseek(thefile,HeadCacheBlock->next->block_id*BLOCK_SIZE,SEEK_SET);
		fwrite(HeadCacheBlock->next->content,1,BLOCK_SIZE,thefile);
	}
	free(HeadCacheBlock->next);
	HeadCacheBlock->next = new_first;
}

int cache_length(){
	cache_entry *temp;
	int count = -1; //start at -1 to account for the HeadCacheBlock

	temp = HeadCacheBlock;

	while(temp != NULL){
		temp = temp->next;
		count++;
	}
	return count;
}

void mark_dirty (int block_id)
{
	cache_entry *temp;
	temp=HeadCacheBlock;

	while(temp->next != NULL){
		if (temp->block_id == block_id){
			temp->is_dirty = 1;
			temp->content = cache_buffer;
		}
		temp = temp->next;
	}
}
