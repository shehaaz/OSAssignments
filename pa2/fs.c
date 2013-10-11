#include "fs.h"
#include "ext.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* constant of how many bits in one freemap entry */
#define SFS_NBITS_IN_FREEMAP_ENTRY (sizeof(u32)*8)

/* in-memory superblock (in consistent with the disk copy) */
static sfs_superblock_t sb;
/* freemap, u32 array (in consistent with the disk copy) */
static u32 *freemap;
/* file descriptor table */
static fd_struct_t fdtable[SFS_MAX_OPENED_FILES];


/* 
 * Flush the in-memory freemap to disk 
 */
static void sfs_flush_freemap()
{
	size_t i;
	blkid bid = 1;
	char *p = (char *)freemap;
	/* TODO: write freemap block one by one */

	//write freemap to Block_ID 1 on HD
	sfs_write_block(&freemap,bid);
}

/* 
 * Allocate a free block, mark it in the freemap and flush the freemap to disk
 */
static blkid sfs_alloc_block()
{
	u32 size = sb.nfreemap_blocks * BLOCK_SIZE / sizeof(u32);	
	u32 i;
	int freemap_bit,offset,block_id;
	/* TODO: find a freemap entry that has a free block */

	/* TODO: find out which bit in the entry is zero,
	   set the bit, flush and return the bid
	 */

	for(freemap_bit=0;freemap_bit<size;freemap_bit++){
		u32 temp = freemap[freemap_bit];
		if(temp != 0xFFFFFFFF){
			/*
			break when freemap doesn't contain all 1's.
			Therefore, it must contain a zero.
			 */
			break; 
		}
	}  

	for(i=0;i<32;i++){
		u32 temp = (freemap[freemap_bit] & (0x00000001 << i));
		if(temp == 0x00000000){
			/*
			Iterate through the freemap and stop when a zero bit is found
			 */
			offset = i;
			break;
		}
	}

	//Calculate the block_id  considering the offset
	block_id = (freemap_bit*SFS_NBITS_IN_FREEMAP_ENTRY) + offset;

	//set the freemap_bit in the freemap
	freemap[freemap_bit] = (freemap[freemap_bit] | (0x00000001 << offset));
	//flush the new freemap to the HD
	sfs_flush_freemap();

	return block_id;
}

/*
 * Free a block, unmark it in the freemap and flush
 */
static void sfs_free_block(blkid bid)
{
	/* TODO find the entry and bit that correspond to the block */
	int entry_loc;
	int bit_loc;

	//Find freemap entry
	entry_loc = bid/SFS_NBITS_IN_FREEMAP_ENTRY;
	//Find the bit that corresponds to the block
	bit_loc = bid % SFS_NBITS_IN_FREEMAP_ENTRY;

	/* TODO unset the bit and flush the freemap */

	//Unset bit
	freemap[entry_loc] = (freemap[entry_loc] & ~(0x00000001 << bit_loc));

	//flush freemap to HD
	sfs_flush_freemap();
}

/* 
 * Resize a file.
 * This file should be opened (in the file descriptor table). The new size
 * should be larger than the old one (not supposed to shrink a file)
 */
static void sfs_resize_file(int fd, u32 new_size)
{
	/* the length of content that can be hold by a full frame (in bytes) */
	int frame_size = BLOCK_SIZE * SFS_FRAME_COUNT;
	/* old file size */
	int old_size = fdtable[fd].inode.size;
	/* how many frames are used before resizing */
	int old_nframe = (old_size + frame_size -1) / frame_size;
	/* how many frames are required after resizing */
	int new_nframe = (new_size + frame_size - 1) / frame_size;
	int i, j;
	blkid frame_bid = 0;
	sfs_inode_frame_t frame;

	/* TODO: check if new frames are required */

	/* TODO: allocate a full frame */

	/* TODO: add the new frame to the inode frame list
	   Note that if the inode is changed, you need to write it to the disk
	 */
}

/*
 * Get the bids of content blocks that hold the file content starting from cur
 * to cur+length. These bids are stored in the given array.
 * The caller of this function is supposed to allocate the memory for this
 * array. It is guaranteed that cur+length<size
 * 
 * This function returns the number of bids being stored to the array.
 */
static u32 sfs_get_file_content(blkid *bids, int fd, u32 cur, u32 length)
{
	/* the starting block of the content */
	u32 start;
	/* the ending block of the content */
	u32 end;
	u32 i;
	sfs_inode_frame_t frame;

	/* TODO: find blocks between start and end.
	   Transverse the frame list if needed
	 */
	return 0;
}

/*
 * Find the directory of the given name.
 *
 * Return block id for the directory or zero if not found
 */
static blkid sfs_find_dir(char *dirname)
{
	blkid curr_bid = 0;
	sfs_dirblock_t dir;
	/* TODO: start from the sb.first_dir, treverse the linked list */

	curr_bid = sb.first_dir;


	while(curr_bid != 0){

		sfs_read_block(&dir,curr_bid);

		if(strcmp(dir.dir_name,dirname) == 0){
			return curr_bid;
		}
		curr_bid = dir.next_dir;
		sfs_read_block(&dir,curr_bid);
	}

	return 0;
}

/*
 * Create a SFS with one superblock, one freemap block and 1022 data blocks
 *
 * The freemap is initialized be 0x3(11b), meaning that
 * the first two blocks are used (sb and the freemap block).
 *
 * This function always returns zero on success.
 */
int sfs_mkfs()
{
	/* one block in-memory space for freemap (avoid malloc) */
	static char freemap_space[BLOCK_SIZE];
	int i;
	sb.magic = SFS_MAGIC;
	sb.nblocks = 1024;
	sb.nfreemap_blocks = 1;
	sb.first_dir = 0;
	for (i = 0; i < SFS_MAX_OPENED_FILES; ++i) {
		/* no opened files */
		fdtable[i].valid = 0;
	}
	sfs_write_block(&sb, 0);
	freemap = (u32 *)freemap_space;
	memset(freemap, 0, BLOCK_SIZE);
	/* just to enlarge the whole file */
	sfs_write_block(freemap, sb.nblocks);
	/* initializing freemap */
	freemap[0] = 0x3; /* 11b, freemap block and sb used*/
	sfs_write_block(freemap, 1);
	memset(&sb, 0, BLOCK_SIZE);
	return 0;
}

/*
 * Load the super block from disk and print the parameters inside
 */
sfs_superblock_t *sfs_print_info()
{
	/* TODO: load the superblock from disk and print*/

	/*Argument 1: Pass the address of superblock "sb" as buffer to store the data fetched
	  Argument 2: The superblock is at index 0*/
	sfs_read_block(&sb,0);

	return &sb;
}

/*
 * Create a new directory and return 0 on success.
 * If the dir already exists, return -1.
 */
int sfs_mkdir(char *dirname)
{
	/* TODO: test if the dir exists */
	/* TODO: insert a new dir to the linked list */

	sfs_dirblock_t new_dir;
	blkid first_bid, new_dir_bid;

	/*
	Didn't find the directory. 
	Create and add new directory to linked list 
	 */
	if(sfs_find_dir(dirname) == 0){

		//allocated a block and get a blkid for the new directory
		new_dir_bid = sfs_alloc_block();
		//set new directory name
		strcpy(new_dir.dir_name,dirname);
		//Make the first inode have blkid zero to show that it is a new Directory
		new_dir.inodes[0] = 0;

		/* Using the append new directory in front of Linked List technique */
		//Make new_dir point to first dir
		new_dir.next_dir = sb.first_dir;
		//make sb point to new_dir
		sb.first_dir = new_dir_bid;

		//write sb to HD
		sfs_write_block(&sb,0);

		//write new directory to HD at new_dir_bid
		sfs_write_block(&new_dir,new_dir_bid);

		return 0;
	}
	//If the directory already exists return -1
	return -1;
}

/*
 * Remove an existing empty directory and return 0 on success.
 * If the dir does not exist or still contains files, return -1.
 */
int sfs_rmdir(char *dirname)
{
	/* TODO: check if the dir exists */
	/* TODO: check if no files */
	/* TODO: go thru the linked list and delete the dir*/

	if(sfs_find_dir(dirname) != 0){

		blkid curr_bid = 0;
		blkid prev_bid = 0;
		sfs_dirblock_t curr_dir, prev_dir;

		curr_bid = sb.first_dir;

		while(curr_bid != 0){

			sfs_read_block(&curr_dir,curr_bid);

			blkid first_inode_blkid = curr_dir.inodes[0];

			if((strcmp(curr_dir.dir_name,dirname) == 0) && (first_inode_blkid == 0)){

				//Case: Removing first Directory. Modify sb
				if(curr_bid == sb.first_dir){
					sb.first_dir = curr_dir.next_dir;
					sfs_write_block(&sb,0);
					sfs_free_block(curr_bid);
					return 0;
				}else{ //Case: Any other Directory
					prev_dir.next_dir = curr_dir.next_dir;
					sfs_write_block(&prev_dir,prev_bid);
					sfs_free_block(curr_bid);
					return 0;
				}
			}
			//Save Previous Directory Information
			prev_bid = curr_bid;
			prev_dir = curr_dir;
			//Set Next Directory
			curr_bid = curr_dir.next_dir;
		}
	}
	return -1;
}

/*
 * Print all directories. Return the number of directories.
 */
int sfs_lsdir()
{
	sfs_dirblock_t temp_dir;
	blkid check_bid;
	check_bid = sb.first_dir;
	int counter = 0;

	/* go thru the linked list */
	while(check_bid != 0){
		sfs_read_block(&temp_dir,check_bid);
		check_bid = temp_dir.next_dir;
		counter++;
	}

	return counter;
}

/*
 * Open a file. If it does not exist, create a new one.
 * Allocate a file desriptor for the opened file and return the fd.
 */
int sfs_open(char *dirname, char *name)
{
	blkid dir_bid = 0, inode_bid = 0;
	sfs_inode_t *inode;
	sfs_dirblock_t dir;
	int fd;
	int i;

	/* TODO: find a free fd number */

	/* TODO: find the dir first */

	/* TODO: traverse the inodes to see if the file exists.
	   If it exists, load its inode. Otherwise, create a new file.
	 */

	/* TODO: create a new file */
	return fd;
}

/*
 * Close a file. Just mark the valid field to be zero.
 */
int sfs_close(int fd)
{
	/* TODO: mark the valid field */
	return 0;
}

/*
 * Remove/delete an existing file
 *
 * This function returns zero on success.
 */
int sfs_remove(int fd)
{
	blkid frame_bid;
	sfs_dirblock_t dir;
	int i;

	/* TODO: update dir */

	/* TODO: free inode and all its frames */

	/* TODO: close the file */
	return 0;
}

/*
 * List all the files in all directories. Return the number of files.
 */
int sfs_ls()
{
	/* TODO: nested loop: traverse all dirs and all containing files*/
	return 0;
}

/*
 * Write to a file. This function can potentially enlarge the file if the 
 * cur+length exceeds the size of file. Also you should be aware that the
 * cur may already be larger than the size (due to sfs_seek). In such
 * case, you will need to expand the file as well.
 * 
 * This function returns number of bytes written.
 */
int sfs_write(int fd, void *buf, int length)
{
	int remaining, offset, to_copy;
	blkid *bids;
	int i, n;
	char *p = (char *)buf;
	char tmp[BLOCK_SIZE];
	u32 cur = fdtable[fd].cur;

	/* TODO: check if we need to resize */

	/* TODO: get the block ids of all contents (using sfs_get_file_content() */

	/* TODO: main loop, go through every block, copy the necessary parts
	   to the buffer, consult the hint in the document. Do not forget to 
	   flush to the disk.
	 */
	/* TODO: update the cursor and free the temp buffer
	   for sfs_get_file_content()
	 */
	return 0;
}

/*
 * Read from an opend file. 
 * Read can not enlarge file. So you should not read outside the size of 
 * the file. If the read exceeds the file size, its result will be truncated.
 *
 * This function returns the number of bytes read.
 */
int sfs_read(int fd, void *buf, int length)
{
	int remaining, to_copy, offset;
	blkid *bids;
	int i, n;
	char *p = (char *)buf;
	char tmp[BLOCK_SIZE];
	u32 cur = fdtable[fd].cur;

	/* TODO: check if we need to truncate */
	/* TODO: similar to the sfs_write() */
	return 0;
}

/* 
 * Seek inside the file.
 * Loc is the starting point of the seek, which can be:
 * - SFS_SEEK_SET represents the beginning of the file.
 * - SFS_SEEK_CUR represents the current cursor.
 * - SFS_SEEK_END represents the end of the file.
 * Relative tells whether to seek forwards (positive) or backwards (negative).
 * 
 * This function returns 0 on success.
 */
int sfs_seek(int fd, int relative, int loc)
{
	/* TODO: get the old cursor, change it as specified by the parameters */
	return 0;
}

/*
 * Check if we reach the EOF(end-of-file).
 * 
 * This function returns 1 if it is EOF, otherwise 0.
 */
int sfs_eof(int fd)
{
	/* TODO: check if the cursor has gone out of bound */
	return 0;
}
