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
	/*write freemap block one by one */

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
	/* find a freemap entry that has a free block */

	/* find out which bit in the entry is zero,
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
	/* find the entry and bit that correspond to the block */
	int entry_loc;
	int bit_loc;

	//Find freemap entry
	entry_loc = bid/SFS_NBITS_IN_FREEMAP_ENTRY;
	//Find the bit that corresponds to the block
	bit_loc = bid % SFS_NBITS_IN_FREEMAP_ENTRY;

	/* unset the bit and flush the freemap */

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
	if(fdtable[fd].inode.first_frame != 0){
		fdtable[fd].inode.size = new_size;
		sfs_write_block(&fdtable[fd].inode, fdtable[fd].inode_bid);
	}
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
	/* the starting/ending block/frame of the content */
	u32 start_block, end_block, start_frame, end_frame;;
	/* the ending block of the content */
	int i, j, num_bids_stored, remaining, offset, to_copy;
	sfs_inode_frame_t frame;

	num_bids_stored = 0;

	start_block = cur / BLOCK_SIZE;
	end_block = (cur + length) / BLOCK_SIZE;
	start_frame = start_block / SFS_FRAME_COUNT;
	end_frame = end_block / SFS_FRAME_COUNT;

	//num of blocks to read
	remaining = (end_block - start_block) + 1;
	// offset of first block in the frame
	offset = start_block % SFS_FRAME_COUNT;

	//read first block
	sfs_read_block(&frame, fdtable[fd].inode.first_frame);

	//calculate to_copy with "offset"
	if (remaining < (SFS_FRAME_COUNT - offset)) {
		to_copy = remaining;
	} else {
		to_copy = (SFS_FRAME_COUNT - offset);
	}

	/* find blocks between start and end.
	   Transverse the frame list if needed
	 */

	for (i = 0; i <= (end_frame - start_frame); i++) {

		int counter = 0;

		//setup *bids
		for (j = 0; j < to_copy; j++) {
			*bids = frame.content[counter + offset];
			counter++;
			bids++;
		}

		//update
		num_bids_stored = num_bids_stored + to_copy;
	}

	return num_bids_stored;
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

	/* start from the sb.first_dir, traverse the linked list */

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
	/* load the superblock from disk and print*/

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
	/* test if the dir exists */
	/* insert a new dir to the linked list */

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

		//initialize the inodes to blkid zero
		int i;
		for (i = 0; i < SFS_DB_NINODES; ++i)
		{
			new_dir.inodes[i] = 0;
		}

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
	/* check if the dir exists */
	/* check if no files */
	/* go thru the linked list and delete the dir*/

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
	blkid dir_bid = 0, inode_bid = 0, frame_bid = 0;
	sfs_inode_t inode;
	sfs_dirblock_t dir;
	int free_fd_index, i, file_exists, free_inode_index;


	if(strlen(name) > SFS_MAX_FILENAME_LEN){
		//not sure what to return
		return -1;
	}
	else{
		/* find a free fd number */

		free_fd_index = 0;
		while (fdtable[free_fd_index].valid != 0) {
			free_fd_index = free_fd_index++;
		}

		/* find the dir first */
		int mkdir_result;
		mkdir_result = sfs_mkdir(dirname); //mkdir if it doesn't exist
		dir_bid = sfs_find_dir(dirname); //get the blkid
		sfs_read_block(&dir,dir_bid); //read content of the dir from HD

		/* traverse the inodes to see if the file exists.
	   	   If it exists, load its inode. Otherwise, create a new file.
		 */

		file_exists = 0;
		free_inode_index = 0;

		//IF Directory already exists
		if(mkdir_result == -1){
			for (i = 0; i < SFS_DB_NINODES; ++i)
			{
				if(dir.inodes[i] != 0){
					inode_bid = dir.inodes[i];
					sfs_read_block(&inode,inode_bid);
					if(strcmp(inode.file_name,name) == 0){
						file_exists = 1;
						break;
					}
				}else{
					free_inode_index = i;
				}
			}
		}

		/* case: file DNE or a new Directory */
		if(file_exists == 0){

			sfs_inode_frame_t new_frame;

			inode_bid = sfs_alloc_block();
			frame_bid = sfs_alloc_block();

			//Setup the new Inode
			inode.first_frame = frame_bid;
			inode.size = 0;
			strcpy(inode.file_name,name);

			//Initialize a fresh frame. set next and content to zero
			new_frame.next = 0;
			for (i = 0; i < SFS_FRAME_COUNT; i++) {
				//setting content block_ids to zero
				new_frame.content[i] = 0;
			}

			//update directory and write to HD
			dir.inodes[free_inode_index] = inode_bid;
			sfs_write_block(&dir,dir_bid);

			//write new node and frame to HD
			sfs_write_block(&inode,inode_bid);
			sfs_write_block(&new_frame,frame_bid);
		}

		//Update the "open" fdtable
		fdtable[free_fd_index].dir_bid = dir_bid;
		fdtable[free_fd_index].inode = inode;
		fdtable[free_fd_index].inode_bid = inode_bid;
		fdtable[free_fd_index].valid = 1;
		fdtable[free_fd_index].cur = 0;


		return free_fd_index;
	}
}

/*
 * Close a file. Just mark the valid field to be zero.
 */
int sfs_close(int fd)
{
	/*mark the valid field */
	fdtable[fd].valid = 0;
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
	fd_struct_t file_desc;
	sfs_inode_t inode;
	sfs_inode_frame_t frame;
	blkid dir_bid, inode_bid;

	//boundary check
	if(0 < fd < SFS_MAX_OPENED_FILES){

		/* 1- free inode and all its frames */
		/* 2- update dir */
		/* 3- close the file */

		file_desc = fdtable[fd];
		dir_bid = file_desc.dir_bid;
		inode_bid = file_desc.inode_bid;

		sfs_read_block(&dir,dir_bid);

		//loop backwards becuz inode_bids were stored starting from last free dir.inodes[i] index
		for (i = (SFS_DB_NINODES-1); i >= 0; --i)
		{
			if(dir.inodes[i] == inode_bid){

				//update directory
				dir.inodes[i] = 0;
				sfs_write_block(&dir,dir_bid);

				sfs_read_block(&inode,inode_bid);
				frame_bid = inode.first_frame;

				while(frame_bid != 0){
					sfs_read_block(&frame,frame_bid);
					sfs_free_block(frame_bid);
					frame_bid = frame.next;
				}
				sfs_free_block(inode_bid);
				break;
			}
		}
		sfs_close(fd);
		return 0;
	}else{
		return -1;
	}
}

/*
 * List all the files in all directories. Return the number of files.
 */
int sfs_ls()
{
	blkid currid;
	sfs_dirblock_t dir;
	int count = 0;

	/*nested loop: traverse all dirs and all containing files*/

	currid = sb.first_dir;

	while(currid != 0){
		sfs_read_block(&dir,currid);
		int i;
		for (i = 0; i < SFS_DB_NINODES; ++i)
		{
			if(dir.inodes[i] != 0){
				count++;
			}
		}
		currid = dir.next_dir;
	}

	return count;;
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
	blkid *bids = malloc(sizeof(*bids) * ((length - 1) / BLOCK_SIZE + 1));;
	blkid *original_bid;
	//save the pointer location of "bids"
	original_bid = bids;
	int i, n, resize_needed;
	char *p_buffer = (char *)buf;
	char tmp_block[BLOCK_SIZE];
	u32 cur = fdtable[fd].cur;
	u32 num_cntnt_blks, num_bytes;

	num_bytes = 0;
	resize_needed = ((cur + length) > fdtable[fd].inode.size);

	/* set remaining, offset, to_copy */
	offset = cur % BLOCK_SIZE;
	remaining = length;
	if (remaining < (BLOCK_SIZE - offset)) {
		to_copy = remaining;
	} else {
		to_copy = BLOCK_SIZE - offset;
	}
	/* resize needed? */
	if (resize_needed) {
		sfs_resize_file(fd, (cur + length));
	}

	/*get the block ids of all contents (using sfs_get_file_content() */
	num_cntnt_blks = sfs_get_file_content(bids,fd,cur,length);

	/* main loop, go through every block, copy the necessary parts
	   to the buffer, consult the hint in the document. Do not forget to 
	   flush to the disk.
	 */

	for (i = 0; i < num_cntnt_blks; i++) {

		sfs_read_block(&tmp_block, *bids);

		int tmp_counter = 0;
		for (n = 0; n < to_copy; n++) {
			*(tmp_block + offset + tmp_counter) = *p_buffer;
			tmp_counter = tmp_counter + 1;
			p_buffer++;
		}
		//writing to HD
		sfs_write_block(&tmp_block, *bids);

		//update
		fdtable[fd].cur = fdtable[fd].cur + to_copy;
		remaining = remaining - to_copy;
		num_bytes = num_bytes + to_copy;
		offset = 0;

		if (remaining < BLOCK_SIZE) {
			to_copy = remaining;
		} else {
			to_copy = BLOCK_SIZE;
		}
		bids++;
	}

	//reset pointer
	bids = original_bid;

	/* update the cursor and free the temp buffer
	   for sfs_get_file_content()
	 */
	free(original_bid);
	return num_bytes;
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
	blkid *bids = malloc(sizeof(*bids) * ((length - 1) / BLOCK_SIZE + 1));
	blkid *original_bid;
	original_bid = bids;
	int i, n;
	char *p = (char *)buf;
	char tmp[BLOCK_SIZE];
	u32 cur = fdtable[fd].cur;
	u32 num_cntnt_blks;
	u32 bytes_read;

	bytes_read = 0;
	offset = cur % BLOCK_SIZE;
	remaining = length;

	/*check if we need to truncate */

	if (cur + length > fdtable[fd].inode.size) {
		length = (fdtable[fd].inode.size - cur) + 1;
	}

	/* Set amount to_copy */
	if (remaining < (BLOCK_SIZE - offset)) {
		to_copy = remaining;
	} else {
		to_copy = BLOCK_SIZE - offset;
	}

	/* similar to the sfs_write() */

	num_cntnt_blks = sfs_get_file_content(bids, fd, cur, length);

	for (i = 0; i < num_cntnt_blks; i++) {
		sfs_read_block(&tmp, *bids);
		int tmp_counter = 0;

		for (n = 0; n < to_copy; n++) {
			*p = *(tmp + offset + tmp_counter);
			tmp_counter = tmp_counter + 1;
			p++;
		}

		fdtable[fd].cur = fdtable[fd].cur + to_copy;
		remaining = remaining - to_copy;

		bytes_read = bytes_read + to_copy;

		if (remaining < BLOCK_SIZE) {
			to_copy = remaining;
			offset = 0;
		} else {
			to_copy = BLOCK_SIZE;
			offset = 0;
		}
		bids++;
	}

	//reset pointer
	bids = original_bid;

	free(bids);
	return bytes_read;
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
	/* get the old cursor, change it as specified by the parameters */
	if (loc == SFS_SEEK_SET) {
		fdtable[fd].cur = 0;
	} else if (loc == SFS_SEEK_END) {
		fdtable[fd].cur = fdtable[fd].inode.size;
	}

	fdtable[fd].cur = fdtable[fd].cur + relative;

	return 0;
}

/*
 * Check if we reach the EOF(end-of-file).
 * 
 * This function returns 1 if it is EOF, otherwise 0.
 */
int sfs_eof(int fd)
{
	/* check if the cursor has gone out of bound */
	if (fdtable[fd].cur == fdtable[fd].inode.size) {
		return 1;
	} else {
		return 0;
	}
}
