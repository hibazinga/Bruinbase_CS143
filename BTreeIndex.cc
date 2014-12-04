/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
	treeHeight = 0;
    rootPid = -1;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
	if (pf.open(indexname, mode) != 0) return RC_FILE_OPEN_FAILED;
	char buffer[PageFile::PAGE_SIZE];
	if (pf.read(0, buffer) != 0) return RC_FILE_READ_FAILED;
	memcpy(&rootPid, buffer, sizeof(PageId));
	memcpy(&treeHeight, buffer + sizeof(PageId), sizeof(int));
    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	char buffer[PageFile::PAGE_SIZE];
	memcpy(buffer, &rootPid, sizeof(PageId));
	memcpy(buffer + sizeof(PageId), &treeHeight, sizeof(int));
	if (pf.write(0, buffer) != 0) return RC_FILE_WRITE_FAILED;
	return pf.close();
}

/*
 * The help function of function insert to do the recursive work
 */
RC BTreeIndex::insertHelp(int key, const RecordId& rid, int currentHeight, PageId currentPid, PageId &siblingPid, int &siblingKey)
{
	//base case: the current node is the leaf node
	if (currentHeight == treeHeight)
	{
		//create leaf node
		BTLeafNode *leaf = new BTLeafNode();
		//read the content of current page file to the leaf node
		if (leaf->read(currentPid, pf)) return RC_FILE_READ_FAILED;
		//if there exists space to insert, do it
		if (leaf->getKeyCount() < 80)
		{
			//insert the pair
			if (leaf->insert(key, rid)) return RC_FILE_WRITE_FAILED; 
			//write the modified content into the current page file
			if (leaf->write(currentPid, pf)) return RC_FILE_WRITE_FAILED; 
			return 0;  //success
		}
		else  //else, insert and split
		{
			//create a sibling leaf node for splitting
			BTLeafNode sibling = BTLeafNode();
			//insert and split
			if (leaf->insertAndSplit(key, rid, sibling, siblingKey)) return RC_FILE_WRITE_FAILED; 
			//assign the page id to the sibling 
			siblingPid = pf.endPid(); 
			//assign the next pointer of current node to the next pointer of sibling node
			if (sibling.setNextNodePtr(leaf->getNextNodePtr())) return RC_FILE_WRITE_FAILED; 
			//change the next pointer of the current node to the sibling node pid
			if (leaf->setNextNodePtr(siblingPid)) return RC_FILE_WRITE_FAILED;
			//write the modified current node to the page file
			if (leaf->write(currentPid, pf)) return RC_FILE_WRITE_FAILED;
			//write the new sibling node to the page file
			if (sibling.write(siblingPid, pf)) return RC_FILE_WRITE_FAILED; 
			return RC_LEAFNODE_OVERFLOW;  //need to be tackle with on the upper level tree
		}
	}
	//recursive case: the current node is non-leaf node 
	else
	{
		//create a non-leaf node
		BTNonLeafNode *nonleaf = new BTNonLeafNode(); 
		//read the content of current page file to the non-leaf node
		if (nonleaf->read(currentPid, pf)) return RC_FILE_READ_FAILED;
		//get the child pid that should be pointed to
		PageId child;
		if (nonleaf->locateChildPtr(key, child)) return RC_FILE_SEEK_FAILED;
		//recursive call the the child page file
		RC result = insertHelp(key, rid, currentHeight + 1, child, siblingPid, siblingKey);
		//if result < 0, return the error code
		if (result == 0) return 0;
		else if (result == RC_LEAFNODE_OVERFLOW) //insert key on the parent node
		{
			//if there exists space to insert, do it
			if (nonleaf->getKeyCount() < 120)
			{
				// insert the first pair of key and pid of the sibling to the non-leaf node
				if (nonleaf->insert(siblingKey, siblingPid)) return RC_FILE_WRITE_FAILED;
				// write the modified current node to the page file
				if (nonleaf->write(currentPid, pf)) return RC_FILE_WRITE_FAILED;
				return 0; //success
			}
			else //insert and split
			{
				//create new sibling non-leaf node
				BTNonLeafNode sibling = BTNonLeafNode();
				//insert and split
				int nonLeafSiblingKey = 0;
				if (nonleaf->insertAndSplit(siblingKey, siblingPid, sibling, nonLeafSiblingKey)) return RC_FILE_WRITE_FAILED;
				//siblingkey to be insert into upper level tree comes to be nonLeafSiblingKey
				siblingKey = nonLeafSiblingKey;
				siblingPid = pf.endPid();
				//write the modified current node to the page file
				if (nonleaf->write(currentPid, pf)) return RC_FILE_WRITE_FAILED;
				//write the modified sibling node to the page file
				if (sibling.write(siblingPid, pf)) return RC_FILE_WRITE_FAILED;
				return RC_LEAFNODE_OVERFLOW;  //need to be tackle with on the upper level tree
			}
		}
		else return RC_FILE_WRITE_FAILED; //otherwise, return the error code
	}
}
/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	// if the tree is empty, create one
	if (treeHeight == 0)
	{
		//create the leaf node
		BTLeafNode *leaf = new BTLeafNode();
		//insert the key-rid pair directly, since we are sure that node is empty
		if (leaf->insert(key, rid)) return RC_FILE_WRITE_FAILED;
		rootPid = pf.endPid();
		//write the new leaf node to the page file
		if (leaf->write(rootPid, pf)) return RC_FILE_WRITE_FAILED;
		//increase the height by one
		treeHeight++;
		return 0; //success
	}
	else //if not empty, need to be done recursively
	{
		//recursive call
		int siblingKey;
		PageId siblingPid;
		RC result = insertHelp(key, rid, 1, rootPid, siblingPid, siblingKey);
		if (result == 0) return 0;  //success
		else if (result == RC_LEAFNODE_OVERFLOW) //in the case of overflow
		{
			//create and initialize the root node
			BTNonLeafNode *root = new BTNonLeafNode();
			if (root->initializeRoot(rootPid, siblingKey, siblingPid)) return RC_FILE_WRITE_FAILED;
			rootPid = pf.endPid();
			//write the new root to the page file
			if (root->write(rootPid, pf)) return RC_FILE_WRITE_FAILED;
			//increase the height by one
			treeHeight++;
			return 0; //success
		}
		else return RC_FILE_WRITE_FAILED;  //otherwise, return error code
	}
    return 0;
}

/*
 * Find the leaf-node index entry whose key value is larger than or 
 * equal to searchKey, and output the location of the entry in IndexCursor.
 * IndexCursor is a "pointer" to a B+tree leaf-node entry consisting of
 * the PageId of the node and the SlotID of the index entry.
 * Note that, for range queries, we need to scan the B+tree leaf nodes.
 * For example, if the query is "key > 1000", we should scan the leaf
 * nodes starting with the key value 1000. For this reason,
 * it is better to return the location of the leaf node entry 
 * for a given searchKey, instead of returning the RecordId
 * associated with the searchKey directly.
 * Once the location of the index entry is identified and returned 
 * from this function, you should call readForward() to retrieve the
 * actual (key, rid) pair from the index.
 * @param key[IN] the key to find.
 * @param cursor[OUT] the cursor pointing to the first index entry
 *                    with the key value.
 * @return error code. 0 if no error.
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	// if the tree is empty return the error code
	if (treeHeight == 0) return RC_FILE_SEEK_FAILED;
	//create both the leaf node and the non-leaf node
	BTLeafNode *leaf = new BTLeafNode();
	BTNonLeafNode *nonleaf = new BTNonLeafNode();
	PageId pid = rootPid;
	//determine the pid the target leaf node
	for (int i = 0; i < treeHeight - 1; i++)
	{
		//read the content of certain page file with page id equals to pid
		if (nonleaf->read(pid, pf)) return RC_FILE_READ_FAILED;
		//locate the target child that could point to the search key
		if (nonleaf->locateChildPtr(searchKey, pid)) return RC_FILE_SEEK_FAILED;
	}
	//now pid stores the pid of the target leaf node
	//read the content of certain page file with page id equals to pid
	if (leaf->read(pid, pf)) return RC_FILE_READ_FAILED;
	//locate the target entry with search key in the leaf node
	int eid = -1;
	if (leaf->locate(searchKey, eid)) return RC_FILE_SEEK_FAILED;
	//store the two indices into the index cursor
	cursor.pid = pid;
	cursor.eid = eid;
    return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
	BTLeafNode *leaf = new BTLeafNode();
	//read the content to the leaf node
	if (leaf->read(cursor.pid, pf)) return RC_FILE_READ_FAILED;
	//read the entry of target eid
	if (leaf->readEntry(cursor.eid, key, rid)) return RC_INVALID_CURSOR;
	//point the cursor to the next entry if the entry is not the last one
	if (cursor.eid < leaf->getKeyCount() - 1) cursor.eid++;
	else //point the the first entry of the next node if the current entry is the last one
	{
		//set the pid to the next node if it exists
		if (cursor.pid = leaf->getNextNodePtr() < 0) return RC_END_OF_TREE;
		//set eid to the first entry
		cursor.eid = 0;
	}
    return 0;
}
