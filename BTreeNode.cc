#include "BTreeNode.h"

using namespace std;

/*
 *The structure of a 1024-byte buffer for the leaf node:
 *----------------------------------------------------------------------------
 *|KeyCount  |nextNode  |Pair of (key, rid)			 |Left for potential use |
 *|(4 bytes) |(4 bytes) |(12 bytes * 80 = 960 bytes) |(56 bytes)             |
 *----------------------------------------------------------------------------
 */
/*
 *Constructor of the class BTLeafNode.
 *Set the memory space for the buffer with the size of PAGE_SIZE
 *We are going to store maximum of 80 keys in one leaf node.
 */
BTLeafNode::BTLeafNode()
{
	memset(buffer, 0, PageFile::PAGE_SIZE);
}
/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
	return pf.read(pid, buffer);
}

/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{
	int count = 0;
	memcpy(&count, &buffer, sizeof(int));
	return count;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{
	//check the number of key first, return error code if full
	int count = getKeyCount();
	if (count >= 80)
	{
		printf("%s\n", "The leaf node is full");
		return RC_NODE_FULL;
	}
	char *it = buffer; //create iterator for the buffer
	it += sizeof(int) + sizeof(PageId); //pass the keyCount and pointer to next node
	int temp = 0; //temporarily stores the key of entry in buffer
	int smallCount = 0; //count the number of entries with key smaller than inserted key
	//look for the key of entry larger or equal to the key
	while (it)
	{
		memcpy(&temp, it, sizeof(int));
		if (temp == 0 || temp >= key) break;
		else it += sizeof(int) + sizeof(RecordId);
		smallCount++;
	}
	//create a string to store the entries with key larger than the inserted key
	char *larger = (char*)malloc((count - smallCount) * (sizeof(int) + sizeof(RecordId)));
	memcpy(larger, it, (count - smallCount) * (sizeof(int) + sizeof(RecordId)));
	//insert the key and rid in the current position it points to
	memcpy(it, &key, sizeof(int));
	it += sizeof(int);
	memcpy(it, &rid, sizeof(RecordId));
	it += sizeof(RecordId);
	//append the entries in larger to the buffer;
	memcpy(it, larger, (count - smallCount) * (sizeof(int) + sizeof(RecordId)));
	//set iterator to the begin of the buffer and update the number of keys
	it = buffer;
	count++;
	memcpy(it, &count, sizeof(int));
	free(larger);
	return 0;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid,
                              BTLeafNode& sibling, int& siblingKey)
{
	char *it = buffer; //create iterator from begin of buffer
	it += sizeof(int) + sizeof(PageId); //pass the key count and pointer to the next node;
	int half = getKeyCount() / 2;
	for (int i = 0; i < half; i++) it += sizeof(int) + sizeof(RecordId); //iterator points to the middle pair
	char *middle = it; //maintain a pointer to the middle pair before it being modified
	//copy the right half to the sibling node;
	int tempKey = 0;
	RecordId tempRid;
	while (it)
	{
		memcpy(&tempKey, it, sizeof(int));
		it += sizeof(int);
		memcpy(&tempRid, it, sizeof(RecordId));
		it += sizeof(RecordId);
		if (!tempKey) break;
		if (sibling.insert(tempKey, tempRid)) return RC_FILE_WRITE_FAILED;
	}
	memset(middle, 0, half * (sizeof(int) + sizeof(RecordId))); //clear the right half of the node;
	memcpy(buffer, &half, sizeof(int)); //update the new key count to the node;
	if (sibling.readEntry(0, siblingKey, tempRid)) return RC_FILE_READ_FAILED; //store temporarily first key in siblingKey
	if (siblingKey < key)
	{
		if (sibling.insert(key, rid)) return RC_FILE_WRITE_FAILED;
	}
	else
	{
		if (insert(key, rid)) return RC_FILE_WRITE_FAILED;
	}
	return 0;
}

/*
 * Find the entry whose key value is larger than or equal to searchKey
 * and output the eid (entry number) whose key value >= searchKey.
 * Remeber that all keys inside a B+tree node should be kept sorted.
 * @param searchKey[IN] the key to search for
 * @param eid[OUT] the entry number that contains a key larger than or equalty to searchKey
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{
	char *it = buffer; //create iterator from begin of buffer
	it += sizeof(int) + sizeof(PageId); //pass the key count and pointer to the next node
	int temp = 0;
	eid = 0;
	while (it)
	{
		memcpy(&temp, it, sizeof(int));
		if (temp == 0 || temp >= searchKey) break;
		else it += sizeof(int) + sizeof(RecordId);
		eid++;
	}
	return 0;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{
	char *it = buffer; //create iterator from begin of buffer
	it += sizeof(int) + sizeof(PageId); //pass the key count and pointer to the next node
	for (int i = 0; i < eid; i++) it += sizeof(int) + sizeof(RecordId); //go to the location
	memcpy(&key, it, sizeof(int)); //copy the key
	it += sizeof(int);
	memcpy(&rid, it, sizeof(RecordId)); //copy the rid
	return 0;
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node
 */
PageId BTLeafNode::getNextNodePtr()
{
	char *it = buffer; //create iterator from begin of buffer
	it += sizeof(int); //pass the key count
	PageId id;
	memcpy(&id, it, sizeof(PageId));
	return id;
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
	char *it = buffer; //create iterator from begin of buffer
	it += sizeof(int); //pass the key count
	memcpy(it, &pid, sizeof(PageId));
	return 0;
}


/*
 *The structure of a 1024-byte buffer for the non-leaf node:
 *----------------------------------------------------------------------------
 *|KeyCount  |First Pid |Pair of (key, PageId)		|Left for potential use |
 *|(4 bytes) |(4 bytes) |(8 bytes * 120 = 960 bytes) |(56 bytes)             |
 *----------------------------------------------------------------------------
 */
/*
 *Constructor of the class BTNonLeafNode.
 *Set the memory space for the buffer with the size of PAGE_SIZE
 *We are going to store maximum of 80 keys in one leaf node.
 */
BTNonLeafNode::BTNonLeafNode()
{
	memset(buffer, 0, PageFile::PAGE_SIZE);
}
/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
	return pf.read(pid, buffer);
}

/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
	int count;
	memcpy(&count, buffer, sizeof(int));
	return count;
}

RC BTNonLeafNode::readEntry(int eid, int& key, PageId& pid)
{
	char *it = buffer; //create iterator from begin of buffer
	it += sizeof(int) + sizeof(PageId); //pass the key count and pointer to the next node
	for (int i = 0; i < eid; i++) it += sizeof(int) + sizeof(PageId); //go to the location
	memcpy(&key, it, sizeof(int)); //copy the key
	it += sizeof(int);
	memcpy(&pid, it, sizeof(PageId)); //copy the rid
	return 0;
}

/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{
	//check the number of key first, return error code if full
	int count = getKeyCount();
	if (count >= 120)
	{
		printf("%s\n", "The leaf node is full");
		return RC_NODE_FULL;
	}
	char *it = buffer; //create iterator for the buffer
	it += sizeof(int) + sizeof(PageId); //pass the keyCount and the first page id
	int temp = 0; //temporarily stores the key of entry in buffer
	int smallCount = 0; //count the number of entries with key smaller than inserted key
	//look for the key of entry larger or equal to the key
	while (it)
	{
		memcpy(&temp, it, sizeof(int));
		if (temp == 0 || temp >= key) break;
		else it += sizeof(int) + sizeof(PageId);
		smallCount++;
	}
	if (temp == key)
	{
		it += sizeof(int);
		memcpy(it, &pid, sizeof(PageId));
		return 0;
	}
	//create a string to store the entries with key larger than the inserted key
	char *larger = (char*)malloc((count - smallCount) * (sizeof(int) + sizeof(PageId)));
	memcpy(larger, it, (count - smallCount) * (sizeof(int) + sizeof(PageId)));
	//insert the key and page id in the current position it points to
	memcpy(it, &key, sizeof(int));
	it += sizeof(int);
	memcpy(it, &pid, sizeof(PageId));
	it += sizeof(PageId);
	//append the entries in larger to the buffer;
	memcpy(it, larger, (count - smallCount) * (sizeof(int) + sizeof(PageId)));
	//set iterator to the begin of the buffer and update the number of keys
	it = buffer;
	count++;
	memcpy(it, &count, sizeof(int));
	free(larger);
	return 0;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{
	char *it = buffer; //create iterator from begin of buffer
	it += sizeof(int) + sizeof(PageId); //pass the key count and the first page if;
	int half = getKeyCount() / 2;
	for (int i = 0; i < half; i++) it += sizeof(int) + sizeof(PageId); //iterator points to the middle pair
	char *middle = it; //maintain a pointer to the middle pair before it being modified
	memcpy(&midKey, it, sizeof(int)); //pull the middle key to the parent node
	//it += sizeof(int) + sizeof(PageId);
	//copy the right half to the sibling node;
	int tempKey = 0;
	PageId tempPid;
	while (it)
	{
		memcpy(&tempKey, it, sizeof(int));
		it += sizeof(int);
		memcpy(&tempPid, it, sizeof(PageId));
		it += sizeof(PageId);
		if (!tempKey) break;
		if (sibling.insert(tempKey, tempPid)) return RC_FILE_WRITE_FAILED;
	}
	memset(middle, 0, half * (sizeof(int) + sizeof(PageId))); //clear the right half of the node;
	memcpy(buffer, &half, sizeof(int)); //update the new key count to the node;
	if (midKey < key)
	{
		if (sibling.insert(key, pid)) return RC_FILE_WRITE_FAILED;
	}
	else
	{
		if (insert(key, pid)) return RC_FILE_WRITE_FAILED;
	}
	return 0;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{
	char *it = buffer; //create iterator from begin of buffer
	it += sizeof(int); //pass the key count
	memcpy(&pid, it, sizeof(PageId)); //assign the first page id to the pid
	it += sizeof(PageId); //pass the first page id
	int temp = 0;
	while (it)
	{
		memcpy(&temp, it, sizeof(int));
		if (temp == 0 || temp > searchKey) break;
		it += sizeof(int);
		memcpy(&pid, it, sizeof(PageId));
		it += sizeof(PageId);
	}
	return 0;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
	char *it = buffer; //create iterator for the buffer
	int count = 1;
	memcpy(it, &count, sizeof(int)); //assign the key count as 1;
	it += sizeof(int);
	memcpy(it, &pid1, sizeof(PageId)); //insert the first pid
	it += sizeof(PageId);
	memcpy(it, &key, sizeof(int)); //insert the key;
	it += sizeof(int);
	memcpy(it, &pid2, sizeof(PageId)); //insert the second pid
	return 0;
}
