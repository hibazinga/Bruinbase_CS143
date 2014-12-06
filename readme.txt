					README


Team Info:

Name		SID		Email
Yanbin Ren	004435008	yanbinren@cs.ucla.edu
Yijia Liu	804414479	liuyijia@cs.ucla.edu


Project 2:

Part A:

We modified the source file SqlEngine.cc and implemented function “RC SqlEngine::load(..)”.

Part B:

We modified the source file BTreeNode.h and BTreeNode.cc:
1) implemented all the functions that are declared in BTreeNode.h;
2) add constructor for both class (BTLeafNode and BTNonLeafNode);
3) add function readEntry for the class of BTNonLeafNode for the test purpose.

Part C:

We modified the source file BTreeIndex.h and BTreeIndex.cc:
1) implemented all the functions that are declared in BTreeIndex.h;
2) add an help function for insert function so that it could be called recursively.

Part D:

We modified the source file SqlEngine.cc, BTreeIndex.h, and BTreeIndex.cc:
1) implemented the select function in SqlEngine.cc, in order to support select by index;
2) modified the load function in SqlEngine.cc that now can support load index;
3) add two variable in BTreeIndex class in BTreeIndex.h, for the purpose of reading fast;
4) modified the readForward function in BTreeIndex.cc, so that reduced the number of read from disk and hence speed up the queries.

