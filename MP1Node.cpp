/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 *              Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 *              This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 *              All initializations routines for a member.
 *              Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
    int id = *(int*)(&memberNode->addr.addr);
    int port = *(short*)(&memberNode->addr.addr[4]);

    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 *              Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        //cout<<"checkMessages: memberNode address"<<printAddress(&memberNode->addr)<<"\n";
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}


/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    /*
     * Your code goes here
     */


    Member *toNode;
    Member *fromNode = new Member;

    

    char addr[6];
    Address *paddr;

    MessageHdr *msg;
    int id;
    short port;
    long heartbeat = 0;
    int timestamp = 0;

    memcpy(&msg->msgType, data, sizeof(MessageHdr));
    if (msg->msgType == JOINREQ){
        toNode   = (Member *)env;
        cout<<"toNode membership list size = "<<toNode->memberList.size()<<"\n";
        cout<<"toNode id = "<<toNode->addr.getAddress()<<"\n";

        //fromNode =  

        //printing the membership list
        //cout<<"revCallback : from node membershiList size = "<<toNode->memberList.size()<<endl;
        //cout<<"revCallback : from node membershiList id = "<<toNode->memberList[0].id<<endl;
        //cout<<"revCallback : from node current heartbeat = "<<fromNode->heartbeat<<endl;
        //cout<<"revCallback : from node current heartbeat = "<<toNode->memberList[0].id<<endl;
        memcpy(&id, data + sizeof(MessageHdr), sizeof(int));
        memcpy(&port, data + sizeof(MessageHdr) + sizeof(int), sizeof(short));
        
        cout<<"from node init\n";
        fromNode->inited   = false;
        //cout<<"check2\n";
        fromNode->inGroup  = true;
        fromNode->bFailed  = false;
        fromNode->nnb      = 1;
        //cout<<"check1\n";
        fromNode->heartbeat= 0;
        fromNode->pingCounter=0;
        fromNode->timeOutCounter = 0;


        cout<<"fromNode->id = "<<id<<"\n";
        cout<<"fromNode->port = "<<port<<"\n";

        cout<<"filling membership list\n";

        MemberListEntry *temp = new MemberListEntry;
        temp->setid(id);
        temp->setport(port);
        temp->setheartbeat(fromNode->heartbeat);
        temp->settimestamp(fromNode->heartbeat);

        //stting membership list
        cout<<"setting membershipList\n";
        toNode->memberList.push_back(*temp);
        cout<<"membershipList Entered\n";


        //assigning hearbeat of the self 
        string toAddress = toNode->addr.getAddress();
        cout<<"To Address = "<<toAddress<<endl;
        //memcpy(&id, toNode->addr[0], sizeof(int));
        //memcpy(&port, toNode->addr[4], sizeof(short));
        cout<<"To Address id= "<<id<<endl;
        cout<<"From address port = "<<port<<endl;
        
        temp->setid(id);
        temp->setport(port);
        temp->setheartbeat(fromNode->heartbeat);
        temp->settimestamp(fromNode->pingCounter);

        cout<<"revCallback : from node membershiList size = "<<toNode->memberList.size()<<endl;
        cout<<"revCallback : from node membershiList id = "<<toNode->memberList[id].id<<endl;
        cout<<"revCallback : from node current heartbeat = "<<toNode->memberList[id].port<<endl;
        cout<<"revCallback : from node current heartbeat = "<<toNode->memberList[id].heartbeat<<endl;

        //toNode->memberList[0] = new MemberListEntry;
        //toNode->memberList[0].setid(id);
        //toNode->memberList[0].setport(port);
        
        //cout<<"revCallback : from node membershiList id = "<<fromNode->memberList[0].id<<endl;
        //cout<<"revCallback : from node membershiList port = "<<fromNode->memberList[0].port<<endl;
        //cout<<"revCallback : from node membershiList heartbeat = "<<fromNode->memberList[0].heartbeat<<endl;
        //cout<<"revCallback : from node membershiList heartbeat = "<<fromNode->myPos<<endl;
        for(toNode->myPos = toNode->memberList.begin(); toNode->myPos != toNode->memberList.end(); toNode->myPos++)
            cout<<"Membership Id ="<<toNode->myPos->id<<"\n";

      //this is from some node to the start node
      memcpy(&fromNode->addr.addr[0], data + sizeof(MessageHdr), sizeof(fromNode->addr.addr));
      cout<<"fromNode address = "<<fromNode->addr.getAddress()<<endl;

      cout<<"tonode address = "<<toNode->addr.getAddress()<<endl;
      //forming return message
      cout<<"Satrting returning message\n";
      MessageHdr *retmsg;

      size_t retmsgsize = sizeof(MessageHdr) + sizeof(toNode->memberList) + sizeof(long) + 1;
      retmsg = (MessageHdr *)malloc(retmsgsize * sizeof(char));
      cout<<"Making the JoinREP\n";
      retmsg->msgType = JOINREP;

      size_t vSize = sizeof(vector<MemberListEntry>) + (size_t) sizeof(toNode->memberList[0]) * (size_t) toNode->memberList.size();
      cout<<"vSize = "<<vSize<<"\n";

      memcpy((char *)(retmsg+1), &vSize, sizeof(size_t));
      memcpy((char *)(retmsg+1) + sizeof(size_t), &toNode->memberList, sizeof(toNode->memberList));
      memcpy((char *)(retmsg+1) + 1 + sizeof(toNode->memberList) + sizeof(size_t), &toNode->heartbeat, sizeof(long));

      emulNet->ENsend(&toNode->addr,&fromNode->addr, (char *)retmsg, retmsgsize);      
    }

    //if JOINREP
    //add the node to the group
    if (msg->msgType == JOINREP){
        cout<<"Inside JOINREP\n";
        Member *mem = (Member *)env;
        cout<<"env address = "<<mem->addr.getAddress()<<endl;

        int message_size;
        memcpy(&message_size, data, sizeof(size_t));
        cout<<"JoinRep: message_size = "<<message_size<<endl;
        //copying membershiplist for the first time
        MemberListEntry *newlist = new MemberListEntry;


        mem->inGroup = true;
        //mem->inited = true;
        mem->bFailed = false;
        //number of neighbours  = size of memberlist
        mem->nnb = mem->memberList.size();
        //adding the members and starting action
        //MemberListEntry *newlist = new MemberListEntry;
        //memcpy((char *)(msg+1), &newlist, sizeof(MemberListEntry));

        //send heartbeat to the memberlist
    }
    
    cout<<"MP1Node : return from the Mp1node\n";
    return true;

}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 *              the nodes
 *              Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    /*
     * Your code goes here
     */

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
