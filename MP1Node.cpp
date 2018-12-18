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
    cout<<"Starting checkMessages()\n";
    checkMessages();

    //send heartbeat
    //send_heartbeat();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}


/**
* FUNCTION NAME: send_heartbeat
*
* DESCRIPTION: To send heartbeat at each time intewrval
*/

void MP1Node::send_heartbeat(){

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

    toNode = (Member *)env;

    //par = new Params();
    //cout<<"Current time = "<<par->getcurrtime();

    //toNode->timeOutCounter = toNode->timeOutCounter + 1;


    char addr[6];
    Address *toaddr;

    MessageHdr *msg;
    int id;
    short port;
    long heartbeat;
    int timestamp = 0;

    memcpy(&msg->msgType, data, sizeof(MessageHdr));
    cout<<"Inside checkMessages()\n";
    cout<<"msgType = "<<msg->msgType<<"\n";
    if (msg->msgType == JOINREQ){
        //toNode   = (Member *)env;
        cout<<"toNode membership list size = "<<toNode->memberList.size()<<"\n";
        cout<<"toNode id = "<<toNode->addr.getAddress()<<"\n";

        memcpy(&id, data + sizeof(MessageHdr), sizeof(int));
        memcpy(&port, data + sizeof(MessageHdr) + sizeof(int), sizeof(short));
        memcpy(&heartbeat, (data+1) + 1 + sizeof(toNode->addr.addr), sizeof(long));

        cout<<"data ...id   = "<<id<<"\n";
        cout<<"data ...port = "<<port<<"\n";
        cout<<"data ...heartbeat = "<<heartbeat<<"\n";

        cout<<"filling membership list\n";

        MemberListEntry *temp = new MemberListEntry;
        temp->setid(id); //fromNode->id
        temp->setport(port); //fromNode->port
        temp->setheartbeat(heartbeat);
        //this is local timestamp of the toNode
        temp->settimestamp(par->getcurrtime());

        //stting membership list
        cout<<"setting membershipList\n";
        toNode->memberList.push_back(*temp);
        cout<<"membershipList Entered\n";

        //incrementing heartbeat of toNode
        toNode->heartbeat = toNode->heartbeat + 1;
        //toNode->settimestamp(par->getcurrtime());

        for(toNode->myPos = toNode->memberList.begin(); toNode->myPos != toNode->memberList.end(); toNode->myPos++)
            cout<<"Membership Id ="<<toNode->myPos->id<<"\n";

        //forming return message
        cout<<"Satrting returning message\n";
        MessageHdr *retmsg;
        size_t vSize = sizeof(vector<MemberListEntry>) + (size_t) sizeof(toNode->memberList[0]) * (size_t) toNode->memberList.size();

        size_t retmsgsize = sizeof(MessageHdr) + sizeof(toNode->addr.addr) + sizeof(toNode->memberList[0]) + sizeof(long) + 1;
        retmsg = (MessageHdr *)malloc(retmsgsize * sizeof(char));
        cout<<"Making the JoinREP\n";
        retmsg->msgType = JOINREP;

      
        cout<<"vSize = "<<vSize<<"\n";
        int i;
        for (i=0;i<vSize;i++){
          memcpy((char *)(retmsg+1), &toNode->addr.addr, sizeof(memberNode->addr.addr));
          //memcpy((char *)(retmsg+1) + sizeof(toNode->addr.addr), &vSize, sizeof(size_t));
          memcpy((char *)(retmsg+1) + sizeof(toNode->addr.addr) + sizeof(size_t), &toNode->memberList[i], sizeof(toNode->memberList[i]));
          memcpy((char *)(retmsg+1) + 1 + sizeof(toNode->addr.addr) + sizeof(toNode->memberList[i]) + sizeof(size_t), &toNode->heartbeat, sizeof(long));  
          emulNet->ENsend(&toNode->addr,&fromNode->addr, (char *)retmsg, retmsgsize);        
        }
      
      } //if (msg->msgType == JOINREQ)

    //if JOINREP
    //add the node to the group
    if (msg->msgType == JOINREP){
        cout<<"Inside JOINREP\n";
        
        toNode = (Member *)env; //2:0
        int size_of_vector;
        cout<<"env address = "<<toNode->addr.getAddress()<<endl;

        //getting fromNode from the JOINREP message
        memcpy(&id, data + sizeof(MessageHdr), sizeof(int));
        memcpy(&port, data + sizeof(MessageHdr) + sizeof(int), sizeof(short));
        memcpy(&toaddr->addr, data + sizeof(MessageHdr), sizeof(toaddr->addr));

        memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(toaddr->addr), sizeof(long)); //1:0 heartbeat

        cout<<"JOINREP : id = "<<id<<endl; ///1:0
        cout<<"JOINREP : port = "<<port<<"\n";
        cout<<"JOINREP : Address"<<toaddr->getAddress()<<"\n";
        cout<<"JoinREP : heartbeat = "<<heartbeat<<"\n";

        //starting up the node
        cout<<"toNode node init\n"; //2:0
        ///////////////////////////////////
        toNode->inited   = true;
        //cout<<"check2\n";
        toNode->inGroup  = true;
        toNode->bFailed  = false;
        //fromNode->nnb      = 1;
        //cout<<"check1\n";
        toNode->heartbeat = toNode->heartbeat + 1; 
        //toNode->settimestamp(par->getcurrtime()); 
        toNode->pingCounter=toNode->pingCounter + 1;
        toNode->timeOutCounter = toNode->timeOutCounter + 1;
        //////////////////////////////////

        int message_size;
        memcpy(&message_size, data, sizeof(size_t));
        cout<<"JoinRep: message_size = "<<message_size<<endl;
        //copying membershiplist for the first time
        MemberListEntry *newlist = new MemberListEntry;

        //copying vector
        memcpy(&newlist, data + sizeof(MessageHdr) + sizeof(toaddr->addr) + sizeof(long), sizeof(newlist));
        cout<<"JoinRep : newlist->id"<<newlist->id<<endl;
        cout<<"JoinRep : newlist->port"<<newlist->port<<endl;
        cout<<"JoinRep : newlist->heartbeat"<<newlist->heartbeat<<endl;

        //if (toaddr->addr == newlist)
        toNode->memberList.push_back(*newlist);

        cout<<"JoinRep : tonode->vector size = "<<toNode->memberList.size()<<endl;
        toNode->heartbeat = toNode->heartbeat + 1;

        //updating the current node heartbeat in the membership list
        int size_list = toNode->memberList.size();
        cout<<"Size of the toNode->list = "<<size_list<<"\n";
        for(toNode->myPos = toNode->memberList.begin(); toNode->myPos != toNode->memberList.end(); toNode->myPos++){
            if (toNode->myPos->id == toNode->addr.addr[0] && toNode->myPos->port == toNode->addr.addr[4]){
                toNode->myPos->setheartbeat(toNode->heartbeat); 
                toNode->myPos->settimestamp(par->getcurrtime()); 
            }
        }
        
        //send heartbeat to the memberlist
        //creating dummymessage with heartbeat
        MessageHdr *dummymsg;

        for(toNode->myPos = toNode->memberList.begin(); toNode->myPos != toNode->memberList.end(); toNode->myPos++){
          size_t dummysize =  sizeof(MessageHdr) + sizeof(toNode->addr.addr) + sizeof(long) + sizeof(toNode->memberList[0]);
          dummymsg = (MessageHdr *)malloc(dummysize * sizeof(char));
          dummymsg->msgType = DUMMYLASTMSGTYPE;

          memcpy((char *)(dummymsg + 1), &toNode->addr.addr, sizeof(toNode->addr.addr));
          memcpy((char *)(dummymsg + 1 + sizeof(toNode->addr.addr)), &toNode->heartbeat, sizeof(long));
          memcpy((char *)(dummymsg + 1) + sizeof(toNode->addr.addr) + sizeof(long), &toNode->myPos, sizeof(toNode->memberList[0]));
      }
    }
        
        //dummy message
        if (msg->msgType == DUMMYLASTMSGTYPE){
            
            toNode = (Member *)env; //n:0

            //increasing its own heartbeat
            toNode->heartbeat = toNode->heartbeat + 1;
            cout<<"dummymsg : env address = "<<toNode->addr.getAddress()<<endl;
            memcpy(&fromNode->addr.addr, data + sizeof(MessageHdr), sizeof(fromNode->addr.addr));
            cout<<"dummymsg : from Node Address = "<<fromNode->addr.getAddress()<<endl;
            memcpy(&fromNode->heartbeat, data + sizeof(MessageHdr) + sizeof(fromNode->addr.addr), sizeof(long));
            cout<<"dummymsg : from Node Address = "<<fromNode->heartbeat<<endl;

            MemberListEntry *newEntry;
            memcpy(&newEntry,(char *)(data + 1) + sizeof(toNode->addr.addr) + sizeof(long), sizeof(newEntry));

            //adding timestamp 
            for(toNode->myPos = toNode->memberList.begin(); toNode->myPos != toNode->memberList.end(); toNode->myPos++){
                
                Address *timestamp_addr;
                timestamp_addr->addr[0] = toNode->myPos->id;
                timestamp_addr->addr[4] = toNode->myPos->port;

                if (*timestamp_addr == fromNode->addr){
                  if (fromNode->heartbeat > toNode->myPos->heartbeat){
                    toNode->myPos->setheartbeat(fromNode->heartbeat); 
                    toNode->myPos->settimestamp(par->getcurrtime());
                  }  
                }
                //increasing its own heartbeat
                if (*timestamp_addr == toNode->addr) {
                    toNode->myPos->setheartbeat(toNode->heartbeat);
                    toNode->myPos->settimestamp(par->getcurrtime());
                }

            }
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
    //to check for the mpNode memberlist anymember has not responded within a timeperiod.
    for(memberNode->myPos = memberNode->memberList.begin(); memberNode->myPos != memberNode->memberList.end(); memberNode->myPos++){
        if (memberNode->myPos->timestamp == memberNode->timeOutCounter){
            //failing the node 

            //memberNode->myPos
        }
        if (memberNode->myPos->timestamp == (2 * memberNode->timeOutCounter)){
            //removing the node from the memberNode memberList
            memberNode->memberList.erase(memberNode->myPos);
        }

    }


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