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
    initMemberListTable(memberNode);
    delete memberNode;
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
    //cout<<"Starting checkMessages()\n";
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }

    //send heartbeat
    if (!memberNode->bFailed && memberNode->inGroup)
    send_heartbeat();

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
    int id;
    short port;
    memberNode->heartbeat = memberNode->heartbeat + 1;

    //memberNode->settimestamp(par->getcurrtime());
    cout<<"HBEAT: ";
    printAddress(&memberNode->addr);
    cout<<"HBEAT: hearbeat = "<<memberNode->heartbeat<<"\n";
    //if (memberNode->pingCounter == 2)
    int vsize = memberNode->memberList.size();
    cout<<"send_heartbeat : vsize = "<<vsize<<endl;
    for(int i=0;i<(vsize); i++){
        //int ran_var = rand() % vsize;
        //cout<<"HBEAT : ran_var = "<<ran_var<<endl;
        //creating address from the membership list entry of ran_var
        //sending to random
        //id = memberNode->memberList[ran_var].id;
        //port = memberNode->memberList[ran_var].port;
        //sending to all
        id = memberNode->memberList[i].id;
        port = memberNode->memberList[i].port;
        if (id == (int)memberNode->addr.addr[0] && port == (short)memberNode->addr.addr[4]){
            cout<<"HBEAT : updating memberList heartbeat addr=";
            printAddress(&memberNode->addr);
            //memberNode->memberList[ran_var].heartbeat = memberNode->heartbeat; 
            //memberNode->memberList[ran_var].timestamp = par->getcurrtime();

            memberNode->memberList[i].heartbeat = memberNode->heartbeat; 
            memberNode->memberList[i].timestamp = par->getcurrtime();
        } else {
            for(memberNode->myPos = memberNode->memberList.begin(); memberNode->myPos != memberNode->memberList.end(); memberNode->myPos++){
                //cout<<"HBEAT : starting loop\n";
                MessageHdr *dummymsg = new MessageHdr;
                size_t dummysize =  sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + 1 + sizeof(long) + sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long);
                //cout<<"HBEAT : dummysize = "<<dummysize<<"\n";
                dummymsg = (MessageHdr *)malloc(dummysize * sizeof(char));
                dummymsg->msgType = DUMMYLASTMSGTYPE;
                //cout<<"HBEAT : dummysize = "<<dummysize<<"\n";
                memcpy((char *)(dummymsg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
                memcpy((char *)(dummymsg + 1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
                memcpy((char *)(dummymsg + 1) + sizeof(memberNode->addr.addr) + 1 + sizeof(long), &(memberNode->myPos->id), sizeof(int));
                memcpy((char *)(dummymsg + 1) + sizeof(memberNode->addr.addr) + 1 + sizeof(long) + sizeof(int), &(memberNode->myPos->port), sizeof(short));
                memcpy((char *)(dummymsg + 1) + sizeof(memberNode->addr.addr) + 1 + sizeof(long) + sizeof(int) + sizeof(short), &(memberNode->myPos->heartbeat), sizeof(long));
                memcpy((char *)(dummymsg + 1) + sizeof(memberNode->addr.addr) + 1 + sizeof(long) + sizeof(int) + sizeof(short) + sizeof(long), &(memberNode->myPos->id), sizeof(long));\
                //cout<<"Selecting addresses at random\n";
                //cout<<"HBEAT : id = "<<id<<endl;
                //cout<<"HBEAT : port = "<<port<<endl;
                Address *newaddr = new Address;
                memcpy(&newaddr->addr[0], &id, sizeof(int));
                memcpy(&newaddr->addr[4], &port, sizeof(short));
                cout<<"HBEAT : from addr = ";
                printAddress(&memberNode->addr);
                cout<<"HBEAT : to address = ";
                printAddress(newaddr);
                emulNet->ENsend(&memberNode->addr, newaddr, (char *)dummymsg, dummysize);        
            }
        }
    }
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
        cout<<"calling recvCallBack\n";
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


    Member *toNode = new Member;
    Member *fromNode = new Member;

    toNode = (Member *)env;

    //par = new Params();
    //cout<<"Current time = "<<par->getcurrtime();

    //toNode->timeOutCounter = toNode->timeOutCounter + 1;


    char addr[6];
    Address tempaddr;

    MessageHdr *msg = new MessageHdr;
    int id;
    short port;
    long heartbeat;
    int timestamp = 0;
    //msg->msgType = data;

    memcpy(&msg->msgType, data , sizeof(MessageHdr));
    cout<<"Inside checkMessages()\n";
    cout<<"msgType = "<<msg->msgType<<"\n";
    
    //memcpy(&msg->msgType, data, sizeof(MessageHdr));
    if (msg->msgType == JOINREQ){
        //toNode   = (Member *)env;
        //cout<<"JOINREQ : toNode membership list size = "<<toNode->memberList.size()<<"\n";
        cout<<"JOINREQ : toNode address = "<<toNode->addr.getAddress()<<"\n";

        memcpy(&id, data + sizeof(MessageHdr), sizeof(int));
        memcpy(&port, data + sizeof(MessageHdr) + sizeof(int), sizeof(short));
        memcpy(&tempaddr.addr, data + sizeof(MessageHdr), sizeof(tempaddr.addr));
        memcpy(&heartbeat, (data) + sizeof(MessageHdr) + 1 + sizeof(toNode->addr.addr), sizeof(long)); //1:0 heartbeat

        //INCREASING toNode timestamp
        //toNode->settimestamp(par->getcurrtime());
        //cout<<"data ...id   = "<<id<<"\n";
        //cout<<"data ...port = "<<port<<"\n";
        //cout<<"data ...size..id = "<<sizeof(id)<<"\n";
        //cout<<"data ...size..port = "<<sizeof(port)<<"\n";
        //cout<<"data ...size..addr = "<<sizeof(tempaddr.addr)<<"\n";

        //tempaddr->addr[0] = id;
        //tempaddr->addr[4] = port;


        //cout<<"data ...heartbeat = "<<heartbeat<<"\n";

        //cout<<"filling membership list\n";

        MemberListEntry *temp = new MemberListEntry;
        temp->setid(id); //fromNode->id
        temp->setport(port); //fromNode->port
        temp->setheartbeat(heartbeat);
        //this is local timestamp of the toNode
        temp->settimestamp(par->getcurrtime());

        //stting membership list
        //cout<<"setting membershipList\n";
        toNode->memberList.push_back(*temp);
        log->logNodeAdd((Address *)(&toNode->addr), (Address *)(&tempaddr));

        //cout<<"membershipList Entered\n";

        //incrementing heartbeat of toNode
        toNode->heartbeat = toNode->heartbeat + 1;
        cout<<"JOINREQ : heartbeat from message = "<<heartbeat<<endl;
        cout<<"JOINREQ : toNode->heartbeat = "<<toNode->heartbeat<<endl;
        //toNode->settimestamp(par->getcurrtime());


        //for(toNode->myPos = toNode->memberList.begin(); toNode->myPos != toNode->memberList.end(); toNode->myPos++)
        //    cout<<"Membership Id ="<<toNode->myPos->id<<"\n";



        //setting fromNode Address
        fromNode->addr = tempaddr;
        cout<<"JOINREQ : fromNode->addr = "<<fromNode->addr.getAddress()<<endl;

        //cout<<"JOINREQ : sizeof(MessageHdr) = "<<sizeof(MessageHdr)<<endl;

        //cout<<"vSize = "<<vSize<<"\n";
        int i;
        for(toNode->myPos = toNode->memberList.begin(); toNode->myPos != toNode->memberList.end(); toNode->myPos++){
            //forming return message
            //cout<<"Satrting returning message\n";
            cout<<"JOINREQ : toNode->myPos->getid() = "<<toNode->myPos->getid()<<":";
            cout<<toNode->myPos->getport()<<"\n";
            MessageHdr *retmsg;
            //size_t vSize = sizeof(vector<MemberListEntry>) + (size_t) sizeof(toNode->memberList[0]) * (size_t) toNode->memberList.size();
            size_t retmsgsize = sizeof(MessageHdr) + sizeof(toNode->addr.addr) + sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long) + sizeof(long) + 1;
            retmsg = (MessageHdr *)malloc(retmsgsize * sizeof(char));
            //cout<<"Making the JoinREP\n";
            retmsg->msgType = JOINREP;        
            //JOINREP = msgType + address + heartbeat + membershipList
            memcpy((char *)(retmsg+1), &toNode->addr.addr, sizeof(toNode->addr.addr));
            //memcpy((char *)(retmsg+1) + sizeof(toNode->addr.addr), &vSize, sizeof(size_t));
            memcpy((char *)(retmsg+1) + 1 + sizeof(toNode->addr.addr), &toNode->heartbeat, sizeof(long));  
            //memcpy((char *)(retmsg+1) + sizeof(toNode->addr.addr) + 1 + sizeof(long), &toNode->myPos->getid(), sizeof(toNode->memberList[0]));
            memcpy((char *)(retmsg+1) + sizeof(toNode->addr.addr) + 1 + sizeof(long), &(toNode->myPos->id), sizeof(int));
            memcpy((char *)(retmsg+1) + sizeof(toNode->addr.addr) + 1 + sizeof(long) + sizeof(int), &(toNode->myPos->port), sizeof(short));
            memcpy((char *)(retmsg+1) + sizeof(toNode->addr.addr) + 1 + sizeof(long) + sizeof(int) + sizeof(short), &(toNode->myPos->heartbeat), sizeof(long));
            memcpy((char *)(retmsg+1) + sizeof(toNode->addr.addr) + 1 + sizeof(long) + sizeof(int) + sizeof(short) + sizeof(long), &(toNode->myPos->timestamp), sizeof(long));


            
            
            //there is no fromNode set yet
            emulNet->ENsend(&toNode->addr,&fromNode->addr, (char *)retmsg, retmsgsize);      

        }
        cout<<"\n";
        cout<<"\n";

      } //if (msg->msgType == JOINREQ)

    //if JOINREP
    //add the node to the group
    if (msg->msgType == JOINREP){
        cout<<"Inside JOINREP\n";
        
        //toNode = (Member *)env; //2:0

        int size_of_vector;
        cout<<"JOINREP : env address = "<<toNode->addr.getAddress()<<endl;

        //getting fromNode from the JOINREP message
        memcpy(&id, data + sizeof(MessageHdr), sizeof(int));
        memcpy(&port, data + sizeof(MessageHdr) + sizeof(int), sizeof(short));
        memcpy(&tempaddr.addr, data + sizeof(MessageHdr), sizeof(tempaddr.addr));

        memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(tempaddr.addr) + 1, sizeof(long)); //1:0 heartbeat

        cout<<"JOINREP : Address = "<<tempaddr.getAddress()<<"\n";
        cout<<"JoinREP : heartbeat = "<<heartbeat<<"\n";
        

        ///////////////////////////////////
        toNode->inited   = true;
        //cout<<"check2\n";
        toNode->inGroup  = true;
        toNode->bFailed  = false;
        //fromNode->nnb      = 1;
        //cout<<"check1\n";
        toNode->heartbeat = toNode->heartbeat + 1; 
        //toNode->settimestamp(par->getcurrtime()); 
        toNode->pingCounter= 0;
        toNode->timeOutCounter = 0;
        //////////////////////////////////

        int message_size;
        memcpy(&message_size, data, sizeof(size_t));
        //cout<<"JoinRep: message_size = "<<message_size<<endl;
        //copying membershiplist for the first time
        //MemberListEntry *newlist = (MemberListEntry *)(data + sizeof(MessageHdr) + sizeof(tempaddr.addr) + sizeof(long) + 1);
        MemberListEntry *newlist = new MemberListEntry;
        memcpy(&newlist->id,(data + sizeof(MessageHdr) + sizeof(tempaddr.addr) + sizeof(long) + 1), sizeof(int));
        memcpy(&newlist->port,(data + sizeof(MessageHdr) + sizeof(tempaddr.addr) + sizeof(long) + 1 + sizeof(int)), sizeof(short));
        memcpy(&newlist->heartbeat, (data + sizeof(MessageHdr) + sizeof(tempaddr.addr) + sizeof(long) + 1 + sizeof(int) + sizeof(short)), sizeof(long));
        memcpy(&newlist->timestamp, (data + sizeof(MessageHdr) + sizeof(tempaddr.addr) + sizeof(long) + 1 + sizeof(int) + sizeof(short) + sizeof(long)), sizeof(long));
        //copying vector
        //memcpy(&newlist, data + sizeof(MessageHdr) + sizeof(tempaddr.addr) + sizeof(long) + 1, sizeof(newlist));
        //newlist->heartbeat = 
        cout<<"JoinRep : newlist->id = "<<newlist->id<<endl;
        cout<<"JoinRep : newlist->port = "<<newlist->port<<endl;
        cout<<"JoinRep : newlist->heartbeat = "<<newlist->heartbeat<<endl;

        //if (tempaddr->addr == newlist)
        toNode->memberList.push_back(*newlist);
        cout<<"\n";
        cout<<"\n";
        log->logNodeAdd((Address *)(&tempaddr), (Address *)(&toNode->addr));
        //Address naddr = getNodeAddress(newlist->id, newlist->port);
        //log->logNodeAdd((Address *)(&toNode->addr), (Address *)(&naddr));
    }
        
        //dummy message
    if (msg->msgType == DUMMYLASTMSGTYPE){
        int found_heartbeat = 0;
        //toNode = (Member *)env; //n:0
        //increasing its own heartbeat
        //toNode->heartbeat = toNode->heartbeat + 1;
        cout<<"dummymsg : env address = "<<toNode->addr.getAddress()<<endl;
        memcpy(&fromNode->addr.addr, data + sizeof(MessageHdr), sizeof(fromNode->addr.addr));
        cout<<"dummymsg : from Node Address = "<<fromNode->addr.getAddress()<<endl;
        memcpy(&fromNode->heartbeat, data + sizeof(MessageHdr) + sizeof(fromNode->addr.addr) + 1, sizeof(long));
        cout<<"dummymsg : from Node heartbeat = "<<fromNode->heartbeat<<endl;

        MemberListEntry *newEntry = new MemberListEntry;
        memcpy(&newEntry->id,(data + sizeof(MessageHdr) + sizeof(toNode->addr.addr) + 1 + sizeof(long)), sizeof(int));
        memcpy(&newEntry->port,(data + sizeof(MessageHdr) + sizeof(toNode->addr.addr) + 1 + sizeof(long) + sizeof(int)), sizeof(short));
        memcpy(&newEntry->heartbeat,(data + sizeof(MessageHdr) + sizeof(toNode->addr.addr) + 1 + sizeof(long) + sizeof(int) + sizeof(short)), sizeof(long));
        memcpy(&newEntry->timestamp,(data + sizeof(MessageHdr) + sizeof(toNode->addr.addr) + 1 + sizeof(long) + sizeof(int) + sizeof(short) + sizeof(long)), sizeof(long));

        //adding timestamp 
        cout<<"dummymsg : size of list = "<<toNode->memberList.size()<<"\n";
        for(toNode->myPos = toNode->memberList.begin(); toNode->myPos != toNode->memberList.end(); toNode->myPos++){              
            Address *timestamp_addr = new Address;
            memcpy(&timestamp_addr->addr[0] , &toNode->myPos->id, sizeof(int));
            memcpy(&timestamp_addr->addr[4] , &toNode->myPos->port, sizeof(short));
            //cout<<"dummymsg : toNode->myPos->addr = "<<printAddress(timestamp_addr)<<"\n";
            cout<<"dummymsg : timestamp_addr = ";
            printAddress(timestamp_addr);
            cout<<"dummymsg : fromNode->addr";
            printAddress(&fromNode->addr);

            if (*timestamp_addr == fromNode->addr){
                found_heartbeat =  1;
                //log->logNodeAdd((Address *)(&toNode->addr), (Address *)(&fromNode->addr));
                cout<<"dummymsg : fromNode heartbeat increase\n";
              if (fromNode->heartbeat > toNode->myPos->heartbeat){
                    //cout<<"increasing fromNode heartbeat\n";
                    log->logNodeAdd((Address *)(&toNode->addr), (Address *)(&fromNode->addr));
                    
                    toNode->myPos->setheartbeat(fromNode->heartbeat); 
                    toNode->myPos->settimestamp(par->getcurrtime());
              }  
            } else if (*timestamp_addr == toNode->addr) {
                    //increasing its own heartbeat
                    //cout<<"increasing toNode heartbeat\n";
                    //printAddress(timestamp_addr);
                    //toNode->myPos->setheartbeat(toNode->heartbeat);
                    //toNode->myPos->settimestamp(par->getcurrtime());
                    //toNode->timeOutCounter = 0;
            } else {
                    //increase the timeout counter
                    toNode->timeOutCounter = toNode->timeOutCounter + 1;
            }
            //checking the newentry membershiplist heartbeat
            if (toNode->myPos->id == newEntry->id && toNode->myPos->port == newEntry->port){
                //membershiplist of the fromNode and toNode comparison
                if (newEntry->heartbeat > toNode->myPos->heartbeat ){
                    toNode->myPos->setheartbeat(newEntry->heartbeat); 
                    toNode->myPos->settimestamp(par->getcurrtime());    
                }
            }
        }
        //not found in the list . add it with hearbeat
        if (!found_heartbeat){
            cout<<"dummymsg : creating new entry for node\n";
            printAddress(&fromNode->addr);
            MemberListEntry *newlist = new MemberListEntry;
            memcpy(&newlist->id, &fromNode->addr.addr[0], sizeof(int));
            memcpy(&newlist->port,&fromNode->addr.addr[4], sizeof(short));
            newlist->heartbeat = fromNode->heartbeat;
            newlist->timestamp = par->getcurrtime();
            toNode->memberList.push_back(*newlist);
            log->logNodeAdd((Address *)(&toNode->addr), (Address *)(&fromNode->addr));
        }
    }

    if (msg->msgType == RETIRENODE){
        cout<<"In RETIRENODE\n";
        //cout<<"toNode->memberList.size() = "<<toNode->memberList.size()<<"\n";
        cout<<"RETIRENODE : this node =";
        printAddress(&toNode->addr);
        Address *retireaddr = new Address;
        memcpy(&id, data + sizeof(MessageHdr), sizeof(int));
        memcpy(&port, data + sizeof(MessageHdr) + sizeof(int), sizeof(short));
        memcpy(&retireaddr->addr, data + sizeof(MessageHdr), sizeof(retireaddr->addr));
        cout<<"RETIRENODE : failing node = ";
        printAddress(retireaddr);
        cout<<id<<":"<<port<<"\n";
        //if(!toNode->bFailed && toNode->heartbeat != 0){
        //    log->logNodeRemove((Address *)(&toNode->addr), retireaddr);
        //}

        //retireaddr->addr[0] = id;
        //retireaddr->addr[4] = port;
        //checking if the current node is the retired node
        if(!toNode->bFailed && toNode->heartbeat != 10000){
            if (toNode->addr == *retireaddr){
                //fail the node
                cout<<"RETIRENODE : failing node =";
                printAddress(&toNode->addr);
                //log->logNodeRemove((Address *)(&toNode->addr), retireaddr);
                toNode->bFailed   = true;
                //toNode->inited    = false;
                toNode->inGroup   = false;
                toNode->heartbeat = 10000;
            } else{
                //check if the toNode is part of the memberList and delete it
                for(toNode->myPos = toNode->memberList.begin(); toNode->myPos != toNode->memberList.end(); toNode->myPos++){
                    cout<<"RETIRENODE : list address = "<<toNode->myPos->id<<":"<<toNode->myPos->port<<"\n";
                     if (toNode->myPos->id == id && toNode->myPos->port == port && toNode->myPos->heartbeat < 10000){
                        //delete the list item
                        cout<<"deleting node"<<id<<":"<<port<<"\n";
                        toNode->myPos->setheartbeat(10000);
                        toNode->myPos->settimestamp(par->getcurrtime());
                        //Address *raddr = new Address;
                        //(Address)raddr->addr = getNodeAddress(id, port);
                        //printAddress(raddr);
                        log->LOG(retireaddr, "RETIRENODE : retiring this address");
                        log->logNodeRemove((Address *)(&toNode->addr), retireaddr);
                        //if (memberNode->myPos != memberNode->memberList.end())
                            //toNode->memberList.erase(toNode->myPos);
                        //else 
                            //toNode->myPos++;
                    }
                }
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
    //if (!memberNode->bFailed)
    //    send_heartbeat();

    cout<<"Retiring Node in nodeLoopOps\n";
    int maxsize = memberNode->memberList.size();
    for(memberNode->myPos = memberNode->memberList.begin(); memberNode->myPos != memberNode->memberList.end(); memberNode->myPos++){
        cout<<"NODELOOP : memberlist contencts id = "<<memberNode->myPos->id<<":";
        cout<<memberNode->myPos->port<<"\n";
    }
    cout<<"NODELOOP : size before retire = "<<maxsize<<endl;
            
    //cout<<"memberNode->memberList.capacity() = "<<memberNode->memberList.capacity()<<"\n";
    MessageHdr *retiremsg = new MessageHdr;
    if (!memberNode->bFailed && memberNode->heartbeat < 10000){
        for(memberNode->myPos = memberNode->memberList.begin(); memberNode->myPos != memberNode->memberList.end(); memberNode->myPos++){
            if ((par->getcurrtime() - memberNode->myPos->timestamp) == TFAIL){
                //failing the node 
                //memberNode->myPos
            }
            //cout<<"nodeLoopOps : timecounter = "<<(par->getcurrtime() - memberNode->myPos->timestamp)<<"\n";
            cout<<"NODELOOP : memberNode->addr = "<<memberNode->addr.getAddress()<<"\n";
            //cout<<"TFAIL = "<<TFAIL<<"\n";
            cout<<"NODELOOP : memberNode->myPos->id = "<<memberNode->myPos->id<<"\n";
            cout<<"NODELOOP : memberNode->myPos->port = "<<memberNode->myPos->port<<"\n";
            cout<<"NODELOOP : memberNode->myPos->timestamp = "<<memberNode->myPos->timestamp<<"\n";
            cout<<"NODELOOP : par->getcurrtime() = "<<par->getcurrtime()<<"\n";
            if ((par->getcurrtime() - memberNode->myPos->timestamp) == TREMOVE && memberNode->myPos->heartbeat < 10000 && memberNode->inGroup){
                
                //memberNode->myPos->heartbeat = 10000;
                //select n elements
                Address *raddr = new Address;

                //raddr = getNodeAddress(memberNode->myPos->id, memberNode->myPos->port);
                //log->logNodeRemove((Address *)(&memberNode->addr), );
                maxsize = memberNode->memberList.size();
                cout<<"NODELOOP : creatine RETIREMESSAGE\n";
                cout<<"NODELOOP : maxsize = "<<maxsize<<"\n";
                Address *retireaddr = new Address;
                int id     = memberNode->myPos->id;
                short port = memberNode->myPos->port;
                retireaddr->addr[0] = id;
                retireaddr->addr[4] = port;
                cout<<"Printing retireaddr = ";
                printAddress(retireaddr);
                //log->logNodeRemove((Address *)(&memberNode->addr), retireaddr);
                cout<<"NODELOOP : "<<id<<":"<<port<<"\n";
                //removing the node from the memberNode memberList
                size_t retiresize =  sizeof(MessageHdr) + sizeof(retireaddr->addr) + sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long) + 1;
                retiremsg = (MessageHdr *)malloc(retiresize * sizeof(char));
                retiremsg->msgType = RETIRENODE;
                //memcpy((char *)(retiremsg + 1), &retireaddr->addr, sizeof(retireaddr->addr));
                memcpy((char *)(retiremsg + 1), &id, sizeof(int));
                memcpy((char *)(retiremsg + 1) + sizeof(int), &port, sizeof(short));
                memcpy((char *)(retiremsg + 1) + sizeof(retireaddr->addr) + 1, &(memberNode->myPos->id), sizeof(int));
                memcpy((char *)(retiremsg + 1) + sizeof(retireaddr->addr) + 1 + sizeof(int), &(memberNode->myPos->port), sizeof(short));
                memcpy((char *)(retiremsg + 1) + sizeof(retireaddr->addr) + 1 + sizeof(int) + sizeof(short), &(memberNode->myPos->heartbeat), sizeof(long));
                memcpy((char *)(retiremsg + 1) + sizeof(retireaddr->addr) + 1 + sizeof(int) + sizeof(short) + sizeof(long), &(memberNode->myPos->timestamp), sizeof(long));
                cout<<"NODELOOP : erasing node\n";
                //erase the node
                //if (memberNode->myPos != memberNode->memberList.end())
                //    memberNode->memberList.erase(memberNode->myPos);
                maxsize = memberNode->memberList.size();
                cout<<"NODELOOP : maxsize of the random variable = "<<maxsize<<"\n";;
                for(int i=0;i<maxsize; i++){
                    int ran_var = rand() % maxsize;
                    cout<<"ran_var = "<<ran_var<<"\n";
                    //creating address from the membership list entry of ran_var
                    //propogate to random members in the list
                    id = memberNode->memberList[i].id;
                    port = memberNode->memberList[i].port;
                    Address *newaddr = new Address;
                    memcpy(&(newaddr->addr[0]), &id, sizeof(int));
                    memcpy(&(newaddr->addr[4]), &port, sizeof(short));
                    //if (*newaddr == memberNode->addr)
                    //    continue;
                    cout<<"NODELOOP : new address = "<<newaddr->getAddress()<<"\n";
                    //if (newaddr)
                    emulNet->ENsend(&memberNode->addr, newaddr, (char *)retiremsg, retiresize);
                }
            }
        }
    }
    cout<<"\n";
    cout<<"\n";

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


Address MP1Node::getNodeAddress(int id, short port) {
    Address nodeaddr;

    //memset(nodeaddr, 0, sizeof(Address));
    *(int*)(nodeaddr.addr) = id;
    *(short*)(nodeaddr.addr[4]) = port;

    return nodeaddr;
}