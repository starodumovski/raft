import configparser
import random
from enum import IntEnum, unique
from itertools import chain
from random import randint
from collections import Counter

from sys import argv
import re

import threading
import grpc
from concurrent import futures
import sched, time
import multiprocessing

import raft_pb2 as pb2
import raft_pb2_grpc as pb2_grpc

CONFIG_NAME = 'config.conf'
SERVER_ADDR = None
SERVER_PORT = None
SERVER_ID = None

SERVER_ADDRESS = {}
SERVICE_AMOUNT = 0

HOW_MUCH_SEND = 5


@unique
class State(IntEnum):
    FOLLOWER = -1,
    CANDIDATE = 0,
    LEADER = 1


def define_address(init_id):
    defined = False
    parser = configparser.ConfigParser(allow_no_value=True)
    with open(CONFIG_NAME) as lines:
        lines = chain(('[DEFAULT]',), lines)  # This line does the trick.
        parser.read_file(lines)
    values = parser.items('DEFAULT')
    global SERVER_ADDRESS, SERVICE_AMOUNT
    for value in values:
        SERVICE_AMOUNT += 1
        id_, ip_addr_, port_ = value[0].split()
        if init_id == id_:
            global SERVER_ADDR, SERVER_ID, SERVER_PORT
            SERVER_ADDR = ip_addr_
            SERVER_ID = id_
            SERVER_PORT = port_
            defined = True
        else:
            SERVER_ADDRESS[id_] = "{}:{}".format(ip_addr_, port_)
    return defined


def initialize_parameters() -> bool:
    if len(argv) == 2:
        reg_id = r"\d+"
        if re.fullmatch(reg_id, argv[-1]):
            if not define_address(argv[1]):
                print("No such id in configuration file")
            else:
                print(SERVER_ID, SERVER_ADDR, SERVER_PORT)
                return True
        else:
            print("Bad id")
    elif len(argv) > 2:
        print("Too lot parameters")
    else:
        print("too few parameters")
    return False


def initialize_timeout():
    # return 1000
    return randint(200, 400) / 1000


class Node(pb2_grpc.NodeServicer):
    def __init__(self, stop_event: threading.Event, node_id: str, queue: multiprocessing.Queue):
        # main attributes
        self.id_ = node_id
        self.state = State.FOLLOWER
        self.term = 0

        self.to_vote = True
        self.leaderId = None
        self.votedId = None

        # if suspend, then False
        self.answer = True

        # election timeout and reset-timeout event
        self.set_timeout = initialize_timeout()
        self.stop_event = stop_event

        # CANDIDATE items to manipulate with
        self.amount_of_votes = 0
        self.number_of_nodes = SERVICE_AMOUNT

        # LEADER items
        self.list_of_stubs = {}
        self.addresses = []
        self.server_works = True  # threads of send_message
        self.queue = queue

        # scheduler for LEADER (each 50 ms) and CANDIDATE (once)
        self.leader_scheduler = sched.scheduler(time.time, time.sleep)

        # key-value
        self.storage = {}
        self.log = {0: None}
        self.nextIndex = {}
        self.matchIndex = {}

        self.commitIndex = 0
        self.appliedIndex = 0
        self.state_machine = False

        for id_ in SERVER_ADDRESS.keys():
            if id_ != self.id_:
                self.nextIndex[id_] = 0
                self.matchIndex[id_] = 0
                self.addresses.append(SERVER_ADDRESS[id_])
                channel = grpc.insecure_channel(SERVER_ADDRESS[id_])
                stub = pb2_grpc.NodeStub(channel)
                self.list_of_stubs[id_] = (stub, channel, SERVER_ADDRESS[id_],)

    def Suspend(self, request, context):
        if self.answer is False:
            return
        self.answer = False
        print(f"Command from client: suspend {request.period}")
        print(f"Sleeping for {request.period} seconds")
        time.sleep(request.period)
        self.answer = True
        self.stop_event.set()
        return pb2.SuspendResponse()

    def RequestVote(self, request, context):
        if self.answer is False:
            return
        if request.term > self.term:
            if self.state == State.FOLLOWER:
                timeout_reset = False
            else:
                timeout_reset = True
            self.raise_term(request.term, state_reset=State.FOLLOWER, timeout_reset=timeout_reset)
        if request.term < self.term:
            return pb2.VoteResponse(term=self.term, vote=False)
        elif self.to_vote is False:
            return pb2.VoteResponse(term=self.term, vote=False)
        # elif request.lastLogIndex < self.lastLogIndex:
        elif request.lastLogIndex == 0:
            return pb2.VoteResponse(term=self.term, vote=self.vote_for(request.candidateId))
        elif request.lastLogIndex < len(self.log) - 1:
            return pb2.VoteResponse(term=self.term, vote=False)
        # elif request.lastLogIndex == self.lastLogIndex and request.lastLogTerm < self.lastLogTerm:
        elif request.lastLogIndex == len(self.log) - 1 and request.lastLogTerm < self.log[len(self.log) - 1].term:
            return pb2.VoteResponse(term=self.term, vote=False)
        return pb2.VoteResponse(term=self.term, vote=self.vote_for(request.candidateId))

    def check_conflicts(self, request):
        for i in range(len(request.entries)):
            if request.entries[i].index in self.log:
                if self.log[request.entries[i].index].term != request.entries[i].term:
                    self.log = {k:v for k,v in self.log.items() if k < request.entries[i].index}

    def append_new_entries(self, request):
        for i in range(len(request.entries)):
            if request.entries[i].index not in self.log:
                self.log[request.entries[i].index] = request.entries[i]

    def AppendEntries(self, request, context):
        if self.answer is False:
            return
        self.stop_event.set()

        if request.term > self.term:
            if self.state == State.FOLLOWER:
                set_timeout = False
            else:
                set_timeout = True
            self.raise_term(request.term, state_reset=State.FOLLOWER, timeout_reset=set_timeout,
                            leaderId=request.leaderId)
        elif self.term > request.term:
            return pb2.AppendResponse(term=self.term, success=False)
        if len(request.entries) > 0:
            if request.prevLogIndex != 0:
                if request.prevLogIndex not in self.log:
                    return pb2.AppendResponse(term=self.term, success=False)
                else:
                    if self.log[request.prevLogIndex].term != request.prevLogTerm:
                        return pb2.AppendResponse(term=self.term, success=False)
        self.check_conflicts(request)
        self.append_new_entries(request)
        if request.leaderCommit > self.commitIndex:
            self.commitIndex = min(request.leaderCommit, len(self.log) - 1)
        if not self.state_machine:
            self.state_machine = True
        self.to_vote = False
        self.leaderId = request.leaderId
        self.votedId = None
        self.amount_of_votes = 0
        self.stop_event.set()
        return pb2.AppendResponse(term=self.term, success=True)


    def GetLeader(self, request, context):
        if self.answer is False:
            return
        print(f"Command from client: getleader")
        if self.state == State.LEADER:
            print(f"{SERVER_ID} {SERVER_ADDR}:{SERVER_PORT}")
            return pb2.GetLeaderResponse(nothing_id_vote=1, info_1=pb2.GetID(leaderId=SERVER_ID,
                                                                             ip_address=f"{SERVER_ADDR}:{SERVER_PORT}"))
        if self.leaderId:
            print(f"{self.leaderId} {SERVER_ADDRESS[self.leaderId]}")
            return pb2.GetLeaderResponse(nothing_id_vote=1, info_1=pb2.GetID(leaderId=self.leaderId,
                                                                             ip_address=SERVER_ADDRESS[self.leaderId]))
        if self.votedId:
            print(f"I have no leader. Voted for {self.votedId}")
            return pb2.GetLeaderResponse(nothing_id_vote=2, info_2=pb2.GetVoted(votedId=self.votedId))
        return pb2.GetLeaderResponse(nothing_id_vote=0, info_0=pb2.GetNothing())

    def SetVal(self, request, context):
        print(f"Command from client: setval")
        if self.state == State.LEADER:
            is_success = True
            try:
                self.log[len(self.log)] = pb2.LogEntry(
                    term=self.term,
                    index=len(self.log), 
                    command=pb2.Command(name='SetVal', key=request.key, value=request.value))
            except:
                is_success = False
            return pb2.SetValResponse(success=is_success)
        elif self.state == State.CANDIDATE:
            return pb2.SetValResponse(success=False)
        elif self.state == State.FOLLOWER:
            if self.leaderId is not None:
                leader_stub = self.list_of_stubs[self.leaderId][0]
                return leader_stub.SetVal(request)
            else:
                return pb2.SetValResponse(success=False)
        

    def GetVal(self, request, context):
        print(f"Command from client: getval")
        return pb2.GetValResponse(success=(request.key in self.storage), value=self.storage.get(request.key))

    def candidate_state(self):
        if self.state == State.CANDIDATE:
            for key in self.list_of_stubs.keys():
                if self.state == State.CANDIDATE:
                    try:
                        response = self.list_of_stubs[key][0].RequestVote(
                            pb2.VoteRequest(term=self.term,
                                            candidateId=self.id_,
                                            lastLogIndex=len(self.log) - 1,
                                            lastLogTerm=self.log[len(self.log) - 1].term 
                                            if len(self.log) > 1 else 0), timeout=0.3)
                        if response.vote is False:
                            # if self.state == State.CANDIDATE:
                            self.raise_term(response.term, state_reset=State.FOLLOWER, timeout_reset=True)
                            self.scheduler_lock()
                            return
                        self.amount_of_votes += 1
                    except grpc._channel._InactiveRpcError:
                        pass
                else:
                    self.scheduler_lock()
                    break
            if self.state == State.CANDIDATE:
                self.check_for_won_election()

    def find_commitIndex(self, list_: list):
        vals = sorted(Counter(list_).most_common(), key=lambda x: x[0], reverse=True)
        commitIndex = 0
        all_sum = 0
        for tup in vals:
            all_sum += tup[1]
            if all_sum >= (SERVICE_AMOUNT - 1) // 2:
                commitIndex = tup[0]
                return commitIndex
        return commitIndex
    
    def apply_to_state_machine(self):
        while True:
            if self.state_machine:
                if self.state == State.LEADER:
                    commit_idx = self.find_commitIndex(self.matchIndex.values())
                else:
                    commit_idx = self.commitIndex
                if self.commitIndex < commit_idx:
                    self.commitIndex = commit_idx
                if self.commitIndex > self.appliedIndex:
                    for i in range(self.appliedIndex + 1, self.commitIndex + 1):
                        self.storage[self.log[i].command.key] = self.log[i].command.value
                    self.appliedIndex = self.commitIndex
                    self.state_machine = False

    def leader_state(self):
        try:
            if self.state_machine == False:
                self.state_machine = True
            if self.state == State.LEADER and self.answer is True:
                for key in self.list_of_stubs.keys():
                    if self.state == State.LEADER and self.answer is True:
                        self.queue.put(key)
                    else:
                        self.scheduler_lock()
                        return
                if self.state == State.LEADER and self.answer is True:
                    self.leader_scheduler.enter(0.05, 0.05, self.leader_state)
        except Exception:
            pass

    def send_message(self):
        try:
            while self.server_works:
                key = self.queue.get()
                if self.state == State.LEADER and self.answer is True:
                    try:
                        entries=[self.log.get(i) for i in range(self.matchIndex[key] + 1, 
                                                                self.matchIndex[key] + HOW_MUCH_SEND)
                                if self.log.get(i) is not None]
                        message_to_send = pb2.AppendRequest(term=self.term,
                                                            leaderId=self.id_,
                                                            prevLogIndex=self.matchIndex[key],
                                                            prevLogTerm=0 if self.matchIndex[key] == 0 
                                                                        else self.log[self.matchIndex[key]].term,
                                                            entries=entries,
                                                            leaderCommit=self.commitIndex)
                        response = self.list_of_stubs[key][0].AppendEntries(message_to_send)
                        if response.success is False:
                            if self.state == State.LEADER:
                                if response.term > self.term:
                                    self.raise_term(response.term, state_reset=State.FOLLOWER, timeout_reset=True)
                                    self.scheduler_lock()
                                else:
                                    if self.matchIndex[key] < HOW_MUCH_SEND:
                                        self.matchIndex[key] = 0 
                                    else:
                                        self.matchIndex[key] -= HOW_MUCH_SEND
                        else:
                            if self.state == State.LEADER:
                                self.matchIndex[key] += len(entries)
                    except grpc._channel._InactiveRpcError:
                        pass
                else:
                    self.scheduler_lock()
        except KeyboardInterrupt or ValueError:
            pass

    def raise_term(self, term: int, state_reset: State = None,
                   timeout_reset: bool = False, leaderId=None, set_event: bool = True):
        # state control: case of FOLLOWER -> FOLLOWER
        if state_reset is None:
            prev_state = None
        else:
            prev_state = self.state
            self.state = state_reset
        # term control: case of FOLLOWER -> FOLLOWER
        prev_term = self.term
        self.term = term

        if self.state == State.CANDIDATE:
            print("Leader is dead")
            print(f"I am a candidate. Term: {self.term}")
            print(f"Voted for node {self.id_}")
            self.lead_vote_id_amount(to_vote=False, votedId=self.id_, amount_of_votes=1)
        elif self.state == State.LEADER:
            print(f"I am a leader. Term: {self.term}")
            self.lead_vote_id_amount(leaderId=self.id_, to_vote=False, amount_of_votes=self.amount_of_votes)
        else:
            if prev_state == State.FOLLOWER:
                if leaderId is not None:
                    self.lead_vote_id_amount(leaderId=leaderId, to_vote=False)
                else:
                    if prev_term < self.term:
                        self.lead_vote_id_amount(leaderId=self.leaderId)
                    else:
                        self.lead_vote_id_amount(leaderId=self.leaderId, to_vote=self.to_vote, votedId=self.votedId)
            else:
                print(f"I am a {self.state.name.lower()}. Term: {self.term}")
                if leaderId is not None:
                    self.lead_vote_id_amount(leaderId=leaderId, to_vote=False)
                else:
                    self.lead_vote_id_amount()
        if timeout_reset is True:
            if self.state == State.LEADER:
                self.set_timeout = None
            elif self.state == State.CANDIDATE:
                self.set_timeout = 0.3
            else:
                self.set_timeout = initialize_timeout()
        if set_event is True:
            self.stop_event.set()

    def lead_vote_id_amount(self, leaderId=None, to_vote=True, votedId=None, amount_of_votes=0):
        self.leaderId = leaderId
        self.to_vote = to_vote
        self.votedId = votedId
        self.amount_of_votes = amount_of_votes

    def vote_for(self, candidateId):
        if self.to_vote is True:
            self.votedId = candidateId
            self.leaderId = None
            self.to_vote = False
            self.amount_of_votes = 0
            print(f"Voted for node {candidateId}")
            return True
        return False

    def check_for_won_election(self):
        if self.amount_of_votes >= self.number_of_nodes // 2 + 1:
            print('Votes received')
            self.raise_term(self.term, state_reset=State.LEADER, timeout_reset=True)
            self.scheduler_lock()
            self.leader_scheduler.enter(0.05, 0.05, self.leader_state)
            self.leader_scheduler.run()

    def scheduler_lock(self):
        for event in self.leader_scheduler.queue:
            self.leader_scheduler.cancel(event)
        while not self.queue.empty():
            self.queue.get()


def retry_wait(stop_event, timeout, max_retries=10):
    for _ in range(max_retries):
        if stop_event.wait(timeout):
            return True
        time.sleep(randint(25, 75) / 1000)
    return False


# server loop
def server_launch(stop_event: threading.Event, node: Node):
    print(f"I am a {node.state.name.lower()}. Term: {node.term}")

    retry_wait(stop_event, random.randint(100, 200) / 1000, max_retries=20)

    while True:
        set_timeout = node.set_timeout
        # set event (return true)
        if retry_wait(stop_event, set_timeout, max_retries=1) is True:
            if node.answer is True:
                stop_event.clear()
            continue
        # timeout appears
        else:
            if node.answer is False:
                continue
            # becoming a CANDIDATE
            if node.state == State.FOLLOWER:
                node.raise_term(node.term + 1, state_reset=State.CANDIDATE, timeout_reset=True, set_event=False)
                node.leader_scheduler.enter(0.05, 0.05, node.candidate_state)
                node.leader_scheduler.run()
            # becoming a LEADER or again FOLLOWER
            elif node.state == State.CANDIDATE:
                if node.amount_of_votes < node.number_of_nodes // 2 + 1:
                    print("Votes received")
                    node.raise_term(node.term, state_reset=State.FOLLOWER, timeout_reset=True, set_event=False)
                else:
                    node.raise_term(node.term, state_reset=State.LEADER, timeout_reset=True, set_event=False)
                    node.leader_scheduler.enter(0.05, 0.05, node.leader_state)
                    node.leader_scheduler.run()


def main():
    server_node = grpc.server(futures.ThreadPoolExecutor(max_workers=15))
    stop_event = threading.Event()
    queue = multiprocessing.Queue(SERVICE_AMOUNT)  # queue of connections
    thread_pool = []  # threads to serve the clients
    node = Node(stop_event, argv[-1], queue)
    update_entries = threading.Thread(target=node.apply_to_state_machine, daemon=True)
    update_entries.start()
    for _ in range(SERVICE_AMOUNT):
        worker = threading.Thread(target=node.send_message, daemon=True)
        worker.start()
        thread_pool.append(worker)

    try:
        pb2_grpc.add_NodeServicer_to_server(node, server_node)
        server_node.add_insecure_port(f"{SERVER_ADDR}:{SERVER_PORT}")
        server_node.start()
        print(f"The server starts at {SERVER_ADDR}:{SERVER_PORT}")
        time.sleep(1)
        # print(f"Listen the IPaddress: {SERVER_ADDR}:{SERVER_PORT}")
        server_launch(stop_event, node)
    except KeyboardInterrupt:
        pass
    finally:
        node.server_works = False
        queue.close()
        server_node.stop(None)
        print("Shutting Down")


if __name__ == '__main__':
    if initialize_parameters() is False:
        print('Try again...')
        exit(0)
    main()
