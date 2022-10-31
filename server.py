import configparser
from enum import IntEnum, unique
from itertools import chain
from random import randint

from sys import argv
import re

import threading
import grpc
from concurrent import futures
from threading import Timer
import sched, time

import raft_pb2 as pb2
import raft_pb2_grpc as pb2_grpc


CONFIG_NAME = 'config.conf'
SERVER_ADDR = None
SERVER_PORT = None
SERVER_ID = None

SERVER_ADDRESS = {}
SERVICE_AMOUNT = 0

server_node = grpc.server(futures.ThreadPoolExecutor(max_workers=15))

time_start = 0
time_end = 0

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
        if re.fullmatch(reg_id,argv[-1]):
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
    return randint(150, 300) / 1000


# TODO: update the timeouts 
class Node(pb2_grpc.NodeServicer):

    def __init__(self, stop_event: threading.Event, node_id: str):
        self.id_ = node_id
        self.answer = True
        self.term = 0
        self.state = State.FOLLOWER
        self.stop_event = stop_event
        self.set_timeout = initialize_timeout()
        self.amount_of_votes = 0
        self.number_of_nodes = SERVICE_AMOUNT
        self.to_vote = True
        self.leaderId = None
        self.votedId = None
        self.list_of_stubs = {}
        self.leader_scheduler = sched.scheduler(time.time, time.sleep)

        for id_ in SERVER_ADDRESS.keys():
            if id_ != self.id_:
                channel = grpc.insecure_channel(SERVER_ADDRESS[id_])
                stub = pb2_grpc.NodeStub(channel)
                self.list_of_stubs[id_] = stub



    def Suspend(self, request, context):
        if self.answer is False:
            return
        self.answer = False
        self.raise_term(self.term, state_reset=State.FOLLOWER, timeout_reset=True)
        global time_start
        print(f"slept")
        time.sleep(request.period)
        # self.stop_event.clear()
        self.answer = True
        self.stop_event.set()
        # time_start = time.time()
        print(f"woke up after {request.period}")
        return pb2.SuspendResponse()
    
    def RequestVote(self, request, context):
        if self.answer is False:
            return
        # self.stop_event.set()
        if request.term > self.term:
            # self.set_timeout = initialize_timeout()
            if self.state == State.FOLLOWER:
                timeout_reset = False
            else:
                timeout_reset = True
            self.raise_term(request.term, state_reset=State.FOLLOWER, timeout_reset=timeout_reset, leaderId=self.leaderId)
            print(f"I am {State.FOLLOWER.name} now")
            self.stop_event.set()
        elif request.term < self.term:
            return pb2.VoteResponse(term=self.term, vote=False)
        return pb2.VoteResponse(term=self.term, vote=self.vote_for(request.candidateId))

    def AppendEntries(self, request, context):
        if self.answer is False:
            return
        self.stop_event.set()
        if request.term > self.term:
            self.raise_term(request.term, state_reset=State.FOLLOWER, timeout_reset=True, leaderId=request.leaderId)
            self.to_vote = False
            print(f"I am {State.FOLLOWER.name} now")
            self.stop_event.set()
            return pb2.AppendResponse(term=self.term, success=True)
        if request.term == self.term:
            # if self.state == State.LEADER:
            #     return pb2.AppendResponse(term=self.term, success=False)
            if self.state == State.CANDIDATE or self.state == State.LEADER:
                self.raise_term(request.term, state_reset=State.FOLLOWER, timeout_reset=True, leaderId=request.leaderId)
                self.to_vote = True
                print(f"I am {State.FOLLOWER.name} now {self.set_timeout}")
                # self.stop_event.set()
                # return pb2.AppendResponse(term=self.term, success=True)
            self.to_vote = False
            self.leaderId = request.leaderId
            self.stop_event.set()
            return pb2.AppendResponse(term=self.term, success=True)
        self.stop_event.set()    
        return pb2.AppendResponse(term=self.term, success=False)

    def GetLeader(self, request, context):
        if self.answer is False:
            return
        if self.state == State.LEADER:
            return pb2.GetLeaderResponse(nothing_id_vote=1, info_1=pb2.GetID(leaderId=SERVER_ID, ip_address=f"{SERVER_ADDR}:{SERVER_PORT}"))
        if self.leaderId:
             return pb2.GetLeaderResponse(nothing_id_vote=1, info_1=pb2.GetID(leaderId=self.leaderId, ip_address=SERVER_ADDRESS[self.leaderId]))
        if self.votedId:
            return pb2.GetLeaderResponse(nothing_id_vote=2, info_2=pb2.GetVoted(votedId=self.votedId))
        return pb2.GetLeaderResponse(nothing_id_vote=0, info_0=pb2.GetNothing())

    def candidate_state(self):
        if self.state == State.CANDIDATE:
            for key in self.list_of_stubs.keys():
                if self.state == State.CANDIDATE:
                    try:
                        response = self.list_of_stubs[key].RequestVote(pb2.VoteRequest(term=self.term, candidateId=self.id_))
                        if response.vote is False:
                            self.raise_term(response.term, state_reset=State.FOLLOWER, timeout_reset=True)
                            print(f"I am {State.FOLLOWER.name} now {self.set_timeout}")
                            self.scheduler_lock()
                            return
                        self.amount_of_votes += 1
                        # self.check_for_won_election()
                    except grpc._channel._InactiveRpcError:
                        pass
                else:
                    self.scheduler_lock()
                    return
            self.check_for_won_election()

    # TODO: make a parallelism of requesting
    def leader_state(self):
        if self.state == State.LEADER and self.answer:
            for key in self.list_of_stubs.keys():
                if self.state == State.LEADER and self.answer:
                    try:
                        response = self.list_of_stubs[key].AppendEntries(pb2.AppendRequest(term=self.term, leaderId=self.id_))
                        if response.success is False:
                            self.raise_term(response.term, state_reset=State.FOLLOWER, timeout_reset=True)
                            print(f"I am {self.state.name} now {self.set_timeout}")
                            self.scheduler_lock()
                            return
                    except grpc._channel._InactiveRpcError:
                        pass
                else:
                    self.scheduler_lock()
                    return
            self.leader_scheduler.enter(0.05, 0.05, self.leader_state)

    def raise_term(self, term: int, state_reset: State = None,
                    timeout_reset: bool = False, leaderId = None, set_event: bool = True):
        prev_state = self.state
        prev_term  = self.term
        self.term = term
        if state_reset is not None:
            self.state = state_reset
        self.leaderId = leaderId
        if self.state == State.CANDIDATE:
            self.votedId = self.id_
            self.to_vote = False
            self.amount_of_votes = 1
        elif self.state == State.LEADER:
            self.votedId = None
            self.to_vote = False
        else:
            self.amount_of_votes = 0
            if self.leaderId is None:
                # self.votedId = self.voted
                if prev_term < self.term:
                    self.votedId = None
                    self.to_vote = True
                else:
                    self.to_vote = self.to_vote
                    self.votedId = self.votedId
            else:
                if prev_term < self.term:
                    self.votedId = None
                    self.to_vote = False
                else:
                    self.votedId = None
                    self.to_vote = True
        if timeout_reset is True:
            if self.state == State.LEADER:
                self.set_timeout = None
            elif self.state == State.CANDIDATE:
                self.set_timeout = 0.3
            else:
                self.set_timeout = initialize_timeout()
        if set_event is True:
            self.stop_event.set() # ???
        # print(f" I am {self.state.name} now")
        
    def vote_for(self, candidateId):
        if self.to_vote is True:
            self.votedId = candidateId
            self.leaderId = None
            self.to_vote = False
            return True
        return False
    
    def check_for_won_election(self):
        if self.amount_of_votes >= self.number_of_nodes // 2 + 1:
                self.raise_term(self.term, state_reset=State.LEADER, timeout_reset=True)
                print(f"I am {State.LEADER.name} {self.set_timeout} {self.amount_of_votes}")
                self.scheduler_lock()
                self.leader_scheduler.enter(0.05, 0.05, self.leader_state)
                self.leader_scheduler.run()
    
    def scheduler_lock(self):
        for event in self.leader_scheduler.queue:
            self.leader_scheduler.cancel(event)




def server_launch(server_node: grpc.Server, stop_event: threading.Event, node: Node):
    global time_start, time_end
    
    time_start = time.time()
    while True:
        set_timeout = node.set_timeout
        if stop_event.wait(timeout=set_timeout):
            if node.answer:
                stop_event.clear()
                t_end = time.time()
                # print(f"bee = {t_end - time_start}")
                time_start = time.time()
            continue
        else:
            time_end = time.time()
            if node.state == State.FOLLOWER:
                # print(f"I am {State.CANDIDATE.name} now")
                node.raise_term(node.term + 1, state_reset=State.CANDIDATE, timeout_reset=True, set_event=False)
                print(f"I am {node.state.name} now (was FOLLOWER)")
                node.leader_scheduler.enter(0.05, 0.05, node.candidate_state)
                node.leader_scheduler.run()
            elif node.state == State.CANDIDATE:
                if node.amount_of_votes < node.number_of_nodes // 2 + 1:
                    node.raise_term(node.term, state_reset=State.FOLLOWER, timeout_reset=True, set_event=False)
                    # print(f"I am {State.FOLLOWER.name} now")
                    print(f"I am {node.state.name} now")
                else:
                    print("I am leader")
                    node.raise_term(node.term, state_reset=State.LEADER, timeout_reset=None, set_event=False)
                    print(f"I am {node.state.name}")
                    node.leader_scheduler.enter(0.05, 0.05, node.leader_state)
                    node.leader_scheduler.run()
            # print(f"bee = {time_end - time_start}")
            time_start = time.time()


def main():
    server_node = grpc.server(futures.ThreadPoolExecutor(max_workers=15))
    stop_event = threading.Event()
    node = Node(stop_event, argv[-1])
    try:
        pb2_grpc.add_NodeServicer_to_server(node, server_node)
        server_node.add_insecure_port(f"{SERVER_ADDR}:{SERVER_PORT}")
        server_node.start()
        print(f"Listen the IPaddress: {SERVER_ADDR}:{SERVER_PORT}")
        server_launch(server_node, stop_event, node)
    except KeyboardInterrupt:
        pass
    finally:
        server_node.stop(None)
        print("Shutting Down")


if __name__ == '__main__':
    if initialize_parameters() is False:
        print('Try again...')
        exit(0)
    main()