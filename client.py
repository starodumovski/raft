
import grpc
import raft_pb2 as pb2
import raft_pb2_grpc as pb2_grpc
import re

reg_suspend = r"suspend \d+"
reg_connect = r"connect \d+.\d+.\d+.\d+:\d+"
reg_leader = r"getleader"

def parse_string(stub: pb2_grpc.NodeStub, message: str):
    try:
        if re.fullmatch(reg_suspend, message):
            response = stub.Suspend(pb2.SuspendRequest(period=int(message.split()[-1])))
            print("activated")
            return response
        if re.fullmatch(reg_leader, message):
            response = stub.GetLeader(pb2.VoteRequest())
            if response.nothing_id_vote == 0:
                print("No info")
            elif response.nothing_id_vote == 1:
                print(f"{response.info_1.leaderId} {response.info_1.ip_address}")
            elif response.nothing_id_vote == 2:
                print(response.info_2.votedId)
            return response
        return None
    except grpc._channel._InactiveRpcError:
        return -1

class Client:
    def __init__(self):
        self.stub_ = None
        self.channel_ = None
    
    def start(self):
        while True:
            line = input("> ")
            if len(line) != 0:
                if line == "quit":
                    break
                elif re.fullmatch(reg_connect, line):
                    if not self.connect(line.split(maxsplit=1)[-1]):
                        print("No Node with such address")
                elif self.stub_ is not None:
                    response = parse_string(self.stub_, line)
                    if response is None:
                        print("ERROR: unsupported command")
                    if response == -1:
                        print("Disconnected due inactivity from service side")
                        self.stub_ = None
                        self.channel_ = None

    def connect(self, ip_addr: str):
        channel = grpc.insecure_channel(ip_addr)
        try:
            stub = pb2_grpc.NodeStub(channel)
            self.channel_ = channel
            self.stub_ = stub
            return True
        except grpc._channel._InactiveRpcError:
            channel.close()
        self.stub_ = None
        self.channel_ = None
        return False


def main():
    client = Client()
    try:
        client.start()
    except KeyboardInterrupt:
        print("Shutting down")


if __name__ == "__main__":
    main()