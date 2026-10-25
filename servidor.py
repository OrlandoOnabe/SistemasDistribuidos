import zmq
import mensagem_pb2

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

while True:
    mensagem = socket.recv()
    req = mensagem_pb2.Request()
    req.ParseFromString(mensagem)
    print(f"Mensagem recebida: {req}", flush=True)
    socket.send(b"OK")
