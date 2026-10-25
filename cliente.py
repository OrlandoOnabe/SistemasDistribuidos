import zmq
import mensagem_pb2
from google.protobuf.timestamp_pb2 import Timestamp

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://servidor:5555")

req = mensagem_pb2.Request()
req.tiporeq = mensagem_pb2.LOGIN_USUARIO
req.login_usuario.usuario = "bot1"
req.login_usuario.timestamp_req.GetCurrentTime()

print("Enviando:")
print(req, flush=True)

socket.send(req.SerializeToString())

resposta = socket.recv()
print(f"Resposta: {resposta}")