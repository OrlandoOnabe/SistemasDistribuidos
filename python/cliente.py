import zmq
import mensagem_pb2
from google.protobuf.timestamp_pb2 import Timestamp

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://broker:5555")

req = mensagem_pb2.Request()
req.tiporeq = mensagem_pb2.LOGIN_USUARIO
req.login_usuario.usuario = "bot-python"
req.login_usuario.timestamp_req.GetCurrentTime()

print("Enviando:")
print(req, flush=True)
socket.send(req.SerializeToString())

resposta = socket.recv()
rep = mensagem_pb2.Reply()
rep.ParseFromString(resposta)
print("Resposta:")
print(rep, flush=True)

req = mensagem_pb2.Request()
req.tiporeq = mensagem_pb2.CRIAR_CANAL
req.criar_canal.canal_nome = "canal1"
req.criar_canal.timestamp_req.GetCurrentTime()

print("Enviando:")
print(req, flush=True)
socket.send(req.SerializeToString())

resposta = socket.recv()
rep = mensagem_pb2.Reply()
rep.ParseFromString(resposta)
print("Resposta:")
print(rep, flush=True)

req = mensagem_pb2.Request()
req.tiporeq = mensagem_pb2.LISTAGEM_CANAIS
req.listagem_canais.timestamp_req.GetCurrentTime()

print("Enviando:")
print(req, flush=True)
socket.send(req.SerializeToString())

resposta = socket.recv()
rep = mensagem_pb2.Reply()
rep.ParseFromString(resposta)
print("Resposta:")
print(rep, flush=True)