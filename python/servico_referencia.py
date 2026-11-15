import zmq
import mensagem_pb2
import time
from google.protobuf.timestamp_pb2 import Timestamp

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5559")

rank = 0
lista_servidores = {}
relogio = 0
ultimo_heartbeat = {}
TIMEOUT_HEARTBEAT = 60
print("Serviço de referência iniciado!", flush=True)
socket.setsockopt(zmq.RCVTIMEO, 1000)
while True: 
    for nome in list(ultimo_heartbeat):
        if time.monotonic() - ultimo_heartbeat[nome] > TIMEOUT_HEARTBEAT:
            print("Servidor inativo removido: " + nome, flush=True)
            ultimo_heartbeat.pop(nome, None)
            lista_servidores.pop(nome, None)

    try:
        msg = socket.recv()
    except zmq.Again:
        continue

    req = mensagem_pb2.ReqReferencia()
    req.ParseFromString(msg)

    relogio = max(relogio, req.relogio)
    rep = mensagem_pb2.RepReferencia()
    if req.tipo_referencia == mensagem_pb2.REGISTRAR:
        servidor = req.nome_servidor
        if servidor not in lista_servidores:
            rank += 1
            lista_servidores[servidor] = rank

        rep.sucesso = True
        rep.sucedido = "Servidor registrado!"
        rep.rank = lista_servidores[servidor]
        ultimo_heartbeat[req.nome_servidor] = time.monotonic()
        print("Servidor registrado: ", servidor)
        print("Ranking: ", rep.rank)

    elif req.tipo_referencia == mensagem_pb2.LISTAGEM_SERVIDORES:
        rep.sucesso = True
        for nome_servidor, rank_servidor in lista_servidores.items():
            servidor = rep.lista_servidores.add()
            servidor.nome_servidor = nome_servidor
            servidor.rank = rank_servidor

        print("Lista de servidores solicitada!", flush=True)

    elif req.tipo_referencia == mensagem_pb2.HEARTBEAT:
        if req.nome_servidor in lista_servidores:
            ultimo_heartbeat[req.nome_servidor] = time.monotonic()
            rep.sucesso = True
            rep.sucedido = "Hearbeat recebido!"
            print(f"Heartbeat recebido de {req.nome_servidor}", flush=True)
        else:
            rep.sucesso = False
            rep.sucedido = "Servidor não registrado!"

    relogio += 1
    rep.relogio = relogio
    rep.timestamp_ref.GetCurrentTime()
    socket.send(rep.SerializeToString())