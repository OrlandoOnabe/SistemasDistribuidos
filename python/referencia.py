import zmq
import mensagem_pb2

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5559")

rank = 0
lista_servidores = []
while True: 
    req = mensagem_pb2.ReqReferencia()
    req.ParseFromString(socket.recv())
    rep = mensagem_pb2.RepReferencia()
    if req.tipo_referencia == mensagem_pb2.REGISTRAR:
        servidor = req.nome_servidor
        if servidor not in lista_servidores:
            rank += 1
            lista_servidores[servidor] = rank

        rep.sucesso = True
        rep.rank = lista_servidores[servidor]
        print("Servidor registrado: ", servidor)
        print("Ranking: ", rep.rank)

    elif req.tipo_referencia == mensagem_pb2.LISTAGEM_SERVIDORES:
        rep.sucesso = True
        for servidor, rank in lista_servidores.items():
            servidor = rep.servidores.add()
            servidor.nome = servidor
            servidor.rank = rank

        print("Lista de servidores solicitada", flush=True)
        
    socket.send(rep.SerializeToString())
