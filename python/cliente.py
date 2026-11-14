import zmq
import mensagem_pb2
from google.protobuf.timestamp_pb2 import Timestamp
import random
from datetime import datetime, timezone
import time

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://broker:5555")
sub = context.socket(zmq.SUB)
sub.connect("tcp://proxy:5558")

canais_inscritos = []
publicacoes = ["Sistemas distribuidos", "Gestão de Projetos de Software", "Estrutura de dados", "Automatos", "Calculo 3"]
poller = zmq.Poller()
poller.register(sub, zmq.POLLIN)

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

canais = list(rep.listagem_canais.canais)
if len(canais) < 5:
    num = random.randint(1, 100)
    novo_canal = f"canal {num}"
    req = mensagem_pb2.Request()
    req.tiporeq = mensagem_pb2.CRIAR_CANAL
    req.criar_canal.canal_nome = novo_canal
    req.criar_canal.timestamp_req.GetCurrentTime()

    print("Enviando:")
    print(req, flush=True)
    socket.send(req.SerializeToString())

    resposta = socket.recv()
    rep = mensagem_pb2.Reply()
    rep.ParseFromString(resposta)
    print("Resposta:")
    print(rep, flush=True)
    canais.append(novo_canal)

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

if len(canais_inscritos) < 3:
    canais_disponiveis = []
    for canal in canais:
        if canal not in canais_inscritos:
            canais_disponiveis.append(canal)
    if canais_disponiveis:
        canal_escolhido = random.choice(canais_disponiveis)
        sub.setsockopt_string(zmq.SUBSCRIBE, canal_escolhido)
        canais_inscritos.append(canal_escolhido)
        print("Inscrito no canal:", canal_escolhido, flush=True)
    else:
        print("Nenhum canal disponível!", flush=True)

while True:
    canal_escolhido = random.choice(canais)
    print(f"\nPublicando no canal: {canal_escolhido}", flush=True)

    for i in range(10):
        texto = random.choice(publicacoes)
        req = mensagem_pb2.Request()
        req.tiporeq = mensagem_pb2.PUBLICAR
        req.publicar.nome_canal = canal_escolhido
        req.publicar.publicacao = texto
        req.publicar.timestamp_req.GetCurrentTime()
        socket.send(req.SerializeToString())

        rep = mensagem_pb2.Reply()
        rep.ParseFromString(socket.recv())
        print("Resposta:", rep.publicar.sucesso, flush=True)

        pubicacoes_incritas = dict(poller.poll(timeout=0))
        if sub in pubicacoes_incritas:
            topico, dados = sub.recv_multipart()
            publicacao = mensagem_pb2.EnviarPub()
            publicacao.ParseFromString(dados)

            print("\nPublicação recebida:", flush=True)
            print("Topico:", publicacao.topico, flush=True)
            print("Mensagem:", publicacao.publicacao, flush=True)
            print("Enviada em:", publicacao.timestamp_pub.ToDatetime(tzinfo=timezone.utc), flush=True)
            print("Recebida em:", datetime.now(timezone.utc), flush=True)

        time.sleep(1)