import zmq
import mensagem_pb2
import sqlite3
from datetime import datetime, timezone
import time

relogio = 0
qtd_msg = 0
relogio_dif = 0.0

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")
pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5557")
servico_ref = context.socket(zmq.REQ)
servico_ref.connect("tcp://referencia:5559")

req_ref = mensagem_pb2.ReqReferencia()
req_ref.tipo_referencia = mensagem_pb2.REGISTRAR
req_ref.nome_servidor = "servidor-python"
relogio += 1
req_ref.relogio = relogio
servico_ref.send(req_ref.SerializeToString())

rep_ref = mensagem_pb2.RepReferencia()
rep_ref.ParseFromString(servico_ref.recv())
relogio = max(relogio, rep_ref.relogio)
print(rep_ref.sucedido, flush=True)
print("Ranking: ", rep_ref.rank, flush=True)

req_ref = mensagem_pb2.ReqReferencia()
req_ref.tipo_referencia = mensagem_pb2.LISTAGEM_SERVIDORES
req_ref.nome_servidor = "servidor-python"
relogio += 1
req_ref.relogio = relogio
servico_ref.send(req_ref.SerializeToString())

rep_ref = mensagem_pb2.RepReferencia()
rep_ref.ParseFromString(servico_ref.recv())
relogio = max(relogio, rep_ref.relogio)

print("Lista de servidores registrados:", flush=True)
for servidor in rep_ref.lista_servidores:
    print(f"Nome: {servidor.nome_servidor} | Rank: {servidor.rank}", flush=True)

conexao = sqlite3.connect('/app/dados/python.db')
cursor = conexao.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS logins (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario TEXT NOT NULL,
        timestamp_login TEXT NOT NULL
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS canais (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL UNIQUE,
        timestamp_criacao TEXT NOT NULL
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS publicacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topico TEXT NOT NULL,
        mensagem TEXT NOT NULL,
        timestamp_pub TEXT NOT NULL
    )
""")
conexao.commit()

while True:
    mensagem = socket.recv()
    req = mensagem_pb2.Request()
    req.ParseFromString(mensagem)
    relogio = max(relogio, req.relogio)
    qtd_msg += 1
    print("Relógio requisição:", req.relogio, flush=True)
    print("Relógio servidor:", relogio, flush=True)
    print(f"Mensagem recebida: {req}", flush=True)
    tiporeq = req.WhichOneof("requisicao")
    rep = mensagem_pb2.Reply()

    if tiporeq == "login_usuario":
        rep.login_usuario.logado = True
        rep.login_usuario.sucesso = "Login realizado!"
        rep.login_usuario.timestamp_rep.FromNanoseconds(int((time.time() + relogio_dif) * 1_000_000_000))
        usuario = req.login_usuario.usuario
        tempo_login = datetime.fromtimestamp(time.time() + relogio_dif, timezone.utc).isoformat()

        cursor.execute(
            "INSERT INTO logins (usuario, timestamp_login) VALUES (?, ?)",
            (usuario, tempo_login)
        )
        conexao.commit()

    elif tiporeq == "criar_canal":
        nome_canal = req.criar_canal.canal_nome
        cursor.execute(
            "SELECT id FROM canais WHERE nome = ?",
            (nome_canal,)
        )
        canal = cursor.fetchone()

        if canal:
            rep.criar_canal.criado = False
            rep.criar_canal.erro = "Canal já existente!"
            rep.criar_canal.timestamp_rep.FromNanoseconds(int((time.time() + relogio_dif) * 1_000_000_000))
        else:
            rep.criar_canal.criado = True
            rep.criar_canal.sucesso = "Canal criado!"
            rep.criar_canal.timestamp_rep.FromNanoseconds(int((time.time() + relogio_dif) * 1_000_000_000))
            tempo_criar = datetime.fromtimestamp(time.time() + relogio_dif, timezone.utc).isoformat()
            cursor.execute(
                "INSERT INTO canais (nome, timestamp_criacao) VALUES (?, ?)",
                (nome_canal, tempo_criar)
            )
            conexao.commit()

    elif tiporeq == "listagem_canais":
        cursor.execute("SELECT nome FROM canais")
        canais = cursor.fetchall()
        for canal in canais:
            rep.listagem_canais.canais.append(canal[0])
        rep.listagem_canais.timestamp_rep.FromNanoseconds(int((time.time() + relogio_dif) * 1_000_000_000))

    elif tiporeq == "publicar":
        topico = req.publicar.nome_canal
        texto = req.publicar.publicacao
        req.publicar.timestamp_req.FromNanoseconds(int((time.time() + relogio_dif) * 1_000_000_000))
        publicacao = mensagem_pb2.EnviarPub()
        publicacao.topico =  topico
        publicacao.publicacao = texto
        publicacao.timestamp_pub.FromNanoseconds(int((time.time() + relogio_dif) * 1_000_000_000))
        relogio += 1
        publicacao.relogio = relogio
        tempo_pub = datetime.fromtimestamp(time.time() + relogio_dif, timezone.utc).isoformat()
        cursor.execute(
            """INSERT INTO publicacoes (topico, mensagem, timestamp_pub) VALUES (?, ?, ?)""",
            (topico, texto, tempo_pub)
        )
        conexao.commit()
        pub.send_multipart([topico.encode("utf-8"), publicacao.SerializeToString()])
        rep.publicar.publicado = True
        rep.publicar.sucesso = "Mensagem Publicada!"
        rep.publicar.timestamp_rep.FromNanoseconds(int((time.time() + relogio_dif) * 1_000_000_000))

    print(f"Mensagem enviada: {rep}", flush=True)
    relogio += 1
    rep.relogio = relogio
    socket.send(rep.SerializeToString())

    if qtd_msg >= 10:
        req_heartbeat = mensagem_pb2.ReqReferencia()
        req_heartbeat.tipo_referencia = mensagem_pb2.HEARTBEAT
        req_heartbeat.nome_servidor = "servidor-python"
        relogio += 1
        req_heartbeat.relogio = relogio
        servico_ref.send(req_heartbeat.SerializeToString())

        rep_heartbeat = mensagem_pb2.RepReferencia()
        rep_heartbeat.ParseFromString(servico_ref.recv())
        relogio = max(relogio, rep_heartbeat.relogio)
        if rep_heartbeat.sucesso:
            horario_referencia = rep_heartbeat.timestamp_ref.ToNanoseconds() / 1_000_000_000
            relogio_dif = horario_referencia - time.time()
            print("Heartbeat enviado!", flush=True)
            print("Offset do relógio em segundos: ", relogio_dif, flush=True)

        qtd_msg = 0