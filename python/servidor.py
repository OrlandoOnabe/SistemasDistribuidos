import zmq
import mensagem_pb2
import sqlite3
from datetime import datetime, timezone

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")
pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5557")

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
    print(f"Mensagem recebida: {req}", flush=True)
    tiporeq = req.WhichOneof("requisicao")
    rep = mensagem_pb2.Reply()

    if tiporeq == "login_usuario":
        rep.login_usuario.logado = True
        rep.login_usuario.sucesso = "Login realizado!"
        rep.login_usuario.timestamp_rep.GetCurrentTime()

        usuario = req.login_usuario.usuario
        tempo_login = datetime.now(timezone.utc).isoformat()

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
            rep.criar_canal.timestamp_rep.GetCurrentTime()
        else:
            rep.criar_canal.criado = True
            rep.criar_canal.sucesso = "Canal criado!"
            rep.criar_canal.timestamp_rep.GetCurrentTime()
            tempo_criar = datetime.now(timezone.utc).isoformat()
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
        rep.listagem_canais.timestamp_rep.GetCurrentTime()

    elif tiporeq == "publicar":
        topico = req.publicar.nome_canal
        texto = req.publicar.publicacao
        req.publicar.timestamp_req.GetCurrentTime()
        publicacao = mensagem_pb2.EnviarPub()
        publicacao.topico = topico
        publicacao.publicacao = texto
        publicacao.timestamp_pub.GetCurrentTime()
        tempo_pub = datetime.now(timezone.utc).isoformat()
        cursor.execute(
            """INSERT INTO publicacoes (topico, mensagem, timestamp_pub) VALUES (?, ?, ?)""",
            (topico, texto, tempo_pub)
        )
        conexao.commit()
        pub.send_multipart([topico.encode("utf-8"), publicacao.SerializeToString()])
        rep.publicar.publicado = True
        rep.publicar.sucesso = "Mensagem Publicada!"
        rep.publicar.timestamp_rep.GetCurrentTime()

    print(f"Mensagem enviada: {rep}", flush=True)
    socket.send(rep.SerializeToString())
