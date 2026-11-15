import zmq
import mensagem_pb2
import sqlite3
from datetime import datetime, timezone
import time

relogio = 0
qtd_msg = 0
relogio_dif = 0.0
qtd_sinc = 0
coordenador = None
rank = 0
verificacao = 0
eleicao = True
esperando_eleicao = 0
ultima_verificacao = time.monotonic()

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")
pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5557")
servico_ref = context.socket(zmq.REQ)
servico_ref.connect("tcp://referencia:5559")
servidores = context.socket(zmq.REP)
servidores.bind("tcp://*:5560")
sub = context.socket(zmq.SUB)
sub.connect("tcp://proxy:5558")
sub.setsockopt_string(zmq.SUBSCRIBE, "servers")

req_ref = mensagem_pb2.ReqReferencia()
req_ref.tipo_referencia = mensagem_pb2.REGISTRAR
req_ref.nome_servidor = "servidor-python"
relogio += 1
req_ref.relogio = relogio
servico_ref.send(req_ref.SerializeToString())

rep_ref = mensagem_pb2.RepReferencia()
rep_ref.ParseFromString(servico_ref.recv())
relogio = max(relogio, rep_ref.relogio)
rank = rep_ref.rank
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

poller = zmq.Poller()
poller.register(socket, zmq.POLLIN)
poller.register(servidores, zmq.POLLIN)
poller.register(sub, zmq.POLLIN)

while True:
    eventos = dict(poller.poll(500))
    if sub in eventos:
        topico, dados = sub.recv_multipart()
        anuncio = mensagem_pb2.AnuncioCoordenador()
        anuncio.ParseFromString(dados)
        relogio = max(relogio, anuncio.relogio) + 1
        if anuncio.rank >= rank or anuncio.nome_servidor == "servidor-python":
            coordenador = anuncio.nome_servidor
            eleicao = False
            esperando_eleicao = 0
            print("Novo coordenador:", coordenador, flush=True)

    if servidores in eventos:
        req_servidor = mensagem_pb2.ReqServidor()
        req_servidor.ParseFromString(servidores.recv())
        relogio = max(relogio, req_servidor.relogio) + 1
        rep_servidor = mensagem_pb2.RepServidor()
        rep_servidor.sucesso = True

        if req_servidor.tipo_servidor == mensagem_pb2.ELEICAO:
            rep_servidor.sucedido = "Eleição sucedida!"
            if req_servidor.rank < rank:
                eleicao = True
                esperando_eleicao = 0
                print("Eleição solicitada por:", req_servidor.nome_servidor, flush=True)

        elif req_servidor.tipo_servidor == mensagem_pb2.VERIFICAR_COORDENADOR:
            rep_servidor.sucesso = True
            rep_servidor.sucedido = "Coordenador disponível"

        elif req_servidor.tipo_servidor == mensagem_pb2.CONSULTAR_RELOGIO:
            horario_local = time.time() + relogio_dif
            horario_remoto = req_servidor.timestamp.ToNanoseconds() / 1_000_000_000
            horario_medio = (horario_local + horario_remoto) / 2
            rep_servidor.ajuste = horario_medio - horario_remoto
            relogio_dif += horario_medio - horario_local
            rep_servidor.timestamp.FromNanoseconds(int(horario_medio * 1_000_000_000))
            rep_servidor.sucedido = "Relógio sincronizado!"
            print("Horário médio calculado", flush=True)
            print("Offset do coordenador em segundos:", relogio_dif, flush=True)

        elif req_servidor.tipo_servidor == mensagem_pb2.AJUSTAR_RELOGIO:
            relogio_dif += req_servidor.ajuste
            rep_servidor.sucedido = "Relógio ajustado!"
            print("offset em segundos:", relogio_dif, flush=True)

        relogio += 1
        rep_servidor.relogio = relogio
        servidores.send(rep_servidor.SerializeToString())

    if socket in eventos:
        mensagem = socket.recv()
        req = mensagem_pb2.Request()
        req.ParseFromString(mensagem)
        relogio = max(relogio, req.relogio) + 1
        qtd_msg += 1
        qtd_sinc += 1
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
            publicacao = mensagem_pb2.EnviarPub()
            publicacao.topico = topico
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

        relogio += 1
        rep.relogio = relogio
        print(f"Mensagem enviada: {rep}", flush=True)
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
            relogio = max(relogio, rep_heartbeat.relogio) + 1
            if rep_heartbeat.sucesso:
                print("Heartbeat enviado!", flush=True)
            else:
                req_registro = mensagem_pb2.ReqReferencia()
                req_registro.tipo_referencia = mensagem_pb2.REGISTRAR
                req_registro.nome_servidor = "servidor-python"
                relogio += 1
                req_registro.relogio = relogio
                servico_ref.send(req_registro.SerializeToString())
                rep_registro = mensagem_pb2.RepReferencia()
                rep_registro.ParseFromString(servico_ref.recv())
                relogio = max(relogio, rep_registro.relogio) + 1
                rank = rep_registro.rank
                eleicao = True
                print("Servidor registrado novamente!", flush=True)

            qtd_msg = 0

        if qtd_sinc >= 15:
            if coordenador is not None and coordenador != "servidor-python":
                consulta = context.socket(zmq.REQ)
                consulta.setsockopt(zmq.LINGER, 0)
                consulta.setsockopt(zmq.RCVTIMEO, 1000)
                consulta.setsockopt(zmq.SNDTIMEO, 1000)
                consulta.connect(f"tcp://{coordenador}:5560")
                req_sinc = mensagem_pb2.ReqServidor()
                req_sinc.tipo_servidor = mensagem_pb2.CONSULTAR_RELOGIO
                req_sinc.nome_servidor = "servidor-python"
                req_sinc.rank = rank
                relogio += 1
                req_sinc.relogio = relogio
                req_sinc.timestamp.FromNanoseconds(int((time.time() + relogio_dif) * 1_000_000_000))
                try:
                    consulta.send(req_sinc.SerializeToString())
                    rep_sinc = mensagem_pb2.RepServidor()
                    rep_sinc.ParseFromString(consulta.recv())
                    relogio = max(relogio, rep_sinc.relogio) + 1
                    if rep_sinc.sucesso:
                        relogio_dif += rep_sinc.ajuste
                        print("offset em segundos:", relogio_dif, flush=True)
                except zmq.Again:
                    print("Coordenador indisponível!", flush=True)
                    coordenador = None
                    eleicao = True
                finally:
                    consulta.close()

            qtd_sinc = 0

    if time.monotonic() - ultima_verificacao >= 5:
        ultima_verificacao = time.monotonic()
        if coordenador is not None and coordenador != "servidor-python":
            consulta = context.socket(zmq.REQ)
            consulta.setsockopt(zmq.LINGER, 0)
            consulta.setsockopt(zmq.RCVTIMEO, 1000)
            consulta.setsockopt(zmq.SNDTIMEO, 1000)
            consulta.connect(f"tcp://{coordenador}:5560")
            req_verificacao = mensagem_pb2.ReqServidor()
            req_verificacao.tipo_servidor = mensagem_pb2.VERIFICAR_COORDENADOR
            req_verificacao.nome_servidor = "servidor-python"
            req_verificacao.rank = rank
            relogio += 1
            req_verificacao.relogio = relogio
            req_verificacao.timestamp.FromNanoseconds(int((time.time() + relogio_dif) * 1_000_000_000))
            try:
                consulta.send(req_verificacao.SerializeToString())
                rep_verificacao = mensagem_pb2.RepServidor()
                rep_verificacao.ParseFromString(consulta.recv())
                relogio = max(relogio, rep_verificacao.relogio) + 1
            except zmq.Again:
                print("Coordenador indisponível! Iniciando eleição...", flush=True)
                coordenador = None
                eleicao = True
            finally:
                consulta.close()
        elif coordenador is None:
            eleicao = True

    if esperando_eleicao and time.monotonic() - esperando_eleicao >= 3:
        esperando_eleicao = 0
        eleicao = True

    if eleicao:
        eleicao = False
        req_lista = mensagem_pb2.ReqReferencia()
        req_lista.tipo_referencia = mensagem_pb2.LISTAGEM_SERVIDORES
        req_lista.nome_servidor = "servidor-python"
        relogio += 1
        req_lista.relogio = relogio
        servico_ref.send(req_lista.SerializeToString())
        rep_lista = mensagem_pb2.RepReferencia()
        rep_lista.ParseFromString(servico_ref.recv())
        relogio = max(relogio, rep_lista.relogio) + 1
        respondeu = False

        for servidor in rep_lista.lista_servidores:
            if servidor.rank > rank:
                consulta = context.socket(zmq.REQ)
                consulta.setsockopt(zmq.LINGER, 0)
                consulta.setsockopt(zmq.RCVTIMEO, 1000)
                consulta.setsockopt(zmq.SNDTIMEO, 1000)
                consulta.connect(f"tcp://{servidor.nome_servidor}:5560")
                req_eleicao = mensagem_pb2.ReqServidor()
                req_eleicao.tipo_servidor = mensagem_pb2.ELEICAO
                req_eleicao.nome_servidor = "servidor-python"
                req_eleicao.rank = rank
                relogio += 1
                req_eleicao.relogio = relogio
                try:
                    consulta.send(req_eleicao.SerializeToString())
                    rep_eleicao = mensagem_pb2.RepServidor()
                    rep_eleicao.ParseFromString(consulta.recv())
                    relogio = max(relogio, rep_eleicao.relogio) + 1
                    if rep_eleicao.sucesso:
                        respondeu = True
                except zmq.Again:
                    print("Servidor não respondeu à eleição:", servidor.nome_servidor, flush=True)
                finally:
                    consulta.close()

        if respondeu:
            coordenador = None
            esperando_eleicao = time.monotonic()
            print("Aguardando anúncio do coordenador!", flush=True)
        else:
            coordenador = "servidor-python"
            esperando_eleicao = 0
            anuncio = mensagem_pb2.AnuncioCoordenador()
            anuncio.nome_servidor = coordenador
            anuncio.rank = rank
            relogio += 1
            anuncio.relogio = relogio
            pub.send_multipart([b"servers", anuncio.SerializeToString()])
            print("Coordenador eleito:", coordenador, flush=True)