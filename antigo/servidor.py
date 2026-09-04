import zmq

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

tarefas = {}
proximo_id = 1

print("Servidor de tarefas iniciado. Aguardando requisições...", flush=True)

while True:
    requisicao = socket.recv_json()
    acao = requisicao.get("acao")

    if acao == "adicionar":
        descricao = (requisicao.get("descricao") or "").strip()
        if descricao:
            tarefa_id = proximo_id
            tarefas[tarefa_id] = descricao
            proximo_id += 1
            resposta = {
                "status": "ok",
                "mensagem": f"Tarefa {tarefa_id} adicionada com sucesso.",
            }
        else:
            resposta = {
                "status": "erro",
                "mensagem": "A descrição da tarefa não pode ser vazia.",
            }

    elif acao == "remover":
        tarefa_id = requisicao.get("id")
        if tarefa_id in tarefas:
            descricao = tarefas.pop(tarefa_id)
            resposta = {
                "status": "ok",
                "mensagem": f"Tarefa {tarefa_id} ('{descricao}') removida com sucesso.",
            }
        else:
            resposta = {
                "status": "erro",
                "mensagem": f"Tarefa com id {tarefa_id} não encontrada.",
            }

    elif acao == "listar":
        lista = [
            {"id": tid, "descricao": desc}
            for tid, desc in sorted(tarefas.items())
        ]
        resposta = {"status": "ok", "tarefas": lista}

    else:
        resposta = {
            "status": "erro",
            "mensagem": f"Ação desconhecida: {acao}",
        }

    print(f"[{acao}] -> {resposta}", flush=True)
    socket.send_json(resposta)
