import zmq

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

tarefas = {}
proximo_id = 1

print("Servidor de tarefas iniciado...", flush=True)

while True:
    mensagem = socket.recv_string()
    print(f"Mensagem recebida: {mensagem}", flush=True)
    partes = mensagem.split(";", 1)
    comando = partes[0]

    if comando == "ADD":
        if len(partes) > 1:
            descricao = partes[1]
        else:
            descricao = ""
        tarefas[proximo_id] = descricao
        resposta = f"Tarefa {proximo_id} adicionada"
        proximo_id += 1

    elif comando == "REMOVE":
        if len(partes) > 1:
            tarefa_id = int(partes[1])
        else:
            tarefa_id = -1
        if tarefa_id in tarefas:
            del tarefas[tarefa_id]
            resposta = f"Tarefa {tarefa_id} removida"
        else:
            resposta = f"Tarefa {tarefa_id} nao encontrada"

    elif comando == "LIST":
        if tarefas:
            linhas = []
            for i, d in sorted(tarefas.items()):
                linhas.append(f"[{i}] {d}")
            resposta = " | ".join(linhas)
        else:
            resposta = "nenhuma tarefa"

    else:
        resposta = "comando desconhecido"

    socket.send_string(resposta)
