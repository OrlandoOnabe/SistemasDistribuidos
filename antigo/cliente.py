import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://broker:5555")


def exibir_menu():
    print("\n===== Gerenciador de Tarefas =====")
    print("1 - Adicionar tarefa")
    print("2 - Remover tarefa")
    print("3 - Listar tarefas")
    print("4 - Sair")


def adicionar_tarefa():
    descricao = input("Descrição da tarefa: ").strip()
    socket.send_json({"acao": "adicionar", "descricao": descricao})
    resposta = socket.recv_json()
    print(resposta["mensagem"])


def remover_tarefa():
    entrada = input("ID da tarefa a remover: ").strip()
    try:
        tarefa_id = int(entrada)
    except ValueError:
        print("ID inválido. Digite um número.")
        return
    socket.send_json({"acao": "remover", "id": tarefa_id})
    resposta = socket.recv_json()
    print(resposta["mensagem"])


def listar_tarefas():
    socket.send_json({"acao": "listar"})
    resposta = socket.recv_json()
    tarefas = resposta.get("tarefas", [])
    if not tarefas:
        print("Nenhuma tarefa cadastrada.")
    else:
        print("\n--- Tarefas ---")
        for tarefa in tarefas:
            print(f"[{tarefa['id']}] {tarefa['descricao']}")


if __name__ == "__main__":
    while True:
        exibir_menu()
        opcao = input("Escolha uma opção: ").strip()

        if opcao == "1":
            adicionar_tarefa()
        elif opcao == "2":
            remover_tarefa()
        elif opcao == "3":
            listar_tarefas()
        elif opcao == "4":
            print("Encerrando cliente...")
            break
        else:
            print("Opção inválida.")
