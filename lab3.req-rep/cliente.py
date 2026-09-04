import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://broker:5555")

while True:
    print("\n===== Gerenciador de Tarefas =====")
    print("1 - Adicionar tarefa")
    print("2 - Remover tarefa")
    print("3 - Listar tarefas")
    print("4 - Sair")
    opcao = input("Escolha uma opcao: ").strip()

    if opcao == "1":
        descricao = input("Descricao da tarefa: ").strip()
        socket.send_string(f"ADD;{descricao}")
        print(socket.recv_string())

    elif opcao == "2":
        tarefa_id = input("ID da tarefa a remover: ").strip()
        socket.send_string(f"REMOVE;{tarefa_id}")
        print(socket.recv_string())

    elif opcao == "3":
        socket.send_string("LIST")
        print(socket.recv_string())

    elif opcao == "4":
        print("Encerrando cliente...")
        break

    else:
        print("Opcao invalida.")
