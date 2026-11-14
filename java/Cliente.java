import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import org.zeromq.SocketType;
import com.google.protobuf.Timestamp;
import java.util.ArrayList;
import java.util.Random;
import java.time.Instant;

public class Cliente {
    public static Timestamp tempo_agora() {
        long tempo = System.currentTimeMillis();
        return Timestamp.newBuilder().setSeconds(tempo / 1000).setNanos((int) (tempo % 1000) * 1000000).build();
    }

    public static void main(String[] args) throws Exception {
        try (ZContext context = new ZContext()) {
            Random random = new Random();
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.connect("tcp://broker:5555");
            ZMQ.Socket sub = context.createSocket(SocketType.SUB);
            sub.connect("tcp://proxy:5558");
            ZMQ.Poller poller = context.createPoller(1);
            poller.register(sub, ZMQ.Poller.POLLIN);
            ArrayList<String> canais_inscritos = new ArrayList<>();
            String[] publicacoes = {"Sistemas distribuidos", "Gestão de Projetos de Software", "Estrutura de dados", "Automatos", "Calculo 3"};

            Mensagem.Request req = Mensagem.Request.newBuilder().setTiporeq(Mensagem.RequestTipo.LOGIN_USUARIO).setLoginUsuario(
                            Mensagem.ReqLogin.newBuilder().setUsuario("bot-java").setTimestampReq(tempo_agora())
                    ).build();

            System.out.println("Enviando:");
            System.out.println(req);
            socket.send(req.toByteArray());
            byte[] mensagem = socket.recv();
            Mensagem.Reply rep = Mensagem.Reply.parseFrom(mensagem);
            System.out.println("Resposta recebida:");
            System.out.println(rep);

            req = Mensagem.Request.newBuilder().setTiporeq(Mensagem.RequestTipo.LISTAGEM_CANAIS).setListagemCanais(
                            Mensagem.ReqListarCanais.newBuilder().setTimestampReq(tempo_agora())
                    ).build();

            System.out.println("Enviando:");
            System.out.println(req);
            socket.send(req.toByteArray());
            byte[] mensagem3 = socket.recv();
            rep = Mensagem.Reply.parseFrom(mensagem3);
            System.out.println("Resposta recebida:");
            System.out.println(rep);
            ArrayList<String> canais = new ArrayList<>(rep.getListagemCanais().getCanaisList());

            if (canais.size() < 5){
                String novo_canal = "canal " + random.nextInt(100);

                req = Mensagem.Request.newBuilder().setTiporeq(Mensagem.RequestTipo.CRIAR_CANAL).setCriarCanal(
                                Mensagem.ReqCriarCanal.newBuilder().setCanalNome(novo_canal).setTimestampReq(tempo_agora())
                        ).build();

                System.out.println("Enviando:");
                System.out.println(req);
                socket.send(req.toByteArray());
                byte[] mensagem2 = socket.recv();
                canais.add(novo_canal);
                rep = Mensagem.Reply.parseFrom(mensagem2);
                System.out.println("Resposta recebida:");
                System.out.println(rep);
            }

            if(canais_inscritos.size() < 3){
                ArrayList<String> canais_disponiveis = new ArrayList<>(canais);
                canais_disponiveis.removeAll(canais_inscritos);
                String escolhido = canais_disponiveis.get(random.nextInt(canais_disponiveis.size()));
                sub.subscribe(escolhido.getBytes(ZMQ.CHARSET));
                canais_inscritos.add(escolhido);
                System.out.println("Inscrito no canal: " + escolhido);
            }

            while(true){
                String escolhido = canais.get(random.nextInt(canais.size()));
                System.out.println("Publicando no canal: " + escolhido);
                for (int i = 0; i < 10; i++) {
                        String texto = mensagens[random.nextInt(mensagens.length)];
                        req = Mensagem.Request.newBuilder().setTiporeq(Mensagem.RequestTipo.PUBLICAR).setPublicar(
                                Mensagem.ReqPublicar.newBuilder().setNomeCanal(escolhido).setTimestampReq(tempo_agora())
                        ).build(); 
                        socket.send(req.toByteArray());
                        byte[] mensagem2 = socket.recv();
                        rep = Mensagem.Reply.parseFrom(mensagem2);
                        System.out.println("Resposta recebida:");
                        System.out.println(rep);
                        System.out.println("Publicação: " + texto);

                        poller.poll(0);
                        if (poller.pollin(0)){
                            byte[] texto_pub = sub.recv();
                            Mensagem.EnviarPub publicacao = Mensagem.EnviarPub.parseFrom(dados);

                            System.out.println("\nPublicação recebida:");
                            System.out.println("Tópico: " + publicacao.getTopico());
                            System.out.println("Mensagem: " + publicacao.getPublicacao());
                            Instant tempo_enviada = Instant.ofEpochSecond(publicacao.getTimestampPub().getSeconds(), publicacao.getTimestampPub().getNanos());
                            System.out.println("Enviada em: " + tempo_enviada);
                            System.out.println("Recebida em: " + Instant.now());
                        }
                        Thread.sleep(1000);
                }
            }
        }
    }
}