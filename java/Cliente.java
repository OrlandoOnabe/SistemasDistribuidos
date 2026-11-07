import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import org.zeromq.SocketType;
import com.google.protobuf.Timestamp;

public class Cliente {
    public static Timestamp tempo_agora() {
        long tempo = System.currentTimeMillis();
        return Timestamp.newBuilder().setSeconds(tempo / 1000).setNanos((int) (tempo % 1000) * 1000000).build();
    }

    public static void main(String[] args) throws Exception {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.connect("tcp://broker:5555");

            Mensagem.Request req = Mensagem.Request.newBuilder().setTiporeq(Mensagem.RequestTipo.LOGIN_USUARIO)
                    .setLoginUsuario(
                            Mensagem.ReqLogin.newBuilder().setUsuario("bot-java").setTimestampReq(tempo_agora())
                    ).build();

            System.out.println("Enviando:");
            System.out.println(req);
            socket.send(req.toByteArray());
            byte[] mensagem = socket.recv();
            Mensagem.Reply rep = Mensagem.Reply.parseFrom(mensagem);
            System.out.println("Resposta recebida:");
            System.out.println(rep);

            req = Mensagem.Request.newBuilder().setTiporeq(Mensagem.RequestTipo.CRIAR_CANAL)
                    .setCriarCanal(
                            Mensagem.ReqCriarCanal.newBuilder().setCanalNome("canal1").setTimestampReq(tempo_agora())
                    ).build();

            System.out.println("Enviando:");
            System.out.println(req);
            socket.send(req.toByteArray());
            byte[] mensagem2 = socket.recv();
            rep = Mensagem.Reply.parseFrom(mensagem2);
            System.out.println("Resposta recebida:");
            System.out.println(rep);

            req = Mensagem.Request.newBuilder().setTiporeq(Mensagem.RequestTipo.LISTAGEM_CANAIS)
                    .setListagemCanais(
                            Mensagem.ReqListarCanais.newBuilder().setTimestampReq(tempo_agora())
                    ).build();

            System.out.println("Enviando:");
            System.out.println(req);
            socket.send(req.toByteArray());
            byte[] mensagem3 = socket.recv();
            rep = Mensagem.Reply.parseFrom(mensagem3);
            System.out.println("Resposta recebida:");
            System.out.println(rep);
        }
    }
}