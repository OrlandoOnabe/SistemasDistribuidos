import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import org.zeromq.SocketType;
import com.google.protobuf.Timestamp;
import java.util.ArrayList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;

public class Servidor {
    static long relogio = 0;
    static int qtd_msg = 0;
    static long relogio_dif = 0;
    public static Timestamp tempo_agora() {
        long tempo = System.currentTimeMillis() + relogio_dif;
        return Timestamp.newBuilder().setSeconds(tempo / 1000).setNanos((int) (tempo % 1000) * 1000000).build();
    }

    public static void main(String[] args) throws Exception {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.connect("tcp://broker:5556");
            ZMQ.Socket pub = context.createSocket(SocketType.PUB);
            pub.connect("tcp://proxy:5557");
            ZMQ.Socket ref = context.createSocket(SocketType.REQ);
            ref.connect("tcp://referencia:5559");

            relogio++;
            Mensagem.ReqReferencia req_ref = Mensagem.ReqReferencia.newBuilder().setTipoReferencia(Mensagem.ReferenciaTipo.REGISTRAR).setNomeServidor("servidor-java").setRelogio(relogio).build();
            ref.send(req_ref.toByteArray());
            Mensagem.RepReferencia rep_ref = Mensagem.RepReferencia.parseFrom(ref.recv());
            relogio = Math.max(relogio, rep_ref.getRelogio());
            int rank = rep_ref.getRank();
            System.out.println(rep_ref.getSucedido());
            System.out.println("Ranking: " + rank);

            relogio++;
            req_ref = Mensagem.ReqReferencia.newBuilder().setTipoReferencia(Mensagem.ReferenciaTipo.LISTAGEM_SERVIDORES).setNomeServidor("servidor-java").setRelogio(relogio).build();
            ref.send(req_ref.toByteArray());

            rep_ref = Mensagem.RepReferencia.parseFrom(ref.recv());
            relogio = Math.max(relogio, rep_ref.getRelogio());

            System.out.println("Lista de servidores registrados:");
            for (Mensagem.Ranking servidor : rep_ref.getListaServidoresList()) {
                System.out.println("Nome: " + servidor.getNomeServidor() + " | Rank: " + servidor.getRank());
}

            Connection conexao = DriverManager.getConnection("jdbc:sqlite:/app/dados/java.db");
            String sqlLogin ="""
                CREATE TABLE IF NOT EXISTS logins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    usuario TEXT NOT NULL,
                    timestamp_login TEXT NOT NULL
                )
            """;
            String sqlCanal = """
                CREATE TABLE IF NOT EXISTS canais (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nome TEXT NOT NULL UNIQUE,
                    timestamp_criacao TEXT NOT NULL
                )
            """;
            String sqlPub = """
                CREATE TABLE IF NOT EXISTS publicacoes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topico TEXT NOT NULL,
                    mensagem TEXT NOT NULL,
                    timestamp_pub TEXT NOT NULL
                )
            """;

            Statement stmt = conexao.createStatement();
            stmt.execute(sqlLogin);
            stmt.execute(sqlCanal);
            stmt.execute(sqlPub);

            while (true){
                byte[] mensagem = socket.recv();
                Mensagem.Request req = Mensagem.Request.parseFrom(mensagem);
                relogio = Math.max(relogio, req.getRelogio());
                qtd_msg++;
                System.out.println("Relógio requisição: " + req.getRelogio());
                System.out.println("Relógio servidor: " + relogio);
                System.out.println("Mensagem recebida:");
                System.out.println(req);

                Mensagem.Reply.Builder rep = Mensagem.Reply.newBuilder();
                if(req.getRequisicaoCase() == Mensagem.Request.RequisicaoCase.LOGIN_USUARIO){
                    rep.setLoginUsuario(Mensagem.RepLogin.newBuilder().setLogado(true).setSucesso("Login realizado!").setTimestampRep(tempo_agora()));
                    String usuario = req.getLoginUsuario().getUsuario();
                    String tempo_login = Instant.ofEpochMilli(System.currentTimeMillis() + relogio_dif).toString();
                    String sql = "INSERT INTO logins (usuario, timestamp_login) VALUES (?, ?)";
                    PreparedStatement pstmt = conexao.prepareStatement(sql);
                    pstmt.setString(1, usuario);
                    pstmt.setString(2, tempo_login);
                    pstmt.executeUpdate();
                    pstmt.close();
                }
                else if(req.getRequisicaoCase() == Mensagem.Request.RequisicaoCase.CRIAR_CANAL){
                    String nomeCanal = req.getCriarCanal().getCanalNome();
                    String sql = "SELECT id FROM canais WHERE nome = ?";
                    PreparedStatement pstmt = conexao.prepareStatement(sql);
                    pstmt.setString(1, nomeCanal);
                    ResultSet rs = pstmt.executeQuery();
                    boolean existente = rs.next();
                    rs.close();
                    pstmt.close();

                    if (existente){
                        rep.setCriarCanal(Mensagem.RepCriarCanal.newBuilder().setCriado(false).setErro("Canal já existente!").setTimestampRep(tempo_agora()));
                    }
                    else{
                        rep.setCriarCanal(Mensagem.RepCriarCanal.newBuilder().setCriado(true).setSucesso("Canal criado!").setTimestampRep(tempo_agora()));
                        sql = "INSERT INTO canais (nome, timestamp_criacao) VALUES (?, ?)";
                        pstmt = conexao.prepareStatement(sql);
                        String tempo_criacao = Instant.ofEpochMilli(System.currentTimeMillis() + relogio_dif).toString();
                        pstmt.setString(1, nomeCanal);
                        pstmt.setString(2, tempo_criacao);
                        pstmt.executeUpdate();
                        pstmt.close();
                    }
                }
                else if(req.getRequisicaoCase() == Mensagem.Request.RequisicaoCase.LISTAGEM_CANAIS){
                    stmt = conexao.createStatement();
                    String sql = "SELECT nome FROM canais";
                    ResultSet rs = stmt.executeQuery(sql);
                    Mensagem.RepListarCanais.Builder channels = Mensagem.RepListarCanais.newBuilder();
                    while (rs.next()) {
                        channels.addCanais(rs.getString("nome"));
                    }
                    rs.close();
                    stmt.close();
                    channels.setTimestampRep(tempo_agora());
                    rep.setListagemCanais(channels);
                }
                else if(req.getRequisicaoCase() == Mensagem.Request.RequisicaoCase.PUBLICAR){
                    String topico = req.getPublicar().getNomeCanal();
                    String texto = req.getPublicar().getPublicacao();
                    String tempo_publicacao = Instant.ofEpochMilli(System.currentTimeMillis() + relogio_dif).toString();
                    relogio++;
                    Mensagem.EnviarPub publicacao = Mensagem.EnviarPub.newBuilder().setTopico(topico).setPublicacao(texto).setTimestampPub(tempo_agora()).setRelogio(relogio).build();
                    String sql = "INSERT INTO publicacoes (topico, mensagem, timestamp_pub) VALUES (?, ?, ?) ";
                    PreparedStatement pstmt = conexao.prepareStatement(sql);
                    pstmt.setString(1, topico);
                    pstmt.setString(2, texto);
                    pstmt.setString(3, tempo_publicacao);
                    pstmt.executeUpdate();
                    pstmt.close();

                    pub.sendMore(topico);
                    pub.send(publicacao.toByteArray());

                    rep.setPublicar(Mensagem.RepPublicar.newBuilder().setPublicado(true).setSucesso("Mensagem Publicada!").setTimestampRep(tempo_agora()));
                }

                relogio++;
                rep.setRelogio(relogio);
                Mensagem.Reply reply = rep.build();
                System.out.println("Mensagem enviada:");
                System.out.println(reply);
                socket.send(reply.toByteArray());

                if (qtd_msg >= 10) {
                    relogio++;
                    req_ref = Mensagem.ReqReferencia.newBuilder().setTipoReferencia(Mensagem.ReferenciaTipo.HEARTBEAT).setNomeServidor("servidor-java").setRelogio(relogio).build();
                    ref.send(req_ref.toByteArray());

                    rep_ref = Mensagem.RepReferencia.parseFrom(ref.recv());
                    relogio = Math.max(relogio, rep_ref.getRelogio());

                    if (rep_ref.getSucesso()) {
                        long horario_referencia = rep_ref.getTimestampRef().getSeconds() * 1000L + rep_ref.getTimestampRef().getNanos() / 1000000L;
                        relogio_dif = horario_referencia - System.currentTimeMillis();
                        System.out.println("Heartbeat enviado!");
                        System.out.println("Offset do relógio em milissegundos: " + relogio_dif);
                    }

                    qtd_msg = 0;
                }
            }
        }
    }
}