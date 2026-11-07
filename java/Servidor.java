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
    public static Timestamp tempo_agora() {
        long tempo = System.currentTimeMillis();
        return Timestamp.newBuilder().setSeconds(tempo / 1000).setNanos((int) (tempo % 1000) * 1000000).build();
    }

    public static void main(String[] args) throws Exception {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.connect("tcp://broker:5556");

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
            Statement stmt = conexao.createStatement();
            stmt.execute(sqlLogin);
            stmt.execute(sqlCanal);

            while (true){
                byte[] mensagem = socket.recv();
                Mensagem.Request req = Mensagem.Request.parseFrom(mensagem);
                System.out.println("Mensagem recebida:");
                System.out.println(req);

                Mensagem.Reply.Builder rep = Mensagem.Reply.newBuilder();
                if(req.getRequisicaoCase() == Mensagem.Request.RequisicaoCase.LOGIN_USUARIO){
                    rep.setLoginUsuario(Mensagem.RepLogin.newBuilder().setLogado(true).setSucesso("Login realizado!").setTimestampRep(tempo_agora()));
                    String usuario = req.getLoginUsuario().getUsuario();
                    String tempo_login = Instant.now().toString();
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
                        String tempo_criacao = Instant.now().toString();
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

                Mensagem.Reply reply = rep.build();
                System.out.println("Mensagem enviada:");
                System.out.println(reply);
                socket.send(reply.toByteArray());
            }
        }
    }
}