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
import java.util.UUID;

public class Servidor {
    static final String NOME = System.getenv("NOME") != null ? System.getenv("NOME") : "servidor-java";
    static long relogio = 0;
    static int qtd_msg = 0;
    static long relogio_dif = 0;
    static String coordenador = null;
    static int qtd_sinc = 0;
    static int rank = 0;
    static boolean eleicao = true;
    static long esperando_eleicao = 0;
    static long ultima_verificacao = System.currentTimeMillis();
    static long ultima_replicacao = System.currentTimeMillis();
    static final long intervalo = 10000;
    static int pagina = 0;

    public static Timestamp tempo_agora() {
        long tempo = System.currentTimeMillis() + relogio_dif;
        return Timestamp.newBuilder().setSeconds(Math.floorDiv(tempo, 1000)).setNanos((int) Math.floorMod(tempo, 1000) * 1000000).build();
    }

    public static void main(String[] args) throws Exception {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.connect("tcp://broker:5556");
            ZMQ.Socket pub = context.createSocket(SocketType.PUB);
            pub.connect("tcp://proxy:5557");
            ZMQ.Socket ref = context.createSocket(SocketType.REQ);
            ref.connect("tcp://referencia:5559");
            ZMQ.Socket servidores = context.createSocket(SocketType.REP);
            servidores.bind("tcp://*:5560");
            ZMQ.Socket sub = context.createSocket(SocketType.SUB);
            sub.connect("tcp://proxy:5558");
            sub.subscribe("servers");
            sub.subscribe("replicacao");
            sub.subscribe("replicacao_canais");

            relogio++;
            Mensagem.ReqReferencia req_ref = Mensagem.ReqReferencia.newBuilder().setTipoReferencia(Mensagem.ReferenciaTipo.REGISTRAR).setNomeServidor(NOME).setRelogio(relogio).build();
            ref.send(req_ref.toByteArray());
            Mensagem.RepReferencia rep_ref = Mensagem.RepReferencia.parseFrom(ref.recv());
            relogio = Math.max(relogio, rep_ref.getRelogio()) + 1;
            rank = rep_ref.getRank();
            System.out.println(rep_ref.getSucedido());
            System.out.println("Ranking: " + rank);

            relogio++;
            req_ref = Mensagem.ReqReferencia.newBuilder().setTipoReferencia(Mensagem.ReferenciaTipo.LISTAGEM_SERVIDORES).setNomeServidor(NOME).setRelogio(relogio).build();
            ref.send(req_ref.toByteArray());

            rep_ref = Mensagem.RepReferencia.parseFrom(ref.recv());
            relogio = Math.max(relogio, rep_ref.getRelogio()) + 1;

            System.out.println("Lista de servidores registrados:");
            for (Mensagem.Ranking servidor : rep_ref.getListaServidoresList()) {
                System.out.println("Nome: " + servidor.getNomeServidor() + " | Rank: " + servidor.getRank());
}

            Connection conexao = DriverManager.getConnection("jdbc:sqlite:/app/dados/" + NOME + ".db");
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
                    timestamp_pub TEXT NOT NULL,
                    id_uuid TEXT NOT NULL UNIQUE,
                    servidor_origem TEXT NOT NULL,
                    relogio_logico INTEGER NOT NULL DEFAULT 0
                )
            """;

            Statement stmt = conexao.createStatement();
            stmt.execute(sqlLogin);
            stmt.execute(sqlCanal);
            stmt.execute(sqlPub);

            ZMQ.Poller poller = context.createPoller(3);
            poller.register(socket, ZMQ.Poller.POLLIN);
            poller.register(servidores, ZMQ.Poller.POLLIN);
            poller.register(sub, ZMQ.Poller.POLLIN);

            while (true){
                poller.poll(500);

                if (poller.pollin(2)) {
                    String topico = sub.recvStr();
                    byte[] msg = sub.recv();
                    if (topico.equals("servers")){
                        Mensagem.AnuncioCoordenador anuncio = Mensagem.AnuncioCoordenador.parseFrom(msg);
                        relogio = Math.max(relogio, anuncio.getRelogio()) + 1;
                        if (anuncio.getRank() >= rank || anuncio.getNomeServidor().equals(NOME)) {
                            coordenador = anuncio.getNomeServidor();
                            eleicao = false;
                            esperando_eleicao = 0;
                            System.out.println("Novo coordenador: " + coordenador);
                        }
                    }
                    else if (topico.equals("replicacao")) {
                        Mensagem.ReplicaPub replica = Mensagem.ReplicaPub.parseFrom(msg);

                        if (!replica.getServidorOrigem().equals(NOME)) {
                            relogio = Math.max(relogio, replica.getRelogioLogico()) + 1;

                            String sql = "INSERT OR IGNORE INTO publicacoes (id_uuid, topico, mensagem, timestamp_pub, servidor_origem, relogio_logico) VALUES (?, ?, ?, ?, ?, ?)";

                            PreparedStatement pstmt = conexao.prepareStatement(sql);
                            pstmt.setString(1, replica.getId());
                            pstmt.setString(2, replica.getTopico());
                            pstmt.setString(3, replica.getMensagem());
                            pstmt.setString(4, replica.getTimestampPub());
                            pstmt.setString(5, replica.getServidorOrigem());
                            pstmt.setLong(6, replica.getRelogioLogico());

                            int inseridas = pstmt.executeUpdate();
                            pstmt.close();
                            if (inseridas > 0) {
                                System.out.println("Publicação replicada! Id: " + replica.getId());
                            }
                        }
                    }
                    else if (topico.equals("replicacao_canais")) {
                        Mensagem.ReplicaCanal replica = Mensagem.ReplicaCanal.parseFrom(msg);

                        String sql = "INSERT OR IGNORE INTO canais (nome, timestamp_criacao) VALUES (?, ?)";

                        PreparedStatement pstmt = conexao.prepareStatement(sql);
                        pstmt.setString(1, replica.getNome());
                        pstmt.setString(2, replica.getTimestampCriacao());
                        pstmt.executeUpdate();
                        pstmt.close();
                    }
                }

                if (poller.pollin(1)) {
                    Mensagem.ReqServidor req_servidor = Mensagem.ReqServidor.parseFrom(servidores.recv());
                    relogio = Math.max(relogio, req_servidor.getRelogio()) + 1;
                    Mensagem.RepServidor.Builder rep_servidor = Mensagem.RepServidor.newBuilder().setSucesso(true);

                    if (req_servidor.getTipoServidor() == Mensagem.ServidorTipo.ELEICAO) {
                        rep_servidor.setSucedido("OK");
                        if (req_servidor.getRank() < rank) {
                            eleicao = true;
                            esperando_eleicao = 0;
                            System.out.println("Eleição solicitada por: " + req_servidor.getNomeServidor());
                        }
                    }
                    else if (req_servidor.getTipoServidor() == Mensagem.ServidorTipo.VERIFICAR_COORDENADOR) {
                        rep_servidor.setSucesso(true);
                        rep_servidor.setSucedido("Coordenador disponível");
                    }
                    else if (req_servidor.getTipoServidor() == Mensagem.ServidorTipo.CONSULTAR_RELOGIO) {
                        double horario_local = (System.currentTimeMillis() + relogio_dif) / 1000.0;
                        double horario_remoto = req_servidor.getTimestamp().getSeconds() + req_servidor.getTimestamp().getNanos() / 1000000000.0;
                        double horario_medio = (horario_local + horario_remoto) / 2;
                        rep_servidor.setAjuste(horario_medio - horario_remoto);
                        relogio_dif += Math.round((horario_medio - horario_local) * 1000);
                        rep_servidor.setTimestamp(tempo_agora());
                        rep_servidor.setSucedido("Relógio sincronizado!");
                        System.out.println("Horário médio calculado");
                        System.out.println("Offset do coordenador em milissegundos: " + relogio_dif);
                    }
                    else if (req_servidor.getTipoServidor() == Mensagem.ServidorTipo.AJUSTAR_RELOGIO) {
                        relogio_dif += Math.round(req_servidor.getAjuste() * 1000);
                        rep_servidor.setSucedido("Relógio ajustado!");
                        System.out.println("offset em milissegundos: " + relogio_dif);
                    }
                    else if (req_servidor.getTipoServidor() == Mensagem.ServidorTipo.SOLICITAR_REPLICAS) {
                        int pagina = Math.max(0, req_servidor.getPagina());
                        int limite = 100;

                        String sql = "SELECT id_uuid, topico, mensagem, timestamp_pub, servidor_origem, relogio_logico FROM publicacoes WHERE id IS NOT NULL ORDER BY id LIMIT ? OFFSET ?";

                        PreparedStatement pstmt = conexao.prepareStatement(sql);
                        pstmt.setInt(1, limite + 1);
                        pstmt.setInt(2, pagina * limite);
                        ResultSet eq = pstmt.executeQuery();

                        int qtd = 0;
                        while (eq.next()) {
                            if (qtd >= limite) {
                                rep_servidor.setMais(true);
                                break;
                            }
                            Mensagem.ReplicaPub replica = Mensagem.ReplicaPub.newBuilder().setId(eq.getString("id_uuid")).setTopico(eq.getString("topico")).setMensagem(eq.getString("mensagem")).setTimestampPub(eq.getString("timestamp_pub")).setServidorOrigem(eq.getString("servidor_origem")).setRelogioLogico(eq.getLong("relogio_logico")).build();
                            rep_servidor.addPublicacoes(replica);
                            qtd++;
                        }
                        eq.close();
                        pstmt.close();
                        if (pagina == 0) {
                            String sqlCanais = "SELECT nome, timestamp_criacao FROM canais";
                            PreparedStatement pstmtCanais = conexao.prepareStatement(sqlCanais);
                            ResultSet eqCanal = pstmtCanais.executeQuery();
                            while (eqCanal.next()) {
                                Mensagem.ReplicaCanal canal = Mensagem.ReplicaCanal.newBuilder().setNome(eqCanal.getString("nome")).setTimestampCriacao(eqCanal.getString("timestamp_criacao")).build();
                                rep_servidor.addCanais(canal);
                            }
                            eqCanal.close();
                            pstmtCanais.close();
                        }
                        rep_servidor.setSucedido("Réplicas enviadas!");

                        System.out.println("Enviando página " + pagina + " de réplicas");
                    }

                    relogio++;
                    rep_servidor.setRelogio(relogio);
                    servidores.send(rep_servidor.build().toByteArray());
                }

                if (poller.pollin(0)) {
                    byte[] mensagem = socket.recv();
                    Mensagem.Request req = Mensagem.Request.parseFrom(mensagem);
                    relogio = Math.max(relogio, req.getRelogio()) + 1;
                    qtd_msg++;
                    qtd_sinc++;
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
                            Mensagem.ReplicaCanal replicaCanal = Mensagem.ReplicaCanal.newBuilder().setNome(nomeCanal).setTimestampCriacao(tempo_criacao).build();
                            pub.sendMore("replicacao_canais");
                            pub.send(replicaCanal.toByteArray());
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
                        String id = UUID.randomUUID().toString();
                        String topico = req.getPublicar().getNomeCanal();
                        String texto = req.getPublicar().getPublicacao();
                        String tempo_publicacao = Instant.ofEpochMilli(System.currentTimeMillis() + relogio_dif).toString();
                        relogio++;
                        Mensagem.EnviarPub publicacao = Mensagem.EnviarPub.newBuilder().setTopico(topico).setPublicacao(texto).setTimestampPub(tempo_agora()).setRelogio(relogio).build();
                        String sql = "INSERT INTO publicacoes (id_uuid, topico, mensagem, timestamp_pub, servidor_origem, relogio_logico) VALUES (?, ?, ?, ?, ?, ?) ";
                        PreparedStatement pstmt = conexao.prepareStatement(sql);
                        pstmt.setString(1, id);
                        pstmt.setString(2, topico);
                        pstmt.setString(3, texto);
                        pstmt.setString(4, tempo_publicacao);
                        pstmt.setString(5, NOME);
                        pstmt.setLong(6, relogio);
                        pstmt.executeUpdate();
                        pstmt.close();

                        Mensagem.ReplicaPub replica =
                        Mensagem.ReplicaPub.newBuilder().setId(id).setTopico(topico).setMensagem(texto).setTimestampPub(tempo_publicacao).setServidorOrigem(NOME).setRelogioLogico(relogio).build();

                        pub.sendMore("replicacao");
                        pub.send(replica.toByteArray());
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
                        req_ref = Mensagem.ReqReferencia.newBuilder().setTipoReferencia(Mensagem.ReferenciaTipo.HEARTBEAT).setNomeServidor(NOME).setRelogio(relogio).build();
                        ref.send(req_ref.toByteArray());

                        rep_ref = Mensagem.RepReferencia.parseFrom(ref.recv());
                        relogio = Math.max(relogio, rep_ref.getRelogio()) + 1;

                        if (rep_ref.getSucesso()) {
                            System.out.println("Heartbeat enviado!");
                        }
                        else {
                            relogio++;
                            req_ref = Mensagem.ReqReferencia.newBuilder().setTipoReferencia(Mensagem.ReferenciaTipo.REGISTRAR).setNomeServidor(NOME).setRelogio(relogio).build();
                            ref.send(req_ref.toByteArray());
                            rep_ref = Mensagem.RepReferencia.parseFrom(ref.recv());
                            relogio = Math.max(relogio, rep_ref.getRelogio()) + 1;
                            rank = rep_ref.getRank();
                            eleicao = true;
                            System.out.println("Servidor registrado novamente!");
                        }

                        qtd_msg = 0;
                    }

                    if (qtd_sinc >= 15) {
                        if (coordenador != null && !coordenador.equals(NOME)) {
                            ZMQ.Socket consulta = context.createSocket(SocketType.REQ);
                            consulta.setLinger(0);
                            consulta.setReceiveTimeOut(1000);
                            consulta.setSendTimeOut(1000);
                            consulta.connect("tcp://" + coordenador + ":5560");
                            relogio++;
                            Mensagem.ReqServidor req_sinc = Mensagem.ReqServidor.newBuilder().setTipoServidor(Mensagem.ServidorTipo.CONSULTAR_RELOGIO).setNomeServidor(NOME).setRank(rank).setRelogio(relogio).setTimestamp(tempo_agora()).build();
                            try {
                                consulta.send(req_sinc.toByteArray());
                                byte[] dados = consulta.recv();
                                if (dados == null) {
                                    throw new Exception("Timeout");
                                }
                                Mensagem.RepServidor rep_sinc = Mensagem.RepServidor.parseFrom(dados);
                                relogio = Math.max(relogio, rep_sinc.getRelogio()) + 1;
                                if (rep_sinc.getSucesso()) {
                                    relogio_dif += Math.round(rep_sinc.getAjuste() * 1000);
                                    System.out.println("offset em milissegundos: " + relogio_dif);
                                }
                            }
                            catch (Exception e) {
                                System.out.println("Coordenador indisponível!");
                                coordenador = null;
                                eleicao = true;
                            }
                            finally {
                                consulta.close();
                            }
                        }
                        qtd_sinc = 0;
                    }
                }

                if (System.currentTimeMillis() - ultima_verificacao >= 5000) {
                    ultima_verificacao = System.currentTimeMillis();
                    if (coordenador != null && !coordenador.equals(NOME)) {
                        ZMQ.Socket consulta = context.createSocket(SocketType.REQ);
                        consulta.setLinger(0);
                        consulta.setReceiveTimeOut(1000);
                        consulta.setSendTimeOut(1000);
                        consulta.connect("tcp://" + coordenador + ":5560");
                        relogio++;
                        Mensagem.ReqServidor req_verificacao = Mensagem.ReqServidor.newBuilder().setTipoServidor(Mensagem.ServidorTipo.VERIFICAR_COORDENADOR).setNomeServidor(NOME).setRank(rank).setRelogio(relogio).setTimestamp(tempo_agora()).build();
                        try {
                            consulta.send(req_verificacao.toByteArray());
                            byte[] dados = consulta.recv();
                            if (dados == null) {
                                throw new Exception("Timeout");
                            }
                            Mensagem.RepServidor rep_verificacao = Mensagem.RepServidor.parseFrom(dados);
                            relogio = Math.max(relogio, rep_verificacao.getRelogio()) + 1;
                        }
                        catch (Exception e) {
                            System.out.println("Coordenador indisponível! Iniciando eleição.");
                            coordenador = null;
                            eleicao = true;
                        }
                        finally {
                            consulta.close();
                        }
                    }
                    else if (coordenador == null) {
                        eleicao = true;
                    }
                }

                if (System.currentTimeMillis() - ultima_replicacao >= intervalo) {
                    ultima_replicacao = System.currentTimeMillis();
                    relogio++;
                    req_ref = Mensagem.ReqReferencia.newBuilder().setTipoReferencia(Mensagem.ReferenciaTipo.LISTAGEM_SERVIDORES).setNomeServidor(NOME).setRelogio(relogio).build();
                    ref.send(req_ref.toByteArray());
                    rep_ref = Mensagem.RepReferencia.parseFrom(ref.recv());
                    relogio = Math.max(relogio, rep_ref.getRelogio()) + 1;

                    for (Mensagem.Ranking par : rep_ref.getListaServidoresList()) {
                        if (par.getNomeServidor().equals(NOME)) {
                            continue;
                        }
                        int pag = 0;
                        boolean continuar = true;
                        while (continuar) {
                            continuar = false;
                            ZMQ.Socket consulta = context.createSocket(SocketType.REQ);
                            consulta.setLinger(0);
                            consulta.setReceiveTimeOut(1000);
                            consulta.setSendTimeOut(1000);
                            consulta.connect("tcp://" + par.getNomeServidor() + ":5560");

                            relogio++;
                            Mensagem.ReqServidor req_replica = Mensagem.ReqServidor.newBuilder().setTipoServidor(Mensagem.ServidorTipo.SOLICITAR_REPLICAS).setNomeServidor(NOME).setPagina(pag).setRelogio(relogio).build();
                            try {
                                consulta.send(req_replica.toByteArray());
                                byte[] dados = consulta.recv();
                                if (dados == null) {
                                    throw new Exception("Timeout");
                                }
                                Mensagem.RepServidor resposta = Mensagem.RepServidor.parseFrom(dados);
                                relogio = Math.max(relogio, resposta.getRelogio()) + 1;

                                int quantidade = 0;
                                for (Mensagem.ReplicaPub replica : resposta.getPublicacoesList()) {
                                    String sql = "INSERT OR IGNORE INTO publicacoes (id_uuid, topico, mensagem, timestamp_pub, servidor_origem, relogio_logico) VALUES (?, ?, ?, ?, ?, ?)";
                                    PreparedStatement pstmt = conexao.prepareStatement(sql);
                                    pstmt.setString(1, replica.getId());
                                    pstmt.setString(2, replica.getTopico());
                                    pstmt.setString(3, replica.getMensagem());
                                    pstmt.setString(4, replica.getTimestampPub());
                                    pstmt.setString(5, replica.getServidorOrigem());
                                    pstmt.setLong(6, replica.getRelogioLogico());
                                    quantidade += pstmt.executeUpdate();
                                    pstmt.close();
                                }
                                for (Mensagem.ReplicaCanal canal : resposta.getCanaisList()) {
                                    String sql = "INSERT OR IGNORE INTO canais (nome, timestamp_criacao) VALUES (?, ?)";
                                    PreparedStatement pstmt = conexao.prepareStatement(sql);
                                    pstmt.setString(1, canal.getNome());
                                    pstmt.setString(2, canal.getTimestampCriacao());
                                    pstmt.executeUpdate();
                                    pstmt.close();
                                }

                                System.out.println(quantidade + " publicações recuperadas de " + par.getNomeServidor());

                                if (resposta.getMais()) {
                                    pag++;
                                    continuar = true;
                                }
                            } catch (Exception e) {
                                System.out.println("Replicação não foi possível. " + par.getNomeServidor() + " indisponível.");
                            } finally {
                                consulta.close();
                            }
                        }
                    }
                }

                if (esperando_eleicao != 0 && System.currentTimeMillis() - esperando_eleicao >= 3000) {
                    esperando_eleicao = 0;
                    eleicao = true;
                }

                if (eleicao) {
                    eleicao = false;
                    relogio++;
                    req_ref = Mensagem.ReqReferencia.newBuilder().setTipoReferencia(Mensagem.ReferenciaTipo.LISTAGEM_SERVIDORES).setNomeServidor(NOME).setRelogio(relogio).build();
                    ref.send(req_ref.toByteArray());
                    rep_ref = Mensagem.RepReferencia.parseFrom(ref.recv());
                    relogio = Math.max(relogio, rep_ref.getRelogio()) + 1;
                    boolean respondeu = false;

                    for (Mensagem.Ranking servidor : rep_ref.getListaServidoresList()) {
                        if (servidor.getRank() > rank) {
                            ZMQ.Socket consulta = context.createSocket(SocketType.REQ);
                            consulta.setLinger(0);
                            consulta.setReceiveTimeOut(1000);
                            consulta.setSendTimeOut(1000);
                            consulta.connect("tcp://" + servidor.getNomeServidor() + ":5560");
                            relogio++;
                            Mensagem.ReqServidor req_eleicao = Mensagem.ReqServidor.newBuilder().setTipoServidor(Mensagem.ServidorTipo.ELEICAO).setNomeServidor(NOME).setRank(rank).setRelogio(relogio).build();
                            try {
                                consulta.send(req_eleicao.toByteArray());
                                byte[] dados = consulta.recv();
                                if (dados == null) {
                                    throw new Exception("Timeout");
                                }
                                Mensagem.RepServidor rep_eleicao = Mensagem.RepServidor.parseFrom(dados);
                                relogio = Math.max(relogio, rep_eleicao.getRelogio()) + 1;
                                if (rep_eleicao.getSucesso()) {
                                    respondeu = true;
                                }
                            }
                            catch (Exception e) {
                                System.out.println("Servidor não respondeu à eleição: " + servidor.getNomeServidor());
                            }
                            finally {
                                consulta.close();
                            }
                        }
                    }

                    if (respondeu) {
                        coordenador = null;
                        esperando_eleicao = System.currentTimeMillis();
                        System.out.println("Aguardando anúncio do coordenador!");
                    }
                    else {
                        coordenador = NOME;
                        esperando_eleicao = 0;
                        relogio++;
                        Mensagem.AnuncioCoordenador anuncio = Mensagem.AnuncioCoordenador.newBuilder().setNomeServidor(coordenador).setRank(rank).setRelogio(relogio).build();
                        pub.sendMore("servers");
                        pub.send(anuncio.toByteArray());
                        System.out.println("Coordenador eleito: " + coordenador);
                    }
                }
            }
        }
    }
}