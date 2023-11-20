import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.StringTokenizer;
import java.util.Scanner;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class UsuarioCRUD {
    private static final String URL = "jdbc:mysql://localhost:3306/valorantav3";
    private static final String USUARIO = "root";
    private static final String SENHA = "12345678";


    // Create
    public static void adicionarUsuario(Scanner scanner) {
        try (Connection connection = DriverManager.getConnection(URL, USUARIO, SENHA)) {
        String sql = "INSERT INTO Usuario (NOME_USUARIO, VP, LEVELXP, ELO, DATA_CONTA) VALUES (?, ?, ?, ?, ?)";
        connection.setAutoCommit(false); // Inicia a transação

        System.out.println("Informe o nome de usuário:");
        String nomeUsuario = scanner.next();

        System.out.println("Informe os VP:");
        int vp = scanner.nextInt();

        System.out.println("Informe o Level XP:");
        int levelXP = scanner.nextInt();

        System.out.println("Informe o ELO:");
        String elo = scanner.next();

        System.out.println("Informe a data da conta (formato YYYY-MM-DD):");
        String dataConta = scanner.next();

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1, nomeUsuario);
                preparedStatement.setInt(2, vp);
                preparedStatement.setInt(3, levelXP);
                preparedStatement.setString(4, elo);
                preparedStatement.setString(5, dataConta);

                preparedStatement.executeUpdate();
                System.out.println("Usuário adicionado com sucesso!");

                connection.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            rollbackTransaction();
        }
    }

    // Read
    public static void exibirUsuarios() {
        try (Connection connection = DriverManager.getConnection(URL, USUARIO, SENHA)) {
            String sql = "SELECT * FROM Usuario";

            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery(sql);

                while (resultSet.next()) {
                    System.out.println("ID: " + resultSet.getInt("ID"));
                    System.out.println("Nome de Usuário: " + resultSet.getString("NOME_USUARIO"));
                    System.out.println("VP: " + resultSet.getInt("VP"));
                    System.out.println("Level XP: " + resultSet.getInt("LEVELXP"));
                    System.out.println("ELO: " + resultSet.getString("ELO"));
                    System.out.println("Data da Conta: " + resultSet.getString("DATA_CONTA"));
                    System.out.println("---------------------------");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Update
    public static void atualizarUsuario(Scanner scanner) {
        System.out.println("Informe o ID do usuário que deseja atualizar:");
        int id = scanner.nextInt();

        System.out.println("Informe o novo nome de usuário:");
        String novoNomeUsuario = scanner.next();

        System.out.println("Informe o novo VP:");
        int novoVP = scanner.nextInt();

        System.out.println("Informe o novo Level XP:");
        int novoLevelXP = scanner.nextInt();

        System.out.println("Informe o novo ELO:");
        String novoElo = scanner.next();

        try (Connection connection = DriverManager.getConnection(URL, USUARIO, SENHA)) {
            String sql = "UPDATE Usuario SET NOME_USUARIO = ?, VP = ?, LEVELXP = ?, ELO = ? WHERE ID = ?";

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1, novoNomeUsuario);
                preparedStatement.setInt(2, novoVP);
                preparedStatement.setInt(3, novoLevelXP);
                preparedStatement.setString(4, novoElo);
                preparedStatement.setInt(5, id);

                preparedStatement.executeUpdate();
                System.out.println("Usuário atualizado com sucesso!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Delete
    public static void excluirUsuario(Scanner scanner) {
        System.out.println("Informe o ID do usuário que deseja excluir:");
        int id = scanner.nextInt();

        try (Connection connection = DriverManager.getConnection(URL, USUARIO, SENHA)) {
            String sql = "DELETE FROM Usuario WHERE ID = ?";

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setInt(1, id);

                preparedStatement.executeUpdate();
                System.out.println("Usuário excluído com sucesso!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    // Inserir Partida
    public static void inserirPartida(Scanner scanner) {
        try (Connection connection = DriverManager.getConnection(URL, USUARIO, SENHA)) {
        String sql = "INSERT INTO Partida (ID_USUARIO, MODO, DATA_PARTIDA, MAPA) VALUES (?, ?, ?, ?)";
        connection.setAutoCommit(false);

        System.out.println("Informe o ID do usuário que jogou a partida:");
        int idUsuario = scanner.nextInt();

        System.out.println("Informe o modo da partida:");
        String modoPartida = scanner.next();

        System.out.println("Informe a data da partida (formato YYYY-MM-DD):");
        String dataPartida = scanner.next();

        System.out.println("Informe o mapa da partida:");
        String mapa = scanner.next();



            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setInt(1, idUsuario);
                preparedStatement.setString(2, modoPartida);
                preparedStatement.setString(3, dataPartida);
                preparedStatement.setString(4, mapa);

                preparedStatement.executeUpdate();
                System.out.println("Partida adicionada com sucesso!");

                connection.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            rollbackTransaction();
        }
    }

    // Consulta com JOIN
    public static void consultarUsuarioPartida(Scanner scanner) {
        System.out.println("Informe o ID do usuário para consultar as partidas:");
        int idUsuario = scanner.nextInt();

        try (Connection connection = DriverManager.getConnection(URL, USUARIO, SENHA)) {
            String sql = "SELECT Usuario.ID, NOME_USUARIO, VP, LEVELXP, ELO, DATA_CONTA, Partida.MODO, Partida.DATA_PARTIDA, MAPA " +
                    "FROM Usuario " +
                    "JOIN Partida ON Usuario.ID = Partida.ID_USUARIO " +
                    "WHERE Usuario.ID = ?";

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setInt(1, idUsuario);

                ResultSet resultSet = preparedStatement.executeQuery();

                while (resultSet.next()) {
                    System.out.println("ID: " + resultSet.getInt("ID"));
                    System.out.println("Nome de Usuário: " + resultSet.getString("NOME_USUARIO"));
                    System.out.println("VP: " + resultSet.getInt("VP"));
                    System.out.println("Level XP: " + resultSet.getInt("LEVELXP"));
                    System.out.println("ELO: " + resultSet.getString("ELO"));
                    System.out.println("Data da Conta: " + resultSet.getString("DATA_CONTA"));
                    System.out.println("Modo da Partida: " + resultSet.getString("MODO"));
                    System.out.println("Data da Partida: " + resultSet.getString("DATA_PARTIDA"));
                    System.out.println("Mapa: " + resultSet.getString("MAPA"));
                    System.out.println("---------------------------");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void inserirDadosCSV(String caminhoArquivoCSV) {
        try (Connection connection = DriverManager.getConnection(URL, USUARIO, SENHA)) {
            connection.setAutoCommit(false); // Inicia a transação

            String linha;
            try (BufferedReader reader = new BufferedReader(new FileReader(caminhoArquivoCSV))) {
                while ((linha = reader.readLine()) != null) {
                    StringTokenizer tokenizer = new StringTokenizer(linha, ",");

                    // Extrai os dados do CSV
                    String nomeUsuario = tokenizer.nextToken();
                    int vp = Integer.parseInt(tokenizer.nextToken());
                    int levelXP = Integer.parseInt(tokenizer.nextToken());
                    String elo = tokenizer.nextToken();
                    String dataConta = tokenizer.nextToken();

                    // Insere os dados na tabela Usuario
                    String sqlUsuario = "INSERT INTO Usuario (NOME_USUARIO, VP, LEVELXP, ELO, DATA_CONTA) VALUES (?, ?, ?, ?, ?)";
                    try (PreparedStatement preparedStatementUsuario = connection.prepareStatement(sqlUsuario)) {
                        preparedStatementUsuario.setString(1, nomeUsuario);
                        preparedStatementUsuario.setInt(2, vp);
                        preparedStatementUsuario.setInt(3, levelXP);
                        preparedStatementUsuario.setString(4, elo);
                        preparedStatementUsuario.setString(5, dataConta);

                        preparedStatementUsuario.executeUpdate();
                    }
                }
                // Faz o commit após inserir todos os dados
                connection.commit();
                System.out.println("Inserção de dados concluido!");
            } catch (Exception e) {
                e.printStackTrace();
                // Em caso de erro, faz rollback para desfazer qualquer alteração
                rollbackTransaction();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void exportarParaCSV(String caminhoArquivoCSV) {
        try (Connection connection = DriverManager.getConnection(URL, USUARIO, SENHA)) {
            String sql = "SELECT Usuario.ID, NOME_USUARIO, VP, LEVELXP, ELO, DATA_CONTA, Partida.MODO, Partida.DATA_PARTIDA, MAPA " +
                    "FROM Usuario " +
                    "JOIN Partida ON Usuario.ID = Partida.ID_USUARIO";

            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql);
                 CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(caminhoArquivoCSV), CSVFormat.DEFAULT.withHeader("ID", "Nome de Usuário", "VP", "Level XP", "ELO", "Data da Conta", "Modo da Partida", "Data da Partida", "Mapa"))) {

                while (resultSet.next()) {
                    csvPrinter.printRecord(
                            resultSet.getInt("ID"),
                            resultSet.getString("NOME_USUARIO"),
                            resultSet.getInt("VP"),
                            resultSet.getInt("LEVELXP"),
                            resultSet.getString("ELO"),
                            resultSet.getString("DATA_CONTA"),
                            resultSet.getString("MODO"),
                            resultSet.getString("DATA_PARTIDA"),
                            resultSet.getString("MAPA")
                    );
                }

                System.out.println("Exportação para CSV concluída!");
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    // Método para realizar rollback em caso de erro
    private static void rollbackTransaction() {
        try (Connection connection = DriverManager.getConnection(URL, USUARIO, SENHA)) {
            connection.rollback(); // Desfaz qualquer alteração feita na transação
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int escolha;
        String nomeArqvInserir;
        String nomeArqvExportar;
        do {
            System.out.println("Escolha uma opção:");
            System.out.println("1. Adicionar Usuário");
            System.out.println("2. Exibir Usuários");
            System.out.println("3. Atualizar Usuário");
            System.out.println("4. Excluir Usuário");
            System.out.println("5. Inserir Partida");
            System.out.println("6. Consultar Usuário e Partida (JOIN)");
            System.out.println("7. Exportar informações para .CSV");
            System.out.println("8. Inserir dados de um arquivo .CSV");
            System.out.println("0. Sair");
            escolha = scanner.nextInt();

            switch (escolha) {
                case 1:
                    adicionarUsuario(scanner);
                    break;
                case 2:
                    exibirUsuarios();
                    break;
                case 3:
                    atualizarUsuario(scanner);
                    break;
                case 4:
                    excluirUsuario(scanner);
                    break;
                case 5:
                    inserirPartida(scanner);
                    break;
                case 6:
                    consultarUsuarioPartida(scanner);
                    break;
                case 7:
                    System.out.println("Digite o nome para pôr no arquivo (Insira .csv no final do nome): " );
                    nomeArqvExportar = scanner.next();
                    exportarParaCSV(nomeArqvExportar);
                case 8:
                    System.out.println("Digite o nome do arquivo para realizar a inserção de dados (Verifique se está na mesma pasta do projeto)");
                    nomeArqvInserir = scanner.next();
                    inserirDadosCSV(nomeArqvInserir);
                case 0:
                    System.out.println("Encerrando o programa.");
                    break;
                default:
                    System.out.println("Opção inválida. Tente novamente.");
            }
        } while (escolha != 0);

        scanner.close();
    }
}
