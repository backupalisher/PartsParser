package ru.part4;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class Main {
    private static final String DEFAULT_DRIVER = "org.postgresql.Driver";
    private static final String DEFAULT_URL = "jdbc:postgresql://localhost:5432/part4_new";
    private static final String DEFAULT_USERNAME = "zapchasty";
    private static final String DEFAULT_PASSWORD = "zapchasty_GfhjkzYtn321";
    private static final String path = "E:\\part4\\part_codes_all\\Brother";

    public static void main(String[] args) {
        String driver = ((args.length > 0) ? args[0] : DEFAULT_DRIVER);
        String url = ((args.length > 1) ? args[1] : DEFAULT_URL);
        String username = ((args.length > 2) ? args[2] : DEFAULT_USERNAME);
        String password = ((args.length > 3) ? args[3] : DEFAULT_PASSWORD);
        Connection connection = null;

        int brand_id = 1; // ID ПРОИЗВОДИТЕЛЯ
        int model_id = 0;
        int module_id = 0;
        int part_code_id = 0;

        Date date = new Date();
        System.out.println("Запуск " + date.toString());

        try {
            connection = createConnection(driver, url, username, password);

            File dir = new File(path);
            File[] arrFiles = dir.listFiles();
            List<File> lst = Arrays.asList(arrFiles);

            for (File file : lst) {
            try {
                String s = file.getName();
                if (!s.contains(".~")){
                    if (!s.contains("img_url")) {
                        s = s.substring(0, s.length() - 4);
                        if (!s.contains("_img")) {
                            String sqlModel = "INSERT INTO all_models (name,brand_id) SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM all_models WHERE name=? AND brand_id= ?) RETURNING id;";
                            List modelParametrs = Arrays.asList(s,brand_id,s,brand_id);
                            model_id = query(connection, sqlModel, modelParametrs);

                            String sqlModelId = "SELECT id FROM all_models WHERE name = ?;";
                            List modelIdParametrs = Arrays.asList(s);
                            model_id = query(connection, sqlModelId, modelIdParametrs);

                            try {
                                FileInputStream fileInputStream = new FileInputStream(file.getAbsolutePath());
                                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
                                String strLine;
                                String[] subStr;
                                String partcode = null;
                                while ((strLine = bufferedReader.readLine()) != null) {
                                    subStr = strLine.split(";");
                                    for (int i = 1; i < subStr.length; i++) {
                                        if (i == 1) {
                                            String module = subStr[i];
                                            String sqlModule = "INSERT INTO modules (name) SELECT ? WHERE NOT EXISTS (SELECT 1 FROM modules WHERE name=?) RETURNING id;";
//                                            String sqlModule = "INSERT INTO modules (name) VALUES (?) ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id;";
                                            List moduleParametrs = Arrays.asList(module, module);
                                            module_id = query(connection,sqlModule,moduleParametrs);

                                            String sqlModuleId = "SELECT id FROM modules WHERE name = ?;";
                                            List moduleIdParametrs = Arrays.asList(module);
                                            module_id = query(connection,sqlModuleId,moduleIdParametrs);

                                            String sqlLinkDetailsOptions = "INSERT INTO link_model_modules (model_id, module_id) SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM link_model_modules WHERE model_id=? AND module_id=?);";
                                            List linkModelModule = Arrays.asList(model_id,module_id, model_id, module_id);
                                            update(connection,sqlLinkDetailsOptions,linkModelModule);

                                        } else if (i == 2) {
                                            partcode = subStr[i];
                                        } else if (i == 3){
                                            String sqlPartcodes = "INSERT INTO partcodes (code) SELECT ? WHERE NOT EXISTS (SELECT 1 FROM partcodes WHERE code=?) RETURNING id;";
                                            List partcodeParametrs = Arrays.asList(partcode, partcode);
                                            part_code_id = query(connection, sqlPartcodes, partcodeParametrs);

                                            String sqlPartcodesId = "SELECT id FROM partcodes WHERE code = ?;";
                                            List partcodeIdParametrs = Arrays.asList(partcode);
                                            part_code_id = query(connection,sqlPartcodesId,partcodeIdParametrs);

                                            String sqlDetails = "INSERT INTO details (name, all_model_id, partcode_id, module_id) SELECT ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM details WHERE name=? AND all_model_id=? AND partcode_id=?);";
                                            List detailsParametrs = Arrays.asList(subStr[i], model_id, part_code_id, module_id, subStr[i], model_id, part_code_id);
                                            update(connection,sqlDetails,detailsParametrs);
                                        }
                                    }
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        } catch (Exception ex) {
            System.out.println("Connection failed...");
            ex.printStackTrace();
        }

        System.out.println("Завершение " + date.toString());
    }

    public static int query(Connection connection, String sql, List<Object> parameters) throws SQLException {
        int results = 0;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(sql);
            int i = 0;

            for (Object parameter : parameters) {
                ps.setObject(++i, parameter);
            }

//            System.out.println(ps.toString());
            rs = ps.executeQuery();
            while (rs.next()) {
                results = rs.getInt("id");
            }

        } finally {
            close(rs);
            close(ps);
        }
        return results;
    }

    public static int update(Connection connection, String sql, List<Object> parameters) throws SQLException {
        int numRowsUpdated = 0;
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            int i = 0;
            for (Object parameter : parameters) {
                ps.setObject(++i, parameter);
            }

//            System.out.println(ps.toString());
            numRowsUpdated = ps.executeUpdate();
        } finally {
            close(ps);
        }
        return numRowsUpdated;
    }

    public static Connection createConnection(String driver, String url, String username, String password) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        if ((username == null) || (password == null) || (username.trim().length() == 0) || (password.trim().length() == 0)) {
            return DriverManager.getConnection(url);
        } else {
            return DriverManager.getConnection(url, username, password);
        }
    }

    public static void close(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void close(Statement st) {
        try {
            if (st != null) {
                st.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void close(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static List<Map<String, Object>> map(ResultSet rs) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        try {
            if (rs != null) {
                ResultSetMetaData meta = rs.getMetaData();
                int numColumns = meta.getColumnCount();

                while (rs.next()) {
                    Map<String, Object> row = new HashMap<String, Object>();

                    for (int i = 1; i <= numColumns; ++i) {
                        String name = meta.getColumnName(i);
                        Object value = rs.getObject(i);
                        row.put(name, value);
                    }

                    results.add(row);
                }
            }
        } finally {
            close(rs);
        }

        return results;
    }
}
