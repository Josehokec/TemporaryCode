package query;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

public class QueryPatternTest {

    public static void main(String[] args) throws IOException {
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;
        // read query file
        String queryFilePath = prefixPath + "java" + sep + "debug_experiment" + sep + "test_crimes_query.json";
        String jsonStr = new String(Files.readAllBytes(Paths.get(queryFilePath)));

        // use FastJson to parse JSON array file
        JSONArray jsonArray = JSON.parseArray(jsonStr);

        for (int i = 0; i < jsonArray.size(); i++) {
            System.out.println("Query statement:");
            System.out.println("----------------");
            // 获取每个JSON对象
            String queryStr = (String) jsonArray.get(i);
            System.out.println(queryStr);
            PatternQuery query = QueryParse.parseQueryString(queryStr);
            System.out.println("query test:");
            query.print();
            System.out.println("----------------");
        }
    }
}
