package paris.benoit.mob;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.type.MapType;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestJs {

    public static void main(String[] args) throws IOException {


        ObjectMapper mapper = new ObjectMapper();
        MapType type = mapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class);
        Map<String, Object> data = mapper.readValue("{\"leftY\":-32.67326732673267326732673267326733,\"rightY\":0}", type);


        ScriptEngine graaljsEngine = new ScriptEngineManager().getEngineByName("graal.js");

        long took = 0L;
        try {
            graaljsEngine.eval("var a = 3; function inJs(b, c) { a++; return a + c + b + 'inJS12345678'; }");
            Invocable inv = (Invocable) graaljsEngine;

            for (int i = 0; i < 100; i++) {
                long start = System.currentTimeMillis();
                Object result = inv.invokeFunction("inJs", 3, "aa");
                System.out.println(result);
                took = System.currentTimeMillis() - start;
                System.out.println("iteration: " + (took));
            }
        } catch (Exception ex) {
            System.out.println(ex);
        }

    }

}
