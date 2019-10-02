package paris.benoit.mob;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class TestJs {

    public static void main(String[] args) {

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
