package paris.benoit.mob.front;

import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JettyEventServlet extends WebSocketServlet {
    private static final Logger logger = LoggerFactory.getLogger(JettyEventServlet.class);

    @Override
    public void configure(WebSocketServletFactory factory) {
        factory.register(JettyWebSocketHandler.class);
    }
}