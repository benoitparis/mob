package paris.benoit.mob.server;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.paralleluniverse.comsat.webactors.undertow.AutoWebActorHandler;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.RequestDumpingHandler;
import io.undertow.server.handlers.resource.PathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.server.session.InMemorySessionManager;
import io.undertow.server.session.SessionAttachmentHandler;
import io.undertow.server.session.SessionCookieConfig;
import io.undertow.server.session.SessionManager;

public class UnderTowLauncher {
    private static final Logger logger = LoggerFactory.getLogger(UnderTowLauncher.class);

    private Undertow server;
    private final int inetPort;
    protected final String baseUrl;
    
    public UnderTowLauncher(int port) {
        super();
        this.inetPort = port;
        this.baseUrl = "http://localhost:" + inetPort;
    }

    public void launchUntertow(String appName) throws Exception {

        final SessionManager sessionManager = new InMemorySessionManager("SESSION_MANAGER", 1, true);
        final SessionCookieConfig sessionConfig = new SessionCookieConfig();
        sessionConfig.setMaxAge(60);
        final SessionAttachmentHandler sessionAttachmentHandler = new SessionAttachmentHandler(sessionManager, sessionConfig);

        final ResourceHandler fileHandler = Handlers
                .resource(new PathResourceManager(Paths.get(System.getProperty("user.dir") + "/apps/" + appName + "/public"), 100))
                .setWelcomeFiles("index.html");

        final RequestDumpingHandler actorHandler = new RequestDumpingHandler(sessionAttachmentHandler.setNext(new AutoWebActorHandler()));

        final PathHandler routingHandler = Handlers.path().addPrefixPath("/service", actorHandler).addPrefixPath("/", fileHandler);

        server = Undertow.builder().addHttpListener(inetPort, "localhost").setHandler(routingHandler).build();

        server.start();
        logger.info("Undertow is up at: " + baseUrl);
    }

    public void waitUnderTowAvailable() throws InterruptedException, IOException {
        for (;;) {
            Thread.sleep(10);
            try {
                if (HttpClients.createDefault().execute(new HttpGet(baseUrl)).getStatusLine().getStatusCode() > -100)
                    break;
            } catch (HttpHostConnectException ex) {
            }
        }
    }
    
    public String getUrl() {
        return baseUrl;
    }

}
