package paris.benoit.mob.front;

import co.paralleluniverse.comsat.webactors.undertow.AutoWebActorHandler;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.RequestDumpingHandler;
import io.undertow.server.handlers.resource.PathResourceManager;
import io.undertow.server.session.InMemorySessionManager;
import io.undertow.server.session.SessionAttachmentHandler;
import io.undertow.server.session.SessionCookieConfig;
import io.undertow.server.session.SessionManager;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.server.ClusterFront;

import java.nio.file.Paths;

public class UndertowFront implements ClusterFront {
    private static final Logger logger = LoggerFactory.getLogger(UndertowFront.class);

    private Undertow server;
    private final int port;
    private final String baseUrl;
    
    public UndertowFront(int port) {
        super();
        this.port = port;
        this.baseUrl = "http://localhost:" + port;
    }

    @Override
    public void start(MobClusterConfiguration conf) {

        final SessionManager sessionManager = new InMemorySessionManager("SESSION_MANAGER", 1, true);
        final SessionCookieConfig sessionConfig = new SessionCookieConfig();
        sessionConfig.setMaxAge(60);
        final SessionAttachmentHandler sessionAttachmentHandler = new SessionAttachmentHandler(sessionManager, sessionConfig);

        HttpHandler fileHandler = Handlers.disableCache(
                Handlers
                        .resource(new PathResourceManager(Paths.get(System.getProperty("user.dir") + "/apps/" + conf.name + "/public"), 100))
                        .setWelcomeFiles("index.html")
        );

        final RequestDumpingHandler actorHandler = new RequestDumpingHandler(sessionAttachmentHandler.setNext(new AutoWebActorHandler()));

        final PathHandler routingHandler = Handlers.path().addPrefixPath("/service", actorHandler).addPrefixPath("/", fileHandler);

        server = Undertow.builder().addHttpListener(port, "0.0.0.0").setHandler(routingHandler).build();

        server.start();
        logger.info("Undertow is up at: " + baseUrl);
        
        pokeActorService();

    }

    private volatile boolean isUp = false;
    private void pokeActorService() {
        new Thread(() -> {
            try {
                if (HttpClients.createDefault().execute(new HttpGet(baseUrl + "/service")).getStatusLine().getStatusCode() > -100) {
                    isUp = true;
                } else {
                    logger.error("Unable to call Actor service");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    @Override
    public void waitReady() throws InterruptedException {
        while (!isUp) {
            Thread.sleep(10);
        }
    }

    @Override
    public String accessString() {
        return baseUrl;
    }

}
