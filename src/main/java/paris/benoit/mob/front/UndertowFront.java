package paris.benoit.mob.front;

import co.paralleluniverse.comsat.webactors.undertow.AutoWebActorHandler;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
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
import paris.benoit.mob.server.ClusterReceiver;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class UndertowFront implements ClusterFront {
    private static final Logger logger = LoggerFactory.getLogger(UndertowFront.class);

    private final int port;
    private final String baseUrl;
    private MobClusterConfiguration configuration;
    private CompletableFuture<Void> upFuture;

    public UndertowFront(int port) {
        super();
        this.port = port;
        this.baseUrl = "http://localhost:" + port;
    }

    @Override
    public void start() {

        final SessionManager sessionManager = new InMemorySessionManager("SESSION_MANAGER", 1, true);
        final SessionCookieConfig sessionConfig = new SessionCookieConfig();
        sessionConfig.setMaxAge(60);
        final SessionAttachmentHandler sessionAttachmentHandler = new SessionAttachmentHandler(sessionManager, sessionConfig);

        Map<String, HttpHandler> fileHandlersMap = configuration.apps
                .stream()
                .map(it -> it.name)
                .collect(Collectors.toMap(Function.identity(), it ->
                        Handlers.disableCache(
                                Handlers.resource(
                                        new PathResourceManager(Paths.get(System.getProperty("user.dir") + "/apps/" + it + "/public"), 100)
                                ).setWelcomeFiles("index.html")
                        )
                ))
        ;

        SessionAttachmentHandler actorHandler = sessionAttachmentHandler.setNext(new AutoWebActorHandler());
        PathHandler handlers = Handlers.path().addPrefixPath("/service", actorHandler);

        fileHandlersMap.forEach((key, value) -> handlers.addPrefixPath("/app/" + key, value));
        handlers.addPrefixPath("/", fileHandlersMap.get(configuration.apps.get(0).name));

        Undertow server = Undertow.builder().addHttpListener(port, "0.0.0.0").setHandler(handlers).build();

        server.start();
        logger.info("Undertow is up at: " + baseUrl);
        
        upFuture = pokeActorService();

    }

    private CompletableFuture<Void> pokeActorService() {
        return CompletableFuture.runAsync(() -> {
            try {
                // the first one always fails for some reason
                HttpClients.createDefault().execute(new HttpGet(baseUrl + "/service")).getStatusLine().getStatusCode();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void waitReady() throws ExecutionException, InterruptedException {
        upFuture.get();
    }

    @Override
    public String accessString() {
        return baseUrl;
    }

    @Override
    public void configure(MobClusterConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public ClusterReceiver getClusterReceiver() {
        return new UndertowActorClusterReceiver();
    }
}
