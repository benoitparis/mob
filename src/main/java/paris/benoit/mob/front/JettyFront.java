package paris.benoit.mob.front;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.PathResource;
import paris.benoit.mob.server.ClusterFront;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JettyFront implements ClusterFront {
    private final int port;
    private final String baseUrl;
    private String mainApp;
    private CompletableFuture<Void> upFuture;

    public JettyFront(int port) {
        super();
        this.port = port;
        this.baseUrl = "http://localhost:" + port;
    }

    @Override
    public void start() {
        Server server = new Server(port);

        Spliterator<Path> files;
        try {
            files = Files
                    .newDirectoryStream(Paths.get(System.getProperty("user.dir") + "/apps/"))
                    .spliterator();
        } catch (IOException e) {
            files = Spliterators.emptySpliterator();
        }
        List<ContextHandler> fileHandlers = StreamSupport
            .stream(files, false)
            .filter(Files::isDirectory)
            .map(it -> it.getFileName().toString())
            .flatMap(it -> {
                    PathResource pathResource = new PathResource(Paths.get(System.getProperty("user.dir") + "/apps/" + it + "/public"));
                    ResourceHandler resourceHandler = new ResourceHandler();
                    resourceHandler.setDirectoriesListed(true);
                    resourceHandler.setWelcomeFiles(new String[]{"index.html"});
                    resourceHandler.setBaseResource(pathResource);
                    ContextHandler contextHandler = new ContextHandler("/app/" + it);
                    contextHandler.setHandler(resourceHandler);
                    if (it.equalsIgnoreCase(mainApp)) {
                        ContextHandler mainContextHandler = new ContextHandler("/");
                        mainContextHandler.setHandler(resourceHandler);
                        mainContextHandler.setWelcomeFiles(new String[]{"index.html"});
                        return Stream.of(mainContextHandler, contextHandler);
                    } else {
                        return Stream.of(contextHandler);
                    }
                }
            ).collect(Collectors.toList());

        Handler[] handlersArray = fileHandlers.stream().collect(Collectors.toList()).toArray(new Handler[]{});
        HandlerList handlers = new HandlerList();
        handlers.setHandlers(handlersArray);

        ServletContextHandler wsContextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        wsContextHandler.setContextPath("/service/");
        ServletHolder holderEvents = new ServletHolder(JettyEventServlet.class);
        wsContextHandler.addServlet(holderEvents, "/ws/*");

        handlers.addHandler(wsContextHandler);
        handlers.addHandler(new DefaultHandler());

        server.setHandler(handlers);

        try {
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void waitReady() throws ExecutionException, InterruptedException {
        // TODO
//        upFuture.get();
    }

    @Override
    public String accessString() {
        return baseUrl;
    }

    @Override
    public void setMain(String app) {
        this.mainApp = app;
    }










}
