package paris.benoit.mob.front;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.PathResource;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.server.ClusterFront;
import paris.benoit.mob.server.ClusterReceiver;

import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JettyFront implements ClusterFront {
    private final int port;
    private final String baseUrl;
    private MobClusterConfiguration configuration;

    public JettyFront(int port) {
        super();
        this.port = port;
        this.baseUrl = "http://localhost:" + port;
    }

    @Override
    public void start() {
        Server server = new Server(new JettyLoomThreadPool());

        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);

        server.setConnectors(new Connector[]{connector});


        String mainApp = configuration.apps.get(0).name;

        List<ContextHandler> fileHandlers = configuration.apps
            .stream()
            .map(it -> it.name)
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
    public String accessString() {
        return baseUrl;
    }

    @Override
    public void configure(MobClusterConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public ClusterReceiver getClusterReceiver() {
        return new JettyClusterReceiver();
    }


}
