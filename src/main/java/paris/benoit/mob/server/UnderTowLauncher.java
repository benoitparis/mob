package paris.benoit.mob.server;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.HttpClients;

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
    
    private static Undertow server;
    private static final int INET_PORT = 8090;
    
    public static void launchUntertow() throws Exception {
                
        final SessionManager sessionManager = new InMemorySessionManager("SESSION_MANAGER", 1, true);
        final SessionCookieConfig sessionConfig = new SessionCookieConfig();
        sessionConfig.setMaxAge(60);
        final SessionAttachmentHandler sessionAttachmentHandler =
            new SessionAttachmentHandler(sessionManager, sessionConfig);
        
        final ResourceHandler fileHandler = 
                Handlers.resource(new PathResourceManager(Paths.get(System.getProperty("user.dir") + "/static"), 100))
                    .setWelcomeFiles("index.html");
        
        final RequestDumpingHandler actorHandler = new RequestDumpingHandler(
                sessionAttachmentHandler.setNext(new AutoWebActorHandler())
        );
        
        final PathHandler routingHandler = Handlers.path()
            .addPrefixPath("/service", actorHandler)
            .addPrefixPath("/", fileHandler)
        ;
        
        server = Undertow.builder()
            .addHttpListener(INET_PORT, "localhost")
            .setHandler(routingHandler)
            .build();
        
        server.start();
        
        final String url = "http://localhost:" + INET_PORT;
        
        waitUrlAvailable(url);

        System.out.println("Undertow is up at: " + url);
    }
    
    public static void waitUrlAvailable(final String url) throws InterruptedException, IOException {
       for (;;) {
           Thread.sleep(10);
           try {
               if (HttpClients.createDefault().execute(new HttpGet(url)).getStatusLine().getStatusCode() > -100)
                   break;
           } catch (HttpHostConnectException ex) {}
       }
   }
    
}
