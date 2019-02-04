package paris.benoit.massive.front.test;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.flink.api.java.functions.IdPartitioner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
import paris.benoit.mob.loopback.ActorEntrySource;
import paris.benoit.mob.loopback.ActorLoopBackSink;

public class StreamAndAppLauncher {
    // TODO use properly
//    static Logger LOG = LoggerFactory.getLogger(StreamAndAppLauncher.class);

    public static void main(String[] args) throws Exception {
        launchUntertow();
        launchStreamAndBlock();
    }

    // vCPU number, and we let Threads switch with the actor's ForkJoinPool's
    public final static int STREAM_PARALLELISM = 4;
    private static void launchStreamAndBlock() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setParallelism(STREAM_PARALLELISM);
        
        env .addSource(new ActorEntrySource())
            .partitionCustom(new IdPartitioner(), 0)
            .addSink(new ActorLoopBackSink());
        
        System.out.println("Stream is being initialized. Execution plan: {}"+ env.getExecutionPlan());
        
        // Blocking until cancellation
        env.execute();
        System.out.println("Stream END");
        
        
        // Bon, c'est clean so far. Sauf la partie acteur et lifecycle.
        // en SSE on a des reconnect, et WS ça marche juste pas.
        // Faudra ptet s'inspirer de ce qui se faisait et qui marchait.
        // Sinon les logs aussi faudra s'en charger. Plus propre et feeback loop moins couteuse (et on a tjs chrome pour les headers?)
        // Et faudra voir si on peut avoir des fichiers. Et si le trick du ../../../passwords est bien bloqué
        // Clean en fait c'est pour dire que archi ça roule. C'est un tapis in et out clean pour l'instant. Et avec tous les outils pour avancer.
        
        //  WS marchent!
        
    }
    
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
        
        final RequestDumpingHandler actorHandler = new RequestDumpingHandler(sessionAttachmentHandler.setNext(new AutoWebActorHandler()));
        
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
