# MOB: (M)assively (O)nline emergent (B)ehavior 

## Real-time crowd interactions server on top of distributed streaming

MOB consists of a WebSocket server, serviced by actors, that communicate to each other through distributed streaming with Flink and benefit from its low-latency map-reduce capabilities. 

A particular setup of stream processing enables controlling the co-location of system input and output, thus allowing messages to be routed to their intended recipients.

It is my opinion that this can enable crowds to interact with themselves; And that it opens a relatively easy pathway to try and test new ways to scale real-time multi-user experiences to massive sizes.

## A new design space

Pong is a 2-player game; And one can ask: what would be a 10k-player pong? There are lots of different answers, and MOB enables exploring this design space.

One app is available: 'pong'; where half of the players are on the left, half on the right. And the paddles' position is the average of each individual's position. Each crowd has to synchronize with itself in order to win.

## API

Developers only have to specify html and javascript; and SQL for distributed processing. A JS engine is made available inside the distributed stream, in order to share business logic that is common between client and server.

## Launching

We'll launch pong here. Place your app under the apps/ folder.

    git clone https://github.com/benoitparis/mob && cd mob
    mvnw -Dapp-name="pong" clean install exec:exec
