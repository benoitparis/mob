# MOB: (M)assively (O)nline emergent (B)ehavior 

## Real-time crowd interactions server on top of distributed streaming

MOB consists of a websocket server, serviced by actors, that communicate to each other through distributed streaming with Flink and benefit from it's low-latency map-reduce capabilities. 

A particular setup of stream processing enables controlling the colocation of system input and output, thus allowing messages to be routed to their intended recipients.

It is my opinion that this can enable crowds to interact with themselves; And that it opens a relatively easy pathway to try and test new ways to scale real-time multi-user experiences to massive sizes.

## API

Developpers only have to specify html and javascript; and SQL for distributed processing. A JS engine is made available inside the distributed stream, in order to share business logic that is common between client and server.

## A new design space

Pong is a 2-player game; And one can ask: what would be a 10k-player pong? Each could have a different answer, and MOB can enable exploring this design space.

One app is available: 'hello-world-pong'; where half the players are on the left, the other half is on the right. And the paddles' position is the average of each individual's position. Each crowd has to synchronize with itself in order to win.

