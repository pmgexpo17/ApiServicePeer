# ApiServicePeer
A python apscheduler variant for hosting api services to enable cloud apps integrate

The design intention of a smart job is to enable a group of actors to each run a state
machine as a subprogram of an integrated super program
The wikipedia (https://en.wikipedia.org/wiki/Actor_model) software actor description says :

  In response to a message that it receives, an actor can : make local decisions, 
  create more actors, send more messages, and determine how to respond to the 
  next message received. Actors may modify their own private state, but can only 
  affect each other through messages (avoiding the need for any locks)
