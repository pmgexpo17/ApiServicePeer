# ApiServicePeer
A python peer-to-peer framework for hosting api services for cloud apps integration

Note : this is a proof of concept prototype version. The enterprise prototype version is available by consultation

Core design feature : A [UML sequence diagram](http://www.agilemodeling.com/artifacts/sequenceDiagram.htm) of a distributed program is converted to a micro-services collection installed by docker in a server cluster. The code here only documents the micro services part. The front-end web app for UML conversion is currently in development.

The design intention of a smart job is to enable a group of actors to each run a state
machine as a subprogram of an integrated super program
The wikipedia [software actor](https://en.wikipedia.org/wiki/Actor_model) description says :

  In response to a message that it receives, an actor can : make local decisions, 
  create more actors, send more messages, and determine how to respond to the 
  next message received. Actors may modify their own private state, but can only 
  affect each other through messages (avoiding the need for any locks)

- Added notes to app.apibase.handler.serviceHA.advance to explain what might be unexpected behaviour

Note : because this version is not for production, the state transition is not resolved by decision tree or rule evaluation as would be the case where the state transition has more than the one condition. This basic version has 1 condition, ie, resolver[state.current] completes successfully. In this verison, the state change parameters, like inTransition and hasSignal are static, ie, they are determined by the high-level program design. Normally, the state iteration would happen at resolver[state.current] completion. But here it happens when resolver[state.current] is called because the @iterate(actorKey) function updates the state when __call__ runs. Normally, on resolver[state.current] successful completion, the handler.advance method will call state.iterate() to advance the state machine, THEN it will evaluate state.inTransition and state.hasSignal

