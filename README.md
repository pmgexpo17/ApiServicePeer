# ApiServicePeer
A python apscheduler variant for hosting api services to enable cloud apps integration

The design intention of a smart job is to enable a group of actors to each run a state
machine as a subprogram of an integrated super program
The wikipedia (https://en.wikipedia.org/wiki/Actor_model) software actor description says :

  In response to a message that it receives, an actor can : make local decisions, 
  create more actors, send more messages, and determine how to respond to the 
  next message received. Actors may modify their own private state, but can only 
  affect each other through messages (avoiding the need for any locks)

PM 22-09-2018<br>
Improved ApiInstaller.py to support client application access of installation meta data<br>
Now exports api meta info by service/server name to a predefined meta file, apimeta.txt

apiDomain|wcEmulation|localhost:5000<br>
apiDomain|csvChecker|localhost:5000<br>
apiRoot|default|/apps/webapi<br>
apiRoot|<server_name>|/apps/dev1/webapi<br>
sysRoot=default|/data/BASESAS/api<br>
sysRoot|<server_name>|/data/saslib/api<br>

Added sas client sample, see ExampleClient content

PM 05-11-2018<br>
I'm now refactoring to make a Kubernetes-docker application for cloud integration
