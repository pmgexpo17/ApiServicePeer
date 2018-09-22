%macro runCcJob;
  %put [START] runCcJob;
  
  %let currTime = %sysfunc(time(),time.);
  %put [INFO] current time : &currTime;
  
  /* if api is not ready next data step will be empty, and call symputx is not run*/
  %let apiIsReady = 0;

  %let syscc = 0;
  
  %put [STEP_02];  
  data _null_;
    length status $16 pid $16;
    infile "curl -s http://&apiDomain/api/v1/ping" pipe dlm=',';
    input status $ pid $;
    put _infile_;
    apiIsReady = index(strip(status),'"status": 200') > 0;
    call symputx('apiIsReady',apiIsReady);
  run;
  %if %errorquit(quitEOP=0) %then %return;
  
  %put apiIsReady : &apiIsReady;

  %if &apiIsReady %then %do;
  
    %put [INFO] csvChecker api service is running;
    %goto SUBMIT_JOB;
  
  %end;
  
  %put [INFO] starting csvChecker api service ...;
  
  %put [STEP_02];
  systask command "/usr/bin/python &apiRoot/apiAgent.py csvChecker" nowait;

  %let currTime = %sysfunc(time(),time.);  
  %put [INFO] current time : &currTime;
  
  %put [STEP_03];
  data _null_;
    call sleep(5,1);
  run;
  
  %let currTime = %sysfunc(time(),time.);
  %put [INFO] current time : &currTime;
  
  %put [STEP_04];
  data _null_;
    length status $16 pid $16;
    infile "curl -s http://&apiDomain/api/v1/ping" pipe dlm=',';
    input status $ pid $;
    put _infile_;
    apiIsReady = index(strip(status),'"status": 200') > 0;
    call symputx('apiIsReady',apiIsReady);
  run;
  %if %errorquit(quitEOP=0) %then %return;
  
  %if NOT &apiIsReady %then %do;
  
    %put [EEOWW!] csvChecker api service is confirmed NOT running;
    %return;
  
  %end;  
  %else
    %put [INFO] csvChecker api service is confirmed running;

  %SUBMIT_JOB:

  %put [INFO] running job, expect an email soon ...;
  
  %put [STEP_05];
  systask command "/usr/bin/python &apiRoot/apiAgent.py csvChecker -s &linkBase" wait;  

  %put [END] runCcJob;
%mend;
