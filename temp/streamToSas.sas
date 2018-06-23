%macro makeDsByStream(tableName, params=);

  filename apiuri url "http://localhost:5000/api/v1/data?&params" debug;

  DATA trnswrk.&tableName;
    infile apiuri dlmstr='<>' truncover lrecl=2000;
    length 
  {% for item in sasDefn %}
    {{ item[0] }} {{ item[1] }}
  {% endfor %}
    ;
    input 
  {% for item in sasDefn %}
    {{ item[0] }} {{ item[2] }}
  {% endfor %}
    ;
  RUN;
  
%mend;

%macro makeWcInputDs;

  %let jobId = {{ params.jobId }};
  %let tableName = {{ params.tableName }};
  %let trnswrk = {{ params.trnswrk }};

  DATA _null_;
    params = cat('id=',"&jobId",'&dataKey=',"&tableName");
    put 'params : ' params;
    call symput('params',strip(params));
  RUN;

  libname trnswrk "&trnswrk";

  %makeDsByStream(&tableName,params=%superq(params));
  
%mend;
%makeWcInputDs;
