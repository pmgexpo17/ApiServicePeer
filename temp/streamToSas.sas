%macro makeDsByStream(tableName, reclen, params=);

  /*filename apiuri url "http://localhost:5000/api/v1/data?&params" debug;*/
  filename apiuri pipe "curl -s http://localhost:5000/api/v1/sync -d 'job=&params'";

  DATA trnswrk.&tableName;
    infile apiuri dlmstr='<>' truncover lrecl=&reclen;
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

  %let tableName = {{ params.tableName }};
  %let trnswrk = {{ params.trnswrk }};
  %let reclen = {{ params.reclen }};
  %let jparams = {{ params.job }}

  libname trnswrk "&trnswrk";

  %makeDsByStream(&tableName,&reclen,params=%superq(jparams));
  
%mend;
%makeWcInputDs;
