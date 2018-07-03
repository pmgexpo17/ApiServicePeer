%macro makeDsByStream(tableName, reclen);

  DATA trnswrk.&tableName;
    infile STDIN dlmstr='<>' truncover lrecl=&reclen;
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

  libname trnswrk "&trnswrk";

  %makeDsByStream(&tableName,&reclen);
  
%mend;
%makeWcInputDs;
