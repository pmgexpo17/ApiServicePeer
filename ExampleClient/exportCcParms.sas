%macro exportCcParms;
  %put [START] exportCcParms;

  %if %symexist(_SASPROGRAMFILE) %then
	  %let scriptPath = %sysfunc(dequote(&_SASPROGRAMFILE));
  %else
	  %let scriptPath = %sysfunc(getoption(sysin));
  %put [INFO] script path : &scriptPath;
  
  %global linkBase;
  
  /* apparently EG sometimes does not reset syscc = 0 */
  %let syscc = 0;
  
  %put [STEP_01];
  DATA _null_;
    length linkBase $200;
    path = "&scriptPath";
    depth = countw(path,'/');
    linkBase = '';
    do i=1 to depth-1;
    	linkBase = cats(linkBase,'/',scan(path,i,'/'));
    end;
    call symput('linkBase',strip(linkBase));
  RUN;
  %put [INFO] syscc : &syscc;
  %if %errorquit(quitEOP=0) %then %return;
  
  %put linkBase : &linkBase;
  
  %let csvLocation = &linkBase;
  
  %if %length(&csvPath) > 0 %then %do;

    %put [INFO] csvPath : &csvPath;
    %let csvLocation = &linkBase/&csvPath;

  %end;
  
  %if NOT %sysfunc(fileexist(&csvLocation)) %then %do;
  
    %put [ERROR] csv location does not exist : &csvLocation;
    %if %errorquit(errcode=400,quitEOP=0) %then %return;
    
  %end;

  %let hasEmptyParam = %length(&jobTitle) = 0 or %length(&bizType) = 0 or %length(&gipeRegion) = 0;
  %let hasEmptyParam = &hasEmptyParam or %length(&productType) = 0 or %length(&tableList) = 0 or %length(&userEmail) = 0;
  
  %if &hasEmptyParam %then %do;
  
    %put [ERROR] at least 1 csvChecker job param is blank for params :;
    %put [ERROR] jobTitle, bizType, gipeRegion, productType, tableList, userEmail;
    %if %errorquit(errcode=400,quitEOP=0) %then %return;
    
  %end;
  
  %let goodEmailFormat = %index(&userEmail,%str(@));

  %put [INFO] email format status : &goodEmailFormat;

  %if NOT &goodEmailFormat %then %do;
  
    %put [ERROR] bad email address format : &userEmail;
    %if %errorquit(errcode=400,quitEOP=0) %then %return;
    
  %end;

  filename ccpmeta "&linkBase/ccPmeta.json";
  
  %put [STEP_02];
  DATA _null_;
    length param $200;
    file ccpmeta;
    put '{';
    put '  "Global" : {';
    param = cat('"apiRoot" : "',"&apiRoot",'",');
    put '    ' param;
    param = cat('"apiDomain" : "',"&apiDomain",'",');
    put '    ' param;
    param = cat('"assetLib" : "',"&sysRoot",'/assets",');
    put '    ' param;
    param = cat('"ccautoLib" : "',"&sysRoot",'/sasautos",');
    put '    ' param;
    param = cat('"linkBase" : "',"&linkBase",'",');
    put '    ' param;
    param = cat('"userEmail" : "',"&userEmail",'",');
    put '    ' param;
    param = cat('"workSpace" : "/data/userdata/',"&SYSUSERID",'"');
    put '    ' param;
    put '  },';
    put '  "CsvChecker" : {';
    param = cat('"jobTitle" : "',"&jobTitle",'",');    
    put '    ' param;
    param = cat('"bizType" : "',"&bizType",'",');    
    put '    ' param;
    param = cat('"csvLocation" : "',"&csvLocation",'",');    
    put '    ' param;
    param = cat('"gipeRegion" : "',"&gipeRegion",'",');    
    put '    ' param;
    param = cat('"productType" : "',"&productType",'",');    
    put '    ' param;
    param = cat('"tableList" : "',"&tableList",'",');    
    put '    ' param;
    put '    "globals" : ["*"]';
    put '  }';
    put '}';
  RUN;
   
  %put [END] exportCcParms;
%mend;
