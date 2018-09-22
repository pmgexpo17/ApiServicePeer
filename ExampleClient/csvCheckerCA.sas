/*================================================================
 - csvCheckerCA - csvchecker api client version
 - PM 03-08-2018
 ================================================================*/
%macro csvCheckerCA;
  %put [START] csvCheckerCA;

  /* job title for email identification */
  %let jobTitle = Home PI LRF testing;
  /* bizType must be in (PI, CI, CR) */
  %let bizType = PI;
  /* the sub-directory path relative to this script run location */
  %let csvPath = Testing/20082018/home;
  /* gipeRegion is the oracle region to which csvs are loaded  */
  %let gipeRegion = EME0; 
  /* productType must be in (home, motor) */
  %let productType = home;
  /* space separated short table names for all related csvs */
  %let tableList = STE EXS LRF;
  /* space separated list of suncorp email addresses */
  %let userEmail = peter.mcgill@suncorp.com.au;
  /* workbench server name */
  %let serverName = wbp001001;

  %exportCcParms;
  %if %errorquit(quitEOP=0) %then %return;
  %runCcJob;

  %put [END] csvCheckerCA;
%mend;

/*================================================================
 - execCcJob
 - PM 20-09-2018
 ================================================================*/
%macro execCcJob;
  %put [START] execCcJob;
  
  %global apiRoot apiDomain sysRoot;

  %if NOT %sysfunc(fileexist('/apps/etc/apimeta.txt')) %then %do;
  
    %put [ERROR] /apps/etc/apimeta.txt is not found;
    %return;
    
  %end;
  %else
    %put [INFO] importing apimeta vars in /apps/etc/apimeta.txt;
    
	%put [STEP_01];
	DATA apiMeta_all;
  	attrib metaKey1 length = $30
  	       metaKey2 length = $100
		       metaVal length = $100;
  	infile "/apps/etc/apimeta.txt" delimiter = '|' MISSOVER DSD;
		input metaKey1 $ metaKey2 $ metaVal $;
	RUN;
	
	%put [STEP_02];	
	DATA _null_;
	  attrib clientItem length $100;
	  RETAIN clientItem;
	  SET apiMeta_all;
	  BY metaKey1;
	  IF FIRST.metaKey1 THEN
	    clientItem = '';
	  IF metaKey2 in ('csvChecker',"&serverName") THEN
	    clientItem = metaVal;	    
	  /* only set default if service/server name key is not found */
	  ELSE IF metaKey2 = 'default' AND clientItem = '' THEN
	    clientItem = metaVal;
	  IF LAST.metaKey1 THEN DO;
	    SELECT (metaKey1) DO;
	      WHEN ('apiDomain') call symput('apiDomain',strip(clientItem));
	      WHEN ('apiRoot') call symput('apiRoot',strip(clientItem));
	      WHEN ('sysRoot') call symput('sysRoot',strip(clientItem));
	      OTHERWISE;
	    END;
	  END;
  RUN;	  
	
  %put [INFO] sysRoot : &sysRoot;
  %put [INFO] apiDomain : &apiDomain;
  %put [INFO] apiRoot : &apiRoot;

  %let ccautoLib = &sysRoot/pi/csvcheck/sasautos;

  OPTIONS SASAUTOS = ("!SASROOT/sasautos","&ccautoLib"); 

  %if %errorquit(quitEOP=0) %then %return;
  
  %csvCheckerCA;

  %put [END] execCcJob;
%mend;
%execCcJob;
