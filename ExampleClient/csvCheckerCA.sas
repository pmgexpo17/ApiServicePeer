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
	DATA _null_;
  	attrib metaKey length = $16
		       metaVal length = $100
		       domain  length = $50;
  	infile "/apps/etc/apimeta.txt" delimiter = '=' MISSOVER DSD;
		input metaKey $ metaVal $;
		metaKey = strip(metaKey);
		metaVal = strip(metaVal);
		SELECT (metaKey);
			WHEN ('apiRoot') call symput('apiRoot',strip(metaVal));
			WHEN ('apiDomain') DO;
			  domain = strip(metaVal);
			  IF index(domain,'csvChecker') > 0 THEN DO;
			    domain = scan(domain,2,'|');
			    call symput('apiDomain',domain);
			  END;
			END;
			WHEN ('sysRoot') call symput('sysRoot',strip(metaVal));
			OTHERWISE;
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
