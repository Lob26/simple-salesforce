"""Salesforce API message templates"""

DEPLOY_MSG = """<soapenv:Envelope
        xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:met="http://soap.sforce.com/2006/04/metadata">
   <soapenv:Header>
      <met:CallOptions>
         <met:client>{client}</met:client>
      </met:CallOptions>
      <met:SessionHeader>
         <met:sessionId>{sessionId}</met:sessionId>
      </met:SessionHeader>
   </soapenv:Header>
   <soapenv:Body>
      <met:deploy>
         <met:ZipFile>{ZipFile}</met:ZipFile>
         <met:DeployOptions>
            <met:allowMissingFiles>{allowMissingFiles}</met:allowMissingFiles>
            <met:autoUpdatePackage>{autoUpdatePackage}</met:autoUpdatePackage>
            <met:checkOnly>{checkOnly}</met:checkOnly>
            <met:ignoreWarnings>{ignoreWarnings}</met:ignoreWarnings>
            <met:performRetrieve>{performRetrieve}</met:performRetrieve>
            <met:purgeOnDelete>{purgeOnDelete}</met:purgeOnDelete>
            <met:rollbackOnError>{rollbackOnError}</met:rollbackOnError>
            <met:singlePackage>{singlePackage}</met:singlePackage>
            {testLevel}
            {tests}
         </met:DeployOptions>
      </met:deploy>
   </soapenv:Body>
</soapenv:Envelope>"""

CHECK_DEPLOY_STATUS_MSG = """<soapenv:Envelope
xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:met="http://soap.sforce.com/2006/04/metadata">
   <soapenv:Header>
      <met:CallOptions>
         <met:client>{client}</met:client>
      </met:CallOptions>
      <met:SessionHeader>
         <met:sessionId>{sessionId}</met:sessionId>
      </met:SessionHeader>
   </soapenv:Header>
   <soapenv:Body>
      <met:checkDeployStatus>
         <met:asyncProcessId>{asyncProcessId}</met:asyncProcessId>
         <met:includeDetails>{includeDetails}</met:includeDetails>
      </met:checkDeployStatus>
   </soapenv:Body>
</soapenv:Envelope>"""

RETRIEVE_MSG = """<soapenv:Envelope
xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:met="http://soap.sforce.com/2006/04/metadata">
   <soapenv:Header>
      <met:CallOptions>
         <met:client>{client}</met:client>
      </met:CallOptions>
      <met:SessionHeader>
         <met:sessionId>{sessionId}</met:sessionId>
      </met:SessionHeader>
   </soapenv:Header>
   <soapenv:Body>
      <met:retrieve>
         <met:retrieveRequest>
            <met:apiVersion>{apiVersion}</met:apiVersion>
            <met:singlePackage>{singlePackage}</met:singlePackage>
            <met:unpackaged>{unpackaged}</met:unpackaged>
         </met:retrieveRequest>
      </met:retrieve>
   </soapenv:Body>
</soapenv:Envelope>"""

CHECK_RETRIEVE_STATUS_MSG = """<soapenv:Envelope
xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:met="http://soap.sforce.com/2006/04/metadata">
   <soapenv:Header>
      <met:CallOptions>
         <met:client>{client}</met:client>
      </met:CallOptions>
      <met:SessionHeader>
         <met:sessionId>{sessionId}</met:sessionId>
      </met:SessionHeader>
   </soapenv:Header>
   <soapenv:Body>
      <met:checkRetrieveStatus>
         <met:asyncProcessId>{asyncProcessId}</met:asyncProcessId>
         <met:includeZip>{includeZip}</met:includeZip>
      </met:checkRetrieveStatus>
   </soapenv:Body>
</soapenv:Envelope>"""
