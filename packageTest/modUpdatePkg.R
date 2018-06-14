library(dataone)
library(datapack)
library(xml2)
library(digest)
library(uuid)
# Create a csv file for the science object
#d1c <- D1Client("STAGING", "urn:node:mnStageUCSB2")
#d1c <- D1Client("DEV2", "urn:node:mnDevUCSB1")
d1c <- D1Client("STAGING2", "urn:node:mnTestKNB")
#d1c <- D1Client("SANDBOX", "urn:node:mnSandboxUCSB1")
#d1c <- D1Client("STAGING", "urn:node:mnTestARCTIC")

#cn <- CNode("DEV2")
#mn <- MNode("https://mn-demo-8.test.dataone.org/knb/d1/mn/v2")
#d1c <- D1Client(cn,mn)

#d1c <- D1Client("DEV", "urn:node:mnDemo8")
#d1c <- D1Client("DEV", "urn:node:mnDevUCSB1")
#d1c <- D1Client("PROD", "urn:node:GOA")
#d1c <- D1Client("PROD", "urn:node:KNB")
# Set 'subject' to authentication subject, if available, so we will have permission to change this object
numberReplicas <- 1
preferredNodes <- list("urn:node:mnDemo9")
dp <- new("DataPackage")

#setwd("/Users/slaughter/Projects/DataONE/rdataone/modUpdatePkgTest")

# Create metadata object that describes science data
emlFile <- system.file("extdata/strix-pacific-northwest.xml", package="dataone")
metadataObj <- new("DataObject", format="eml://ecoinformatics.org/eml-2.1.1", filename=emlFile)
metadataId <- getIdentifier(metadataObj)
# Associate the metadata ttps://quality.nceas.ucsb.edu/quality/suites/arctic.data.center.suite.1/object with each data object using the 'insertRelationships' method.
# Since a relationship type (the predicate argument) is not specified, the default relationship
# of 'cito:documents' is used, to indicate the the metadata object documents each data object.
# See "http://vocab.ox.ac.uk/cito", for further information about the "Citation Type Ontology".
metadataObj <- addAccessRule(metadataObj, "http://orcid.org/0000-0003-2192-431X", "changePermission")
dp <- addMember(dp, metadataObj)

sourceData <- system.file("extdata/OwlNightj.csv", package="dataone")
sourceObj <- new("DataObject", format="text/csv", filename=sourceData)
sourceObj <- addAccessRule(sourceObj, "http://orcid.org/0000-0003-2192-431X", "changePermission")
dp <- addMember(dp, sourceObj, metadataObj)

resolveURL <- sprintf("%s/%s/object", d1c@mn@baseURL, d1c@mn@APIversion)
# Update the distribution URL in the metadata with the identifier that has been assigned to
# this DataObject. This provides a direct link between the detailed information for this package
# member and DataONE, which will assist DataONE in accessing and displaying this detailed information.
xpathToURL <- "//dataTable/physical/distribution[../objectName/text()=\"OwlNightj.csv\"]/online/url"
#newURL <- sprintf("%s/%s", resolveURL, URLencode(getIdentifier(sourceObj), reserved=TRUE))
newURL <- sprintf("%s/%s", resolveURL, getIdentifier(sourceObj))
dp <- updateMetadata(dp, metadataId, xpath=xpathToURL, newURL)
metadataId <- selectMember(dp, name="sysmeta@formatId", value="eml://ecoinformatics.org/eml-2.1.1")
metadataObj <- getMember(dp, metadataId)

progFile <- system.file("extdata/filterObs.R", package="dataone")
progObj <- new("DataObject", format="application/R", filename=progFile, mediaType="text/x-rsrc")
#progObj <- addAccessRule(progObj, "http://orcid.org/0000-0003-2192-431X", "changePermission")
dp <- addMember(dp, progObj, metadataObj)

#writeLines(rawToChar(getData(dp, getIdentifier(metadataObj))), "/tmp/foo.xml")

xpathToURL <- "//otherEntity/physical/distribution[../objectName/text()=\"filterObs.R\"]/online/url"
#newURL <- sprintf("%s/%s", resolveURL, URLencode(getIdentifier(progObj), reserved=TRUE))
newURL <- sprintf("%s/%s", resolveURL, getIdentifier(progObj))
dp <- updateMetadata(dp, metadataId, xpath=xpathToURL, newURL)
metadataId <- selectMember(dp, name="sysmeta@formatId", value="eml://ecoinformatics.org/eml-2.1.1")
metadataObj <- getMember(dp, metadataId)

outputData <- system.file("extdata/Strix-occidentalis-obs.csv", package="dataone")
outputObj <- new("DataObject", format="text/csv", filename=outputData )
#outputObj <- addAccessRule(outputObj, "http://orcid.org/0000-0003-2192-431X", "changePermission")
dp <- addMember(dp, outputObj, metadataObj)

xpathToURL <- "//dataTable/physical/distribution[../objectName/text()=\"Strix-occidentalis-obs.csv\"]/online/url"
#newURL <- sprintf("%s/%s", resolveURL, URLencode(getIdentifier(outputObj), reserved=TRUE))
newURL <- sprintf("%s/%s", resolveURL, getIdentifier(outputObj))
dp <- updateMetadata(dp, metadataId, xpath=xpathToURL, newURL)

dp <- describeWorkflow(dp, sources=sourceObj, program=progObj, derivations=outputObj)

# Upload the data package to DataONE
newPkg <- uploadDataPackage(d1c, dp, public=TRUE, quiet=FALSE, as="DataPackage")
resourceMapId <- newPkg@resmapId
#resourceMapId <- uploadDataPackage(d1c, dp, public=TRUE, quiet=FALSE)

# Sleep for 90 secondsl to let indexing finish for the #package. Because we are imposing a wait on this
# package, this test is not suitable for use in CRAN. 
Sys.sleep(90)

# Now download the package, lazy loading objects, and check that the package has been downloaded 
# correctly.
if(!is.na(resourceMapId)) {
  # Lazyload the DataPackage from the repository. Any DataObject that is larger that the value of
  # the 'limit' parameter will have only system information downloaded and not the data. The exception
  # are the metadata object and resource maps for a package which will always be downloaded, as these
  # are usually needed to make updates to the package.
  #pkg <- getDataPackage(d1c, identifier=resourceMapId, lazyLoad=FALSE, limit="1TB", quiet=FALSE)
  # Use a different object for the updated package so that we can compare the old and new
  #resourceMapId <- "urn:uuid:f1948c6d-a17d-42ad-aa7a-73ae0f0356af"
  
  pkg <- getDataPackage(d1c, identifier=resourceMapId, lazyLoad=TRUE, limit="0MB", quiet=FALSE)
  
  # Get the downloaded metadata, so that we can update the distribution urls
  metadataId <- selectMember(pkg, name="sysmeta@formatId", value="eml://ecoinformatics.org/eml-2.1.1")
  metadataObj <- getMember(pkg, metadataId)
  
  # Replace the output csv with a zipped version
  # First find the identifier for the object we want to change.
  objId <- selectMember(pkg, name="sysmeta@fileName", value='Strix-occidentalis-obs.csv')
  zipfile <- system.file("extdata/Strix-occidentalis-obs.csv.zip", package="dataone")
  # The replaceMember() method replaces the data content of the DataObject, updates the relevant system metadata elements such as 'size' and 'checksum'
  # and updates the package relationships to replace the old DataPackage identifier with the new one. If the 'newId' parameter is not
  # specified for 'replaceMember()', then one will be automatically generated.
  pkg <- replaceMember(pkg, objId, replacement=zipfile, formatId="application/octet-stream")
                       
  # Get the updated DataObject id
  objId <- selectMember(pkg, name="sysmeta@fileName", value='Strix-occidentalis-obs.csv.zip')
  
  # Metadata still has the old filename for this object, so update now
  #nameXpath <- "//otherEntity/physical/objectName[text()=\"filteredSpecies.csv\"]"
  nameXpath <- '//dataTable/physical/objectName[text()="Strix-occidentalis-obs.csv"]'
  newName <- basename(zipfile)
  pkg <- updateMetadata(pkg, metadataId, xpath=nameXpath, replacement=newName)
  metadataId <- selectMember(pkg, name="sysmeta@formatId", value="eml://ecoinformatics.org/eml-2.1.1")
  
  # Update the distribution URL with the new id from 'replaceMember'
  urlXpath <- sprintf("//dataTable/physical/distribution[../objectName/text()=\"%s\"]/online/url", basename(zipfile))
  newURL <- sprintf("%s/%s/object%s", d1c@mn@baseURL, d1c@mn@APIversion, objId)
  pkg <- updateMetadata(pkg, metadataId, xpath=urlXpath, replacement=newURL)
  metadataId <- selectMember(pkg, name="sysmeta@formatId", value="eml://ecoinformatics.org/eml-2.1.1")
  
  # Now add a new package member that was omitted from the original package
  auxFile <- system.file("extdata/WeatherInf.txt", package="dataone")
  auxObj <- new("DataObject", format="text/plain", file=auxFile)
  pkg <- addMember(pkg, auxObj, metadataId)
  
  urlXpath <- sprintf("//otherEntity/physical/distribution[../objectName/text()=\"%s\"]/online/url", auxObj@sysmeta@fileName)
  newURL <- sprintf("%s/%s/object%s", d1c@mn@baseURL, d1c@mn@APIversion, getIdentifier(auxObj))
  pkg <- updateMetadata(pkg, metadataId, xpath=urlXpath, replacement=newURL)
  
  metadataId <- selectMember(pkg, name="sysmeta@formatId", value="eml://ecoinformatics.org/eml-2.1.1")
  
  updatePkg <- uploadDataPackage(d1c, pkg, public=TRUE, quiet=FALSE, as="DataPackage")
  updateResmapId <- updatePkg@resmapId
}

# Download the original package and just add provenance relationships
# This should cause the resource map object and pid to be updated only. The resource map
# pid should contain an increment number, since the metadata pid was used for the original

rm(pkg)
#pkg <- getDataPackage(d1c, identifier=updateResmapId, lazyLoad=TRUE, limit="0MB", quiet=FALSE)
pkg <- getDataPackage(d1c, identifier=resourceMapId, lazyLoad=TRUE, limit="0MB", quiet=FALSE)

sourceObjId <- selectMember(pkg, name="sysmeta@fileName", value=basename(sourceData))
programObjId <- selectMember(pkg, name="sysmeta@formatId", value="application/R")
outputObjId <- selectMember(pkg, name="sysmeta@fileName", value=basename(outputData))

# Update the sysmeta for all objects - check that sysmeta is updated
pkg <- setValue(pkg, name="sysmeta@replicationAllowed", value=TRUE, identifiers=getIdentifiers(pkg))
pkg <- setValue(pkg, name="sysmeta@preferredNodes", value=list("urn:node:mnDemo9"), identifiers=getIdentifiers(pkg))

#getValue(pkg, name="sysmeta@replicationAllowed")
#getValue(pkg, name="sysmeta@preferredNodes")

pkg <- describeWorkflow(pkg, sources=sourceObjId, program=programObjId, derivations=outputObjId)
newId <- sprintf("urn:uuid:%s", UUIDgenerate())
newId
updatePkg <- uploadDataPackage(d1c, pkg, numberReplicas=numberReplicas, preferredNodes=preferredNodes, public=TRUE, quiet=FALSE, as="DataPackage", packageId=newId)

