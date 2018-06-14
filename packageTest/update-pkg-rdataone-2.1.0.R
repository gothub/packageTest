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
#d1c <- D1Client("DEV", "urn:node:mnDemo6")
#d1c <- D1Client("STAGING", "urn:node:mnStageUCSB2")
#d1c <- D1Client("PROD", "urn:node:GOA")
#d1c <- D1Client("PROD", "urn:node:KNB")
# Set 'subject' to authentication subject, if available, so we will have permission to change this object
dp <- new("DataPackage")

#setwd("/Users/slaughter/Projects/DataONE/rdataone/modUpdatePkgTest")

# Create metadata object that describes science data
dataDir <- "/Users/slaughter/Projects/DataONE/rdataone/data"
emlFile <- paste0(dataDir, "/strix-pacific-northwest.xml")
metadataObj <- new("DataObject", format="eml://ecoinformatics.org/eml-2.1.1", filename=emlFile)
metadataId <- getIdentifier(metadataObj)
# Associate the metadata object with each data object using the 'insertRelationships' method.
# Since a relationship type (the predicate argument) is not specified, the default relationship
# of 'cito:documents' is used, to indicate the the metadata object documents each data object.
# See "http://vocab.ox.ac.uk/cito", for further information about the "Citation Type Ontology".
dp <- addMember(dp, metadataObj)

sourceData <- paste0(dataDir, "/OwlNightj.csv")
sourceObj <- new("DataObject", format="text/csv", filename=sourceData)
dp <- addMember(dp, sourceObj, metadataObj)

resolveURL <- sprintf("%s/%s/object", d1c@mn@baseURL, d1c@mn@APIversion)
# Update the distribution URL in the metadata with the identifier that has been assigned to
# this DataObject. This provides a direct link between the detailed information for this package
# member and DataONE, which will assist DataONE in accessing and displaying this detailed information.
#xpathToURL <- "//dataTable/physical/distribution[../objectName/text()=\"OwlNightj.csv\"]/online/url"
#newURL <- sprintf("%s/%s", resolveURL, getIdentifier(sourceObj))
#dp <- updateMetadata(dp, metadataId, xpath=xpathToURL, newURL)
#metadataId <- selectMember(dp, name="sysmeta@formatId", value="eml://ecoinformatics.org/eml-2.1.1")
#metadataObj <- getMember(dp, metadataId)

progFile <- paste0(dataDir, "/filterObs.R")
progObj <- new("DataObject", format="application/R", filename=progFile, mediaType="text/x-rsrc")
dp <- addMember(dp, progObj, metadataObj)

#writeLines(rawToChar(getData(dp, getIdentifier(metadataObj))), "/tmp/foo.xml")

#xpathToURL <- "//otherEntity/physical/distribution[../objectName/text()=\"filterObs.R\"]/online/url"
#newURL <- sprintf("%s/%s", resolveURL, getIdentifier(progObj))
#dp <- updateMetadata(dp, metadataId, xpath=xpathToURL, newURL)
#metadataId <- selectMember(dp, name="sysmeta@formatId", value="eml://ecoinformatics.org/eml-2.1.1")
#metadataObj <- getData(dp, metadataId)

outputData <- paste0(dataDir, "/Strix-occidentalis-obs.csv")
outputObj <- new("DataObject", format="text/csv", filename=outputData )
dp <- addMember(dp, outputObj, metadataObj)

#xpathToURL <- "//dataTable/physical/distribution[../objectName/text()=\"Strix-occidentalis-obs.csv\"]/online/url"
#newURL <- sprintf("%s/%s", resolveURL, getIdentifier(outputObj))
#dp <- updateMetadata(dp, metadataId, xpath=xpathToURL, newURL)

#dp <- describeWorkflow(dp, sources=sourceObj, program=progObj, derivations=outputObj)

# Upload the data package to DataONE
resmapId <- uploadDataPackage(d1c, dp, public=TRUE, quiet=FALSE)
resourceMapId <- dp@resmapId
#resourceMapId <- uploadDataPackage(d1c, dp, public=TRUE, quiet=FALSE)

# Sleep for 90 secondsl to let indexing finish for the #package. Because we are imposing a wait on this
# package, this test is not suitable for use in CRAN. 
Sys.sleep(90)

# Now download the package, lazy loading objects, and check that the package has been downloaded 
# correctly.
if(!is.na(resourceMapId)) {
  
  pkg <- getDataPackage(d1c, identifier=resourceMapId, lazyLoad=TRUE, limit="0MB", quiet=FALSE)
  
}

