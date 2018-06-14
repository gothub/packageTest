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
#preferredNodes <- NA
# Set 'subject' to authentication subject, if available, so we will have permission to change this object
dp <- new("DataPackage")

setwd("/Users/slaughter/Projects/DataONE/rdataone/modUpdatePkgTest")

# Create metadata object that describes science data
emlFile <- system.file("extdata/strix-pacific-northwest.xml", package="dataone")
metadataDoc <- read_xml(emlFile, encoding = "", as_html = FALSE, options = "NOBLANKS")

sourceData <- system.file("extdata/sample.csv", package="dataone")
sourceObj <- new("DataObject", format="text/csv", filename=sourceData, suggestedFilename=basename(sourceData))
dp <- addMember(dp, sourceObj)

# Update the distribution URL in the metadata with the identifier that has been assigned to
# this DataObject. This provides a direct link between the detailed information for this package
# member and DataONE, which will assist DataONE in accessing and displaying this detailed information.
urlXpath <- sprintf("//otherEntity/physical/distribution[../objectName/text()=\"%s\"]/online/url", sourceObj@sysmeta@fileName)
urlNode <- xml_find_first(metadataDoc,  xpath=urlXpath, ns = xml_ns(metadataDoc))
xml_text(urlNode) <- sprintf("%s/object/%s", d1c@mn@baseURL, getIdentifier(sourceObj))

progFile <- system.file("extdata/filterSpecies.R", package="dataone")
progObj <- new("DataObject", format="application/R", filename=progFile, mediaType="text/x-rsrc", suggestedFilename=basename(progFile))
dp <- addMember(dp, progObj)
urlXpath <- sprintf("//otherEntity/physical/distribution[../objectName/text()=\"%s\"]/online/url", progObj@sysmeta@fileName)
urlNode <- xml_find_first(metadataDoc,  xpath=urlXpath, ns = xml_ns(metadataDoc))
xml_text(urlNode) <- sprintf("%s/object/%s", d1c@mn@baseURL, getIdentifier(progObj))

outputData <- system.file("extdata/filteredSpecies.csv", package="dataone")
outputObj <- new("DataObject", format="text/csv", filename=outputData, suggestedFilename=basename(outputData))
dp <- addMember(dp, outputObj)
urlXpath <- sprintf("//otherEntity/physical/distribution[../objectName/text()=\"%s\"]/online/url", outputObj@sysmeta@fileName)
urlNode <- xml_find_first(metadataDoc,  xpath=urlXpath, ns = xml_ns(metadataDoc))
xml_text(urlNode) <- sprintf("%s/object/%s", d1c@mn@baseURL, getIdentifier(outputObj))


# Write the modified metadata file to a temp file, from which the metadata DataObject will be created..
updatedEMLfile <- tempfile(pattern="strix-pacific-northwest", fileext=".xml")
write_xml(metadataDoc, updatedEMLfile)
metadataObj <- new("DataObject", format="eml://ecoinformatics.org/eml-2.1.1", filename=updatedEMLfile, suggestedFilename="strix-pacific-northwest.xml")
dp <- addMember(dp, metadataObj)

# Associate the metadata object with each data object using the 'insertRelationships' method.
# Since a relationship type (the predicate argument) is not specified, the default relationship
# of 'cito:documents' is used, to indicate the the metadata object documents each data object.
# See "http://vocab.ox.ac.uk/cito", for further information about the "Citation Type Ontology".
for(thisPid in getIdentifiers(dp)) {
  # Skip over the package member for the metadata object, as the metadata object
  # doesn't need to document itself.
  if(thisPid == getIdentifier(metadataObj)) next
  dp <- insertRelationship(dp, subjectID=getIdentifier(metadataObj), objectIDs=thisPid)
}

#dp <- describeWorkflow(dp, sources=sourceObj, derivations=outputObj)
#dp <- describeWorkflow(dp, sources=sourceObj, program=progObj, derivations=outputObj, insertDerivations=FALSE)
dp <- describeWorkflow(dp, sources=sourceObj, program=progObj, derivations=outputObj)

# Now update the metadata object with the re-writen file, as we have updated the in-memory version (metadataDoc), but need to
# now update the DataObject version.
#mfile <- tempfile(pattern="strix-pacific-northwest", fileext=".xml")
#write_xml(metadataDoc, mfile)
#dp <- replaceMember(dp, metadataObj, replacement=mfile)

# CHeck that the metadata object has been replaced correctly in the DataPackage and
# that correct values were calculated for the replacing object.
#fileinfo <- file.info(mfile)
#filesha1 <- digest(mfile, algo="sha1", serialize=FALSE, file=TRUE)
#mo <- getMember(dp, getIdentifier(metadataObj))

# Upload the data package to DataONE

resourceMapId <- uploadDataPackage(d1c, dp, public=TRUE, quiet=FALSE)
#resourceMapId <- uploadDataPackage(d1c, dp, replicate=TRUE, numberReplicas=1, preferredNodes=preferredNodes,  public=TRUE, quiet=FALSE,
#                                   packageId="urn:uuid:8670dbf3-36c8-4b5f-87ac-bb5babe31234")

# Sleep for a minute, to let indexing finish for the package. Because we are imposing a wait on this
# package, this test is not suitable for use in CRAN. It's not advisable to use a package in production,
# as accesses by the test routines will artificially inflate the DataONE usage statistics for the package.
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
  pkg <- getDataPackage(d1c, identifier=resourceMapId, lazyLoad=TRUE, limit="0MB", quiet=FALSE)
  
  # Get the downloaded metadata, so that we can update the distribution urls
  metadataId <- selectMember(pkg, name="sysmeta@formatId", value="eml://ecoinformatics.org/eml-2.1.1")
  metadataObj <- getMember(pkg, metadataId)
  metadataDoc <- read_xml(getData(pkg, metadataId), encoding = "", as_html = FALSE, options = "NOBLANKS")
  
  # Replace the output csv with a zipped version
  # First find the identifier for the object we want to change.
  id <- selectMember(pkg, name="sysmeta@fileName", value='filteredSpecies.csv')
  
  zipfile <- system.file("extdata/filteredSpecies.csv.zip", package="dataone")
  newId <- sprintf("urn:uuid:%s", UUIDgenerate())
  # The replaceMember() method replaces the data content of the DataObject, updates the relevant system metadata elements such as 'size' and 'checksum'
  # and updates the package relationships to replace the old DataPackage identifier with the new one. If the 'newId' parameter is not
  # specified for 'replaceMember()', then one will be automatically generated.
  pkg <- replaceMember(pkg, id, replacement=zipfile, formatId="application/octet-stream", 
                       suggestedFilename=basename(zipfile), 
                       newId=newId)
  setwd(saveDir)
  
  # Metadata still has the old filename for this object, so update now
  nameXpath <- sprintf("//otherEntity/physical/objectName[text()=\"%s\"]", "filteredSpecies.csv")
  nameNode <- xml_find_first(metadataDoc,  xpath=nameXpath, ns = xml_ns(metadataDoc))
  xml_text(nameNode) <- basename(zipfile)
  
  # Update the distribution URL with the new id from 'replaceMember'
  urlXpath <- sprintf("//otherEntity/physical/distribution[../objectName/text()=\"%s\"]/online/url", basename(zipfile))
  urlNode <- xml_find_first(metadataDoc,  xpath=urlXpath, ns = xml_ns(metadataDoc))
  xml_text(urlNode) <- sprintf("%s/object/%s", d1c@mn@baseURL, newId)
  
  # Now add a new package member that was omitted from the original package
  auxFile <- system.file("extdata/collectionMethods.csv", package="dataone")
  auxObj <- new("DataObject", format="text/csv", filename=auxFile, suggestedFilename=basename(auxFile))
  pkg <- addMember(pkg, auxObj)
  urlXpath <- sprintf("//otherEntity/physical/distribution[../objectName/text()=\"%s\"]/online/url", auxObj@sysmeta@fileName)
  urlNode <- xml_find_first(metadataDoc,  xpath=urlXpath, ns = xml_ns(metadataDoc))
  xml_text(urlNode) <- sprintf("%s/object/%s", d1c@mn@baseURL, getIdentifier(auxObj))
  
  pkg <- insertRelationship(pkg, subjectID=getIdentifier(metadataObj), objectIDs=getIdentifier(auxObj))
  
  # Update the metadata DataObject, so that the distribution urls will match the ew DataObject identifiers
  mfile <- tempfile(pattern="strix-pacific-northwest", fileext=".xml")
  write_xml(metadataDoc, mfile)
  
  pkg <- replaceMember(pkg, metadataId, replacement=mfile, 
                       newId=sprintf("urn:uuid:%s", UUIDgenerate(), 
                                     suggestedFilename="strix-pacific-northwest.xml"))
  
  newResmapId <- uploadDataPackage(d1c, pkg, public=TRUE, quiet=FALSE)
}
