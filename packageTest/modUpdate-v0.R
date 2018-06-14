library(dataone)
library(datapack)
library(xml2)
library(digest)
library(uuid)
# Create a csv file for the science object
#d1c <- D1Client("STAGING", "urn:node:mnStageUCSB2")
d1c <- D1Client("DEV2", "urn:node:mnDevUCSB1")
#d1c <- D1Client("STAGING2", "urn:node:mnTestKNB")
#d1c <- D1Client("SANDBOX", "urn:node:mnSandboxUCSB1")
#d1c <- D1Client("DEV", "urn:node:mnDemo6")
#d1c <- D1Client("STAGING", "urn:node:mnStageUCSB2")
preferredNodes <- NA
# Set 'subject' to authentication subject, if available, so we will have permission to change this object
dp <- new("DataPackage")

setwd("/Users/slaughter/Projects/DataONE/rdataone/modUpdatePkgTest")

# Create metadata object that describes science data
#emlFile <- system.file("extdata/strix-pacific-northwest.xml", package="dataone")
emlFile <- "strix-pacific-northwest.xml"
metadataDoc <- read_xml(emlFile, encoding = "", as_html = FALSE, options = "NOBLANKS")
metadataObj <- new("DataObject", format="eml://ecoinformatics.org/eml-2.1.1", filename=emlFile, suggestedFilename=basename(emlFile))
dp <- addData(dp, metadataObj)

#sourceData <- system.file("extdata/sample.csv", package="dataone")
sourceData <- "sample.csv"
sourceObj <- new("DataObject", format="text/csv", filename=sourceData, suggestedFilename="sample.csv")
dp <- addData(dp, sourceObj, metadataObj)

#entityNode <- xml_find_first(metadataObj, xpath="//otherEntity/entityNmae=[@%s]/../", ns = xml_ns(x))
urlXpath <- sprintf("//otherEntity/physical/distribution[../objectName/text()=\"%s\"]/online/url", sourceObj@sysmeta@fileName)
urlNode <- xml_find_first(metadataDoc,  xpath=urlXpath, ns = xml_ns(metadataDoc))
xml_text(urlNode) <- sprintf("%s/object/%s", d1c@mn@baseURL, getIdentifier(sourceObj))

#progFile <- system.file("extdata/filterSpecies.R", package="dataone")
progFile <- "filterSpecies.R"
progObj <- new("DataObject", format="application/R", filename=progFile,
               mediaType="text/x-rsrc", suggestedFilename="filterSpecies.R")
dp <- addData(dp, progObj, metadataObj)
urlXpath <- sprintf("//otherEntity/physical/distribution[../objectName/text()=\"%s\"]/online/url", progObj@sysmeta@fileName)
urlNode <- xml_find_first(metadataDoc,  xpath=urlXpath, ns = xml_ns(metadataDoc))
xml_text(urlNode) <- sprintf("%s/object/%s", d1c@mn@baseURL, getIdentifier(progObj))

#outputData <- system.file("extdata/filteredSpecies.csv", package="dataone")
outputData <- "filteredSpecies.csv"
outputObj <- new("DataObject", format="text/csv", filename=outputData, suggestedFilename="filteredSpecies.csv")
dp <- addData(dp, outputObj, metadataObj)
urlXpath <- sprintf("//otherEntity/physical/distribution[../objectName/text()=\"%s\"]/online/url", outputObj@sysmeta@fileName)
urlNode <- xml_find_first(metadataDoc,  xpath=urlXpath, ns = xml_ns(metadataDoc))
xml_text(urlNode) <- sprintf("%s/object/%s", d1c@mn@baseURL, getIdentifier(outputObj))

#auxFile <- system.file("extdata/collectionMethods.csv", package="dataone")
auxFile <- "collectionMethods.csv"
auxObj <- new("DataObject", format="text/csv", filename=auxFile, suggestedFilename="collectionMethods.csv")
dp <- addData(dp, auxObj, metadataObj)
urlXpath <- sprintf("//otherEntity/physical/distribution[../objectName/text()=\"%s\"]/online/url", auxObj@sysmeta@fileName)
urlNode <- xml_find_first(metadataDoc,  xpath=urlXpath, ns = xml_ns(metadataDoc))
xml_text(urlNode) <- sprintf("%s/object/%s", d1c@mn@baseURL, getIdentifier(auxObj))

#dp <- describeWorkflow(dp, sources=sourceObj, derivations=outputObj)
#dp <- describeWorkflow(dp, sources=sourceObj, program=progObj, derivations=outputObj, insertDerivations=FALSE)
dp <- describeWorkflow(dp, sources=sourceObj, program=progObj, derivations=outputObj)

# Now update the metadata object with the re-writen file, as we have updated the in-memory version (metadataDoc), but need to
# now update the DataObject version.
mfile <- tempfile(pattern="strix-pacific-northwest", fileext=".xml")
write_xml(metadataDoc, mfile)
dp <- replaceMember(dp, metadataObj, replacement=mfile)

# CHeck that the metadata object has been replaced correctly in the DataPackage and
# that correct values were calculated for the replacing object.
fileinfo <- file.info(mfile)
filesha1 <- digest(mfile, algo="sha1", serialize=FALSE, file=TRUE)
mo <- getMember(dp, getIdentifier(metadataObj))

# Upload the data package to DataONE

resourceMapId <- uploadDataPackage(d1c, dp, replicate=TRUE, numberReplicas=1, preferredNodes=preferredNodes,  public=TRUE, quiet=FALSE,
                                   packageId="urn:uuid:8670dbf3-36c8-4b5f-87ac-bb5babe3abcd")

# Sleep for a minute, to let indexing finish for the package. Because we are imposing a wait on this
# package, this test is not suitable for use in CRAN. It's not advisable to use a package in production,
# as accesses by the test routines will artificially inflate the DataONE usage statistics for the package.
# Sys.sleep(90)
# 
# # Now download the package, lazy loading objects, and check that the package has been downloaded 
# # correctly.
# if(!is.na(resourceMapId)) {
#   # It is necessary to download objects (not lazyload) if they are going to be replaced.
#   pkg <- getDataPackage(d1c, identifier=resourceMapId, lazyLoad=FALSE, limit="50GB", quiet=FALSE)
#   metadataId <- selectMember(pkg, name="sysmeta@formatId", value="eml://ecoinformatics.org/eml-2.1.1")
#   metadataObj <- getMember(pkg, metadataId)
#   metadataDoc <- read_xml(getData(pkg, metadataId), encoding = "", as_html = FALSE, options = "NOBLANKS")
#   
#   # Replace the output csv with a zipped versiono
#   zipfile <- tempfile(pattern="filteredSpecies.csv", fileext=".zip")
#   files <- system.file("extdata/filteredSpecies.csv", package="dataone")
#   zip(zipfile, files, flags = "-r9X", extras = "", zip = Sys.getenv("R_ZIPCMD", "zip"))
#   outputObjId <- selectMember(pkg, name="sysmeta@fileName", value="filteredSpecies.csv")
#   pkg <- replaceMember(pkg, outputObjId, replacement=zipfile, formatId="application/octet-stream",  suggestedFilename="filteredSpecies.csv.zip")
#   
#   urlXpath <- sprintf("//otherEntity/physical/distribution[../objectName/text()=\"%s\"]/online/url", outputObj@sysmeta@fileName)
#   urlNode <- xml_find_first(metadataDoc,  xpath=urlXpath, ns = xml_ns(metadataDoc))
#   xml_text(urlNode) <- sprintf("%s/object/%s", d1c@mn@baseURL, getIdentifier(auxObj))
#   
#   # Update the metadata DataObject, so that the distribution urls will match the new DataObject identifiers
#   mfile <- tempfile(pattern="strix-pacific-northwest", fileext=".xml")
#   write_xml(metadataDoc, mfile)
#   pkg <- replaceMember(pkg, metadataId, replacement=mfile)
#   
#   newOutputObjId <- sprintf("urn:uuid:%s", UUIDgenerate())
#   newIds <- data.frame(oldId=c(outputObjId), newId=c(newOutputObjId), stringsAsFactors = FALSE)
#   #newResmapId <- updateDataPackage(d1c, pkg, public=TRUE, quiet=FALSE, identifies=newIds)
# }
