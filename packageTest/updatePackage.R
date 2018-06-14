library(dataone)
library(datapack)
library(xml2)
library(digest)
library(uuid)
# Create a csv file for the science object
d1c <- D1Client("STAGING2", "urn:node:mnTestKNB")

# Lazyload the DataPackage from the repository. Any DataObject that is larger that the value of
# the 'limit' parameter will have only system information downloaded and not the data. The exception
# are the metadata object and resource maps for a package which will always be downloaded, as these
# are usually needed to make updates to the package.

id <- "resource_map_urn:uuid:b8254d28-fd98-4bd5-9c86-63ab69516bf5"
pkg <- getDataPackage(d1c, identifier=id, lazyLoad=TRUE, limit="0MB", quiet=FALSE)

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

resmapId <- uploadDataPackage(d1c, pkg, public=TRUE, quiet=FALSE)
