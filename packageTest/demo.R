library(EML)
library(arcticdatautils)

# Load the EML file into R
emlFile <- "strix-pacific-northwest.xml"
#doc <- read_eml(emlFile)
doc <- EML::read_eml('https://raw.githubusercontent.com/NCEAS/oss-lessons/gh-pages/publishing-data/strix-pacific-northwest.xml')

# Change creator to us
doc@dataset@creator <- c(eml_creator("Peter", "Slaughter", email = "slaughter@nceas.ucsb.edu"))

# Change abstract to the better one we wrote
doc@dataset@abstract <- as(set_TextType("better-abstract.md"), "abstract")

# Save it back to the filesystem
write_eml(doc, "strix-pacific-northwest.xml")



library(dataone)
library(datapack)
library(uuid)

d1c <- D1Client("STAGING2", "urn:node:mnTestKNB")
dp <- new("DataPackage")
show(dp)

# Generate identifiers for our data and program objects, and add them to the metadata
sourceId <- paste0("urn:uuid:", uuid::UUIDgenerate())
progId <- paste0("urn:uuid:", uuid::UUIDgenerate())
outputId <- paste0("urn:uuid:", uuid::UUIDgenerate())

doc@dataset@otherEntity[[1]]@id <- new("xml_attribute", sourceId)
doc@dataset@otherEntity[[2]]@id <- new("xml_attribute", progId)
doc@dataset@otherEntity[[3]]@id <- new("xml_attribute", outputId)
repo_obj_service <- paste0(d1c@mn@endpoint, "/object/")
doc@dataset@otherEntity[[1]]@physical[[1]]@distribution[[1]]@online@url <- 
  new("url", paste0(repo_obj_service, sourceId))
doc@dataset@otherEntity[[2]]@physical[[1]]@distribution[[1]]@online@url <- 
  new("url", paste0(repo_obj_service, progId))
doc@dataset@otherEntity[[3]]@physical[[1]]@distribution[[1]]@online@url <- 
  new("url", paste0(repo_obj_service, outputId))

write_eml(doc, "strix-pacific-northwest.xml")

# Add the metadata document to the package
metadataObj <- new("DataObject", 
                   format="eml://ecoinformatics.org/eml-2.1.1", 
                   filename=paste(getwd(), emlFile, sep="/"))
dp <- addMember(dp, metadataObj)

# Add our input data file to the package
sourceData <- "sample.csv"
sourceObj <- new("DataObject",
                 id = sourceId,
                 format="text/csv", 
                 filename=paste(getwd(), sourceData, sep="/"))
dp <- addMember(dp, sourceObj, metadataObj)

# Add our processing script to the package
progFile <- "filterSpecies.R"
progObj <- new("DataObject",
               id = progId,
               format="application/R", 
               filename=paste(getwd(), progFile, sep="/"), 
               mediaType="text/x-rsrc")
dp <- addMember(dp, progObj, metadataObj)

# Add our derived output data file to the package
outputData <- "filteredSpecies.csv"
outputObj <- new("DataObject", 
                 id = outputId,
                 format="text/csv", 
                 filename=paste(getwd(), outputData, sep="/"))
dp <- addMember(dp, outputObj, metadataObj)

myAccessRules <- data.frame(subject="http://orcid.org/0000-0003-0077-4738", permission="changePermission") 

# Add the provenance relationships to the data package
dp <- describeWorkflow(dp, sources=sourceObj, program=progObj, derivations=outputObj)

show(dp)

packageId <- uploadDataPackage(d1c, dp, public=TRUE, accessRules=myAccessRules, quiet=FALSE)
