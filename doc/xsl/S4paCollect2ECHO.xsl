<?xml version="1.0" encoding="utf-8"?>
<!-- $Id: S4paCollect2ECHO.xsl,v 1.3 2007/02/26 18:21:12 hegde Exp $ -->
<!-- -@@@ S4PA, Version $Name:  $ -->
<xsl:stylesheet
          version="1.0"
          xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" indent="yes"/>
 <!-- This XSLT can be used to transform S4PA Granule level XML Meta-data
      files into ECHO ingest files. -->
  <xsl:template match="/">
    <xsl:for-each select="S4PACollectionMetaDataFile">
    <CollectionMetaDataFile>
      <DTDVersion>1.0</DTDVersion>
      <DataCenterId>GSF</DataCenterId>
        <CollectionMetaDataSets>
         <Collections>
           <CollectionMetaData>
              <xsl:copy-of select="CollectionMetaData/ShortName"/>
              <xsl:copy-of select="CollectionMetaData/VersionID"/>
              <xsl:copy-of select="CollectionMetaData/InsertTime"/>
              <xsl:copy-of select="CollectionMetaData/LastUpdate"/>
              <xsl:copy-of select="CollectionMetaData/LongName"/>
              <ArchiveCenter><xsl:value-of select="CollectionMetaData/DataCenter"/></ArchiveCenter>
              <Spatial>
                <SpatialCoverageType>Horizontal</SpatialCoverageType>
                <HorizontalSpatialDomain>
                  <Geometry>
                    <BoundingRectangle>
                      <xsl:copy-of select="CollectionMetaData/SpatialCoverage/WestBoundingCoordinate"/>
                      <xsl:copy-of select="CollectionMetaData/SpatialCoverage/NorthBoundingCoordinate"/>
                      <xsl:copy-of select="CollectionMetaData/SpatialCoverage/EastBoundingCoordinate"/>
                      <xsl:copy-of select="CollectionMetaData/SpatialCoverage/SouthBoundingCoordinate"/>
                    </BoundingRectangle>
                  </Geometry>
                </HorizontalSpatialDomain>
              </Spatial>
              <Temporal>
                <RangeDateTime>
                 <RangeBeginningDate> <xsl:value-of select="CollectionMetaData/TemporalCoverage/StartDate"/></RangeBeginningDate>
                 <RangeBeginningTime>00:00:00</RangeBeginningTime>
                 <RangeEndingDate><xsl:value-of select="CollectionMetaData/TemporalCoverage/StopDate"/></RangeEndingDate>
                 <RangeEndingTime>23:59:59</RangeEndingTime>
                </RangeDateTime>
              </Temporal>
              <xsl:copy-of select="CollectionMetaData/Platform"/>
           </CollectionMetaData>
         </Collections>  
        </CollectionMetaDataSets>
    </CollectionMetaDataFile>
   </xsl:for-each>
  </xsl:template>
</xsl:stylesheet>
