<?xml version="1.0" encoding="utf-8"?>
<!-- $Id: S4paGran2Giovanni.xsl,v 1.2 2008/06/26 02:42:58 glei Exp $ -->
<!-- -@@@ S4PA, Version $Name:  $ -->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" indent="yes"/>
 <!-- This XSLT transform S4PA Granule level XML Meta-data files into Giovanni ingest files. -->
  <xsl:template match="/">
   <xsl:for-each select="S4PAGranuleMetaDataFile">
   <GranuleMetaDataFile>
    <GranuleURMetaData>
     <xsl:copy-of select="CollectionMetaData/ShortName"/>
     <xsl:copy-of select="CollectionMetaData/VersionID"/>
     <DataURL><xsl:value-of select="DataGranule/GranuleID"/></DataURL>
     <xsl:copy-of select="DataGranule/SizeBytesDataGranule"/>
     <xsl:copy-of select="DataGranule/InsertDateTime"/>
     <xsl:copy-of select="RangeDateTime"/>
     <xsl:copy-of select="SpatialDomainContainer"/>
    </GranuleURMetaData>
   </GranuleMetaDataFile>
  </xsl:for-each>
 </xsl:template>
</xsl:stylesheet>
